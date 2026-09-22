package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errNotFound signals that a referenced row does not exist for the tenant.
var errNotFound = errors.New("not found")

const schemaSQL = `
CREATE TABLE IF NOT EXISTS quota_pools (
    tenant_id   text        NOT NULL,
    pool_id     text        NOT NULL,
    lim         bigint      NOT NULL CHECK (lim > 0),
    valid_from  timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, pool_id),
    CHECK (valid_from < valid_until)
);

CREATE TABLE IF NOT EXISTS reservations (
    tenant_id      text        NOT NULL,
    pool_id        text        NOT NULL,
    reservation_id text        NOT NULL,
    amount         bigint      NOT NULL CHECK (amount > 0),
    status         text        NOT NULL CHECK (status IN ('pending', 'confirmed', 'released', 'expired')),
    expires_at     timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, pool_id, reservation_id),
    FOREIGN KEY (tenant_id, pool_id) REFERENCES quota_pools (tenant_id, pool_id)
);

-- Team scoping is optional; pre-team reservations keep a NULL team_id.
ALTER TABLE reservations ADD COLUMN IF NOT EXISTS team_id text;

CREATE INDEX IF NOT EXISTS reservations_active_idx
    ON reservations (tenant_id, pool_id, expires_at)
    WHERE status IN ('pending', 'confirmed');

CREATE INDEX IF NOT EXISTS reservations_team_idx
    ON reservations (tenant_id, pool_id, team_id)
    WHERE team_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS team_allocations (
    tenant_id  text        NOT NULL,
    pool_id    text        NOT NULL,
    team_id    text        NOT NULL,
    allocated  bigint      NOT NULL CHECK (allocated >= 0),
    version    bigint      NOT NULL CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, pool_id, team_id),
    FOREIGN KEY (tenant_id, pool_id) REFERENCES quota_pools (tenant_id, pool_id)
);

CREATE TABLE IF NOT EXISTS idempotency_records (
    id              bigserial    PRIMARY KEY,
    tenant_id       text         NOT NULL,
    operation       text         NOT NULL,
    idempotency_key text         NOT NULL,
    request_hash    text         NOT NULL,
    status_code     integer      NOT NULL,
    response_body   bytea        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);
`

// EnsureSchema creates the required tables and indexes if they are absent.
// It also pins every pooled connection to UTC so RFC3339 timestamps are
// rendered consistently regardless of the database server's timezone.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	pool.Config().ConnConfig.RuntimeParams["timezone"] = "UTC"
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	return nil
}

type pgStore struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pgx connection pool as the API backend.
func NewStore(pool *pgxpool.Pool) backend {
	return &pgStore{pool: pool}
}

func (s *pgStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *pgStore) createPool(ctx context.Context, tenantID, poolID string, in poolInput) (poolResponse, bool, error) {
	var resp poolResponse
	err := s.pool.QueryRow(ctx, `
		INSERT INTO quota_pools (tenant_id, pool_id, lim, valid_from, valid_until)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, pool_id) DO NOTHING
		RETURNING tenant_id, pool_id, lim, valid_from, valid_until, created_at`,
		tenantID, poolID, in.limit, in.validFrom, in.validUntil,
	).Scan(&resp.TenantID, &resp.PoolID, &resp.Limit, &resp.ValidFrom, &resp.ValidUntil, &resp.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return poolResponse{}, false, nil
	}
	if err != nil {
		return poolResponse{}, false, err
	}
	resp.normalize()
	return resp, true, nil
}

// idempotencyOutcome is the resolution of the idempotency record race.
type idempotencyOutcome int

const (
	idempotencyClaimed  idempotencyOutcome = iota // this transaction owns the record and must run business logic
	idempotencyReplay                             // a completed record exists with the same request hash
	idempotencyMismatch                           // a record exists but carries a different request hash
)

// claimIdempotency inserts or locks the tenant-scoped idempotency record.
// Idempotency scope is (tenant, key): the same key reused with different
// request content anywhere across the tenant's POST endpoints is a 409,
// while an identical replay returns 200 and the original result. The claim
// is resolved before any business validation, and concurrent same-key
// requests serialize on the row lock so the business logic runs at most
// once. Postgres makes a speculative insert wait for a competing
// transaction; ON CONFLICT DO NOTHING returns no rows only once the
// competitor committed, so a surviving row always exists for FOR UPDATE.
func claimIdempotency(ctx context.Context, tx pgx.Tx, tenantID, operation, key, requestHash string) (idempotencyOutcome, operationResult, error) {
	var insertedID int64
	err := tx.QueryRow(ctx, `
		INSERT INTO idempotency_records
		    (tenant_id, operation, idempotency_key, request_hash, status_code, response_body)
		VALUES ($1, $2, $3, $4, 0, ''::bytea)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING id`,
		tenantID, operation, key, requestHash,
	).Scan(&insertedID)
	if err == nil {
		return idempotencyClaimed, operationResult{}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, operationResult{}, err
	}

	var storedHash string
	var storedBody []byte
	err = tx.QueryRow(ctx, `
		SELECT request_hash, response_body
		FROM idempotency_records
		WHERE tenant_id = $1 AND idempotency_key = $2
		FOR UPDATE`,
		tenantID, key,
	).Scan(&storedHash, &storedBody)
	if err != nil {
		return 0, operationResult{}, err
	}
	if storedHash != requestHash {
		return idempotencyMismatch, operationResult{
			status: http.StatusConflict,
			body:   mustJSON(errorResponse{Error: "Idempotency-Key was reused with a different request"}),
		}, nil
	}
	// Same request replay: always surface as HTTP 200 with the original result.
	return idempotencyReplay, operationResult{status: http.StatusOK, body: storedBody}, nil
}

func completeIdempotency(ctx context.Context, tx pgx.Tx, tenantID, key string, result operationResult) error {
	_, err := tx.Exec(ctx, `
		UPDATE idempotency_records
		SET status_code = $3, response_body = $4
		WHERE tenant_id = $1 AND idempotency_key = $2`,
		tenantID, key, result.status, result.body)
	return err
}

// conflictResult finalizes a claimed idempotency record with a 409 result
// and commits the transaction, making the conflict response itself replayable.
func conflictResult(ctx context.Context, tx pgx.Tx, tenantID, key, message string) (operationResult, error) {
	result := operationResult{status: http.StatusConflict, body: mustJSON(errorResponse{Error: message})}
	if err := completeIdempotency(ctx, tx, tenantID, key, result); err != nil {
		return operationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return operationResult{}, err
	}
	return result, nil
}

func mustJSON(v any) []byte {
	body, err := json.Marshal(v)
	if err != nil {
		// All values marshalled here are plain structs with fixed fields.
		panic(err)
	}
	return body
}

func (r *reservationResponse) normalize() {
	r.ExpiresAt = r.ExpiresAt.UTC()
	r.CreatedAt = r.CreatedAt.UTC()
	r.UpdatedAt = r.UpdatedAt.UTC()
}

func (p *poolResponse) normalize() {
	p.ValidFrom = p.ValidFrom.UTC()
	p.ValidUntil = p.ValidUntil.UTC()
	p.CreatedAt = p.CreatedAt.UTC()
}

// expirePending moves past-deadline pending reservations of the pool to
// expired, releasing their reserved quota. Must run while the pool row lock
// is held so the availability check observes a stable reservation set.
func expirePending(ctx context.Context, tx pgx.Tx, tenantID, poolID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE reservations
		SET status = 'expired', updated_at = now()
		WHERE tenant_id = $1 AND pool_id = $2 AND status = 'pending' AND expires_at <= now()`,
		tenantID, poolID)
	return err
}

// teamUsed sums the quota a team currently occupies: unexpired pending
// reservations plus all confirmed ones. Released and expired reservations
// occupy nothing, so releasing or expiring restores the team's balance.
func teamUsed(ctx context.Context, tx pgx.Tx, tenantID, poolID, teamID string) (int64, error) {
	var used int64
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)
		FROM reservations
		WHERE tenant_id = $1 AND pool_id = $2 AND team_id = $3 AND (
		    status = 'confirmed'
		    OR (status = 'pending' AND expires_at > now())
		)`,
		tenantID, poolID, teamID,
	).Scan(&used)
	return used, err
}

func (s *pgStore) createReservation(ctx context.Context, tenantID, poolID, key, requestHash string, in reservationInput) (operationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return operationResult{}, err
	}
	defer tx.Rollback(ctx)

	operation := "create_reservation:" + poolID
	outcome, replay, err := claimIdempotency(ctx, tx, tenantID, operation, key, requestHash)
	if err != nil {
		return operationResult{}, err
	}
	if outcome != idempotencyClaimed {
		if err := tx.Commit(ctx); err != nil {
			return operationResult{}, err
		}
		return replay, nil
	}
	fail := func(message string) (operationResult, error) {
		return conflictResult(ctx, tx, tenantID, key, message)
	}

	// Lock the pool so concurrent reservations over the same pool serialize;
	// the availability check and insert then happen atomically.
	var poolLimit int64
	var validFrom, validUntil time.Time
	err = tx.QueryRow(ctx, `
		SELECT lim, valid_from, valid_until
		FROM quota_pools
		WHERE tenant_id = $1 AND pool_id = $2
		FOR UPDATE`,
		tenantID, poolID,
	).Scan(&poolLimit, &validFrom, &validUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail("quota pool not found")
	}
	if err != nil {
		return operationResult{}, err
	}

	// Lazy expiry: pending reservations past their deadline release quota.
	if err := expirePending(ctx, tx, tenantID, poolID); err != nil {
		return operationResult{}, err
	}

	now := time.Now().UTC()
	if validFrom.After(now) || !validUntil.After(now) {
		return fail("quota pool is not currently valid")
	}
	if !in.expiresAt.After(now) || in.expiresAt.After(validUntil) {
		return fail("expiresAt must be in the future and no later than the pool validity end")
	}

	// Available quota is the limit minus the amounts of unexpired pending
	// reservations and all confirmed reservations (confirmed quota is held
	// permanently and is not released by pending expiry).
	var reserved int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)
		FROM reservations
		WHERE tenant_id = $1 AND pool_id = $2 AND (
		    status = 'confirmed'
		    OR (status = 'pending' AND expires_at > now())
		)`,
		tenantID, poolID,
	).Scan(&reserved)
	if err != nil {
		return operationResult{}, err
	}
	if poolLimit-reserved < in.amount {
		return fail("insufficient quota available")
	}

	// Team-scoped reservations additionally need an existing team allocation
	// with enough team-level balance. The pool row lock held above serializes
	// this against concurrent allocations, reservations, decisions and expiry
	// cleanup, so neither level can be oversubscribed.
	var teamIDParam *string
	if in.teamID != "" {
		teamIDParam = &in.teamID
		var allocated int64
		err = tx.QueryRow(ctx, `
			SELECT allocated
			FROM team_allocations
			WHERE tenant_id = $1 AND pool_id = $2 AND team_id = $3
			FOR UPDATE`,
			tenantID, poolID, in.teamID,
		).Scan(&allocated)
		if errors.Is(err, pgx.ErrNoRows) {
			return fail("team not found")
		}
		if err != nil {
			return operationResult{}, err
		}
		used, err := teamUsed(ctx, tx, tenantID, poolID, in.teamID)
		if err != nil {
			return operationResult{}, err
		}
		if allocated-used < in.amount {
			return fail("insufficient team quota available")
		}
	}

	resp := reservationResponse{
		TenantID:      tenantID,
		PoolID:        poolID,
		ReservationID: in.reservationID,
		Amount:        in.amount,
		Status:        "pending",
		ExpiresAt:     in.expiresAt,
		TeamID:        teamIDParam,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO reservations (tenant_id, pool_id, reservation_id, amount, status, expires_at, team_id)
		VALUES ($1, $2, $3, $4, 'pending', $5, $6)
		RETURNING created_at, updated_at`,
		tenantID, poolID, in.reservationID, in.amount, in.expiresAt, teamIDParam,
	).Scan(&resp.CreatedAt, &resp.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fail("reservation already exists")
		}
		return operationResult{}, err
	}
	resp.normalize()

	result := operationResult{status: http.StatusCreated, body: mustJSON(resp)}
	if err := completeIdempotency(ctx, tx, tenantID, key, result); err != nil {
		return operationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return operationResult{}, err
	}
	return result, nil
}

func (s *pgStore) decideReservation(ctx context.Context, tenantID, poolID, reservationID, key, requestHash, decision string) (operationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return operationResult{}, err
	}
	defer tx.Rollback(ctx)

	operation := "decide_reservation:" + poolID + ":" + reservationID
	outcome, replay, err := claimIdempotency(ctx, tx, tenantID, operation, key, requestHash)
	if err != nil {
		return operationResult{}, err
	}
	if outcome != idempotencyClaimed {
		if err := tx.Commit(ctx); err != nil {
			return operationResult{}, err
		}
		return replay, nil
	}
	fail := func(message string) (operationResult, error) {
		return conflictResult(ctx, tx, tenantID, key, message)
	}

	// Lock the pool first to serialize with reservation creation and with the
	// opposite decision, so confirm and release cannot both take effect.
	var poolLimit int64
	err = tx.QueryRow(ctx, `
		SELECT lim FROM quota_pools
		WHERE tenant_id = $1 AND pool_id = $2
		FOR UPDATE`,
		tenantID, poolID,
	).Scan(&poolLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail("reservation is not pending")
	}
	if err != nil {
		return operationResult{}, err
	}

	if err := expirePending(ctx, tx, tenantID, poolID); err != nil {
		return operationResult{}, err
	}

	var resp reservationResponse
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, pool_id, reservation_id, amount, status, expires_at, team_id, created_at, updated_at
		FROM reservations
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3
		FOR UPDATE`,
		tenantID, poolID, reservationID,
	).Scan(&resp.TenantID, &resp.PoolID, &resp.ReservationID, &resp.Amount, &resp.Status, &resp.ExpiresAt, &resp.TeamID, &resp.CreatedAt, &resp.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail("reservation is not pending")
	}
	if err != nil {
		return operationResult{}, err
	}
	if resp.Status != "pending" {
		return fail("reservation is not pending")
	}

	err = tx.QueryRow(ctx, `
		UPDATE reservations
		SET status = $4, updated_at = now()
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3
		RETURNING updated_at`,
		tenantID, poolID, reservationID, decision,
	).Scan(&resp.UpdatedAt)
	if err != nil {
		return operationResult{}, err
	}
	resp.Status = decision
	resp.normalize()

	result := operationResult{status: http.StatusOK, body: mustJSON(resp)}
	if err := completeIdempotency(ctx, tx, tenantID, key, result); err != nil {
		return operationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return operationResult{}, err
	}
	return result, nil
}

func (s *pgStore) getReservation(ctx context.Context, tenantID, poolID, reservationID string) (reservationResponse, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return reservationResponse{}, err
	}
	defer tx.Rollback(ctx)

	// Reflect lazy expiry on the fetched row.
	if _, err := tx.Exec(ctx, `
		UPDATE reservations
		SET status = 'expired', updated_at = now()
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3
		  AND status = 'pending' AND expires_at <= now()`,
		tenantID, poolID, reservationID); err != nil {
		return reservationResponse{}, err
	}

	var resp reservationResponse
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, pool_id, reservation_id, amount, status, expires_at, team_id, created_at, updated_at
		FROM reservations
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3`,
		tenantID, poolID, reservationID,
	).Scan(&resp.TenantID, &resp.PoolID, &resp.ReservationID, &resp.Amount, &resp.Status, &resp.ExpiresAt, &resp.TeamID, &resp.CreatedAt, &resp.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return reservationResponse{}, errNotFound
	}
	if err != nil {
		return reservationResponse{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return reservationResponse{}, err
	}
	resp.normalize()
	return resp, nil
}

// setAllocation creates or updates a team's allocation within a pool. A team
// that has never been allocated starts at allocated 0, version 0. The update
// only applies when expectedVersion matches the current version; the total of
// all team allocations must stay within the pool limit, and a lowered
// allocation must not drop below the quota the team currently occupies.
func (s *pgStore) setAllocation(ctx context.Context, tenantID, poolID, teamID, key, requestHash string, amount, expectedVersion int64) (operationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return operationResult{}, err
	}
	defer tx.Rollback(ctx)

	operation := "set_allocation:" + poolID + ":" + teamID
	outcome, replay, err := claimIdempotency(ctx, tx, tenantID, operation, key, requestHash)
	if err != nil {
		return operationResult{}, err
	}
	if outcome != idempotencyClaimed {
		if err := tx.Commit(ctx); err != nil {
			return operationResult{}, err
		}
		return replay, nil
	}
	fail := func(message string) (operationResult, error) {
		return conflictResult(ctx, tx, tenantID, key, message)
	}

	// Lock the pool row so allocation changes serialize with reservations,
	// decisions and expiry cleanup over the same pool.
	var poolLimit int64
	err = tx.QueryRow(ctx, `
		SELECT lim FROM quota_pools
		WHERE tenant_id = $1 AND pool_id = $2
		FOR UPDATE`,
		tenantID, poolID,
	).Scan(&poolLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail("quota pool not found")
	}
	if err != nil {
		return operationResult{}, err
	}

	// Lazy expiry first so the occupied-quota check sees a stable state.
	if err := expirePending(ctx, tx, tenantID, poolID); err != nil {
		return operationResult{}, err
	}

	var currentVersion int64
	err = tx.QueryRow(ctx, `
		SELECT version
		FROM team_allocations
		WHERE tenant_id = $1 AND pool_id = $2 AND team_id = $3
		FOR UPDATE`,
		tenantID, poolID, teamID,
	).Scan(&currentVersion)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return operationResult{}, err
	}
	// A missing team row means allocated 0, version 0.
	if expectedVersion != currentVersion {
		return fail("version mismatch")
	}

	var othersAllocated int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(allocated), 0)
		FROM team_allocations
		WHERE tenant_id = $1 AND pool_id = $2 AND team_id <> $3`,
		tenantID, poolID, teamID,
	).Scan(&othersAllocated)
	if err != nil {
		return operationResult{}, err
	}
	if othersAllocated+amount > poolLimit {
		return fail("total team allocations would exceed the pool limit")
	}

	used, err := teamUsed(ctx, tx, tenantID, poolID, teamID)
	if err != nil {
		return operationResult{}, err
	}
	if amount < used {
		return fail("allocation cannot be lower than the team's occupied quota")
	}

	var newVersion int64
	err = tx.QueryRow(ctx, `
		INSERT INTO team_allocations (tenant_id, pool_id, team_id, allocated, version)
		VALUES ($1, $2, $3, $4, 1)
		ON CONFLICT (tenant_id, pool_id, team_id)
		DO UPDATE SET allocated = EXCLUDED.allocated,
		              version = team_allocations.version + 1,
		              updated_at = now()
		RETURNING version`,
		tenantID, poolID, teamID, amount,
	).Scan(&newVersion)
	if err != nil {
		return operationResult{}, err
	}

	resp := allocationResponse{
		TeamID:    teamID,
		Allocated: amount,
		Used:      used,
		Available: amount - used,
		Version:   newVersion,
	}
	result := operationResult{status: http.StatusOK, body: mustJSON(resp)}
	if err := completeIdempotency(ctx, tx, tenantID, key, result); err != nil {
		return operationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return operationResult{}, err
	}
	return result, nil
}

func (s *pgStore) getAllocation(ctx context.Context, tenantID, poolID, teamID string) (allocationResponse, error) {
	resp := allocationResponse{TeamID: teamID}
	err := s.pool.QueryRow(ctx, `
		SELECT a.allocated, a.version,
		    COALESCE((SELECT SUM(r.amount)
		        FROM reservations r
		        WHERE r.tenant_id = a.tenant_id AND r.pool_id = a.pool_id AND r.team_id = a.team_id
		          AND (r.status = 'confirmed' OR (r.status = 'pending' AND r.expires_at > now()))), 0)
		FROM team_allocations a
		WHERE a.tenant_id = $1 AND a.pool_id = $2 AND a.team_id = $3`,
		tenantID, poolID, teamID,
	).Scan(&resp.Allocated, &resp.Version, &resp.Used)
	if errors.Is(err, pgx.ErrNoRows) {
		return allocationResponse{}, errNotFound
	}
	if err != nil {
		return allocationResponse{}, err
	}
	resp.Available = resp.Allocated - resp.Used
	return resp, nil
}
