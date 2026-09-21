// Package store contains the PostgreSQL-backed persistence layer for quota
// pools and reservations.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// uniqueViolation is PostgreSQL's SQLSTATE for a unique constraint breach.
const uniqueViolation = "23505"

// Reservation lifecycle states.
const (
	StatusPending   = "pending"
	StatusConfirmed = "confirmed"
	StatusReleased  = "released"
	StatusExpired   = "expired"
)

// Sentinel errors mapped to HTTP status codes by the API layer.
var (
	// ErrPoolExists is returned when creating a pool that already exists.
	ErrPoolExists = errors.New("quota pool already exists")
	// ErrPoolNotFound is returned when a pool does not exist for the tenant.
	ErrPoolNotFound = errors.New("quota pool not found")
	// ErrReservationExists is returned when a reservation id is already used.
	ErrReservationExists = errors.New("reservation already exists")
	// ErrReservationNotFound is returned for unknown or cross-tenant lookups.
	ErrReservationNotFound = errors.New("reservation not found")
	// ErrPoolInactive means the pool is not currently within its validity window.
	ErrPoolInactive = errors.New("quota pool is not currently active")
	// ErrInsufficientQuota means available quota cannot cover the reservation.
	ErrInsufficientQuota = errors.New("insufficient quota")
	// ErrInvalidExpiry means expiresAt is in the past or past pool end.
	ErrInvalidExpiry = errors.New("reservation expires outside pool window")
	// ErrNotPending means a confirm/release targeted a non-pending reservation,
	// or one that has already expired.
	ErrNotPending = errors.New("reservation is not pending")
	// ErrIdempotencyMismatch means a key was replayed with a different request.
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

// Pool is a tenant quota pool.
type Pool struct {
	TenantID   string    `json:"tenantId"`
	PoolID     string    `json:"poolId"`
	Limit      int       `json:"limit"`
	ValidFrom  time.Time `json:"validFrom"`
	ValidUntil time.Time `json:"validUntil"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// Reservation is a quota reservation record.
type Reservation struct {
	ReservationID string     `json:"reservationId"`
	TenantID      string     `json:"tenantId"`
	PoolID        string     `json:"poolId"`
	Amount        int        `json:"amount"`
	Status        string     `json:"status"`
	ExpiresAt     time.Time  `json:"expiresAt"`
	CreatedAt     time.Time  `json:"createdAt"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	ConfirmedAt   *time.Time `json:"confirmedAt,omitempty"`
	ReleasedAt    *time.Time `json:"releasedAt,omitempty"`
}

// Result is a stored idempotent outcome: HTTP status and response body.
type Result struct {
	Status int
	Body   []byte
}

// Store persists pools, reservations and idempotency records.
type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// New creates a Store backed by the given connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, now: func() time.Time { return time.Now().UTC() }}
}

// EnsureSchema creates all required tables and indexes. It is safe to call on
// every startup.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	// Store response bodies verbatim (TEXT): idempotent replays must return
	// the original bytes, which JSONB would re-serialize with reordered keys
	// and added whitespace.
	_, err := s.pool.Exec(ctx,
		`ALTER TABLE idempotency_records ALTER COLUMN response_body TYPE TEXT USING response_body::text`)
	return err
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS quota_pools (
    tenant_id    TEXT        NOT NULL,
    pool_id      TEXT        NOT NULL,
    limit_value  BIGINT      NOT NULL CHECK (limit_value > 0),
    valid_from   TIMESTAMPTZ NOT NULL,
    valid_until  TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, pool_id),
    CHECK (valid_from < valid_until)
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT        NOT NULL,
    tenant_id      TEXT        NOT NULL,
    pool_id        TEXT        NOT NULL,
    amount         BIGINT      NOT NULL CHECK (amount > 0),
    status         TEXT        NOT NULL CHECK (status IN ('pending','confirmed','released','expired')),
    expires_at     TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at   TIMESTAMPTZ,
    released_at    TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, pool_id, reservation_id),
    FOREIGN KEY (tenant_id, pool_id) REFERENCES quota_pools (tenant_id, pool_id)
);

CREATE INDEX IF NOT EXISTS reservations_status_idx
    ON reservations (tenant_id, pool_id, status);

CREATE TABLE IF NOT EXISTS idempotency_records (
    tenant_id        TEXT        NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    request_hash     TEXT        NOT NULL,
    method           TEXT        NOT NULL,
    path             TEXT        NOT NULL,
    response_status  INTEGER,
    response_body    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, idempotency_key)
);
`

// PutPool creates a quota pool and returns the stored record. It returns
// ErrPoolExists if the pool is already present; existing pools are never
// modified.
func (s *Store) PutPool(ctx context.Context, p Pool) (Pool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO quota_pools (tenant_id, pool_id, limit_value, valid_from, valid_until)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING tenant_id, pool_id, limit_value, valid_from, valid_until, created_at, updated_at`,
		p.TenantID, p.PoolID, p.Limit, p.ValidFrom, p.ValidUntil)
	var created Pool
	err := row.Scan(&created.TenantID, &created.PoolID, &created.Limit,
		&created.ValidFrom, &created.ValidUntil, &created.CreatedAt, &created.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return Pool{}, ErrPoolExists
		}
		return Pool{}, err
	}
	return created, nil
}

// GetPool fetches a pool for a tenant.
func (s *Store) GetPool(ctx context.Context, tenantID, poolID string) (Pool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT tenant_id, pool_id, limit_value, valid_from, valid_until, created_at, updated_at
		FROM quota_pools WHERE tenant_id = $1 AND pool_id = $2`,
		tenantID, poolID)
	var p Pool
	err := row.Scan(&p.TenantID, &p.PoolID, &p.Limit, &p.ValidFrom, &p.ValidUntil, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Pool{}, ErrPoolNotFound
	}
	if err != nil {
		return Pool{}, err
	}
	return p, nil
}

// Querier is the database surface business transactions need. Both pgxpool
// and pgx.Tx satisfy it.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// WithIdempotency runs fn under the protection of an idempotency key. An
// identical replay returns the stored Result without invoking fn (replayed is
// true; the caller answers such replays with 200). A key reused with a
// different request payload returns ErrIdempotencyMismatch before any business
// logic runs.
//
// Concurrency is handled by a claim protocol: the first request inserts a
// placeholder row that concurrent same-key requests block on (FOR UPDATE).
// Once the owner commits, waiters read the canonical outcome; if the owner
// rolls back, its placeholder disappears and one waiter claims the key.
// Business failures therefore store nothing and remain retryable.
func (s *Store) WithIdempotency(
	ctx context.Context,
	tenantID, key, method, path string,
	payload []byte,
	fn func(tx pgx.Tx, now time.Time) (Result, error),
) (result Result, replayed bool, err error) {
	hash := fingerprint(method, path, payload)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Result{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for {
		var claimed bool
		err = tx.QueryRow(ctx, `
			INSERT INTO idempotency_records
			    (tenant_id, idempotency_key, request_hash, method, path)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING
			RETURNING TRUE`,
			tenantID, key, hash, method, path).Scan(&claimed)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return Result{}, false, err
		}
		if claimed {
			break
		}

		// A concurrent request owns (or owned) the key. Block until it
		// resolves, then either return its outcome or take over the claim.
		var storedHash string
		var status *int
		var body []byte
		err = tx.QueryRow(ctx, `
			SELECT request_hash, response_status, response_body
			FROM idempotency_records
			WHERE tenant_id = $1 AND idempotency_key = $2
			FOR UPDATE`, tenantID, key).Scan(&storedHash, &status, &body)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// Owner rolled back after a business failure: retry the claim.
			continue
		case err != nil:
			return Result{}, false, err
		case status == nil:
			// Another waiter is already taking over; start over.
			continue
		case storedHash != hash:
			return Result{}, false, ErrIdempotencyMismatch
		default:
			return Result{Status: *status, Body: body}, true, nil
		}
	}

	outcome, err := fn(tx, s.now())
	if err != nil {
		// Rollback removes the placeholder so the key stays retryable.
		return Result{}, false, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE idempotency_records
		SET response_status = $3, response_body = $4
		WHERE tenant_id = $1 AND idempotency_key = $2`,
		tenantID, key, outcome.Status, outcome.Body); err != nil {
		return Result{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Result{}, false, err
	}
	return outcome, false, nil
}

// CreateReservation atomically creates a pending reservation inside tx. It
// locks the pool row, expires pending reservations past their deadline, checks
// pool activity and availability, then inserts. Available quota is the pool
// limit minus the amount of every unexpired pending or confirmed reservation.
func CreateReservation(ctx context.Context, tx pgx.Tx, now time.Time, r Reservation) (Reservation, error) {
	var limit int64
	var validFrom, validUntil time.Time
	err := tx.QueryRow(ctx, `
		SELECT limit_value, valid_from, valid_until
		FROM quota_pools
		WHERE tenant_id = $1 AND pool_id = $2
		FOR UPDATE`, r.TenantID, r.PoolID).Scan(&limit, &validFrom, &validUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrPoolNotFound
	}
	if err != nil {
		return Reservation{}, err
	}

	if now.Before(validFrom) || !now.Before(validUntil) {
		return Reservation{}, ErrPoolInactive
	}
	if !r.ExpiresAt.After(now) || r.ExpiresAt.After(validUntil) {
		return Reservation{}, ErrInvalidExpiry
	}

	// Release quota held by pending reservations whose deadline has passed.
	if _, err := tx.Exec(ctx, `
		UPDATE reservations
		SET status = 'expired', updated_at = $4
		WHERE tenant_id = $1 AND pool_id = $2 AND status = 'pending' AND expires_at <= $3`,
		r.TenantID, r.PoolID, now, now); err != nil {
		return Reservation{}, err
	}

	var used int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)
		FROM reservations
		WHERE tenant_id = $1 AND pool_id = $2 AND status IN ('pending', 'confirmed')`,
		r.TenantID, r.PoolID).Scan(&used); err != nil {
		return Reservation{}, err
	}
	if used+int64(r.Amount) > limit {
		return Reservation{}, ErrInsufficientQuota
	}

	return insertReservation(ctx, tx, r)
}

// ConfirmReservation marks an unexpired pending reservation confirmed. It runs
// inside the caller's (idempotency) transaction. The conditional UPDATE makes
// confirm and release mutually exclusive under concurrency.
func ConfirmReservation(ctx context.Context, tx pgx.Tx, now time.Time, tenantID, poolID, reservationID string) (Reservation, error) {
	return transitionReservation(ctx, tx, now, tenantID, poolID, reservationID, StatusConfirmed)
}

// ReleaseReservation marks an unexpired pending reservation released.
func ReleaseReservation(ctx context.Context, tx pgx.Tx, now time.Time, tenantID, poolID, reservationID string) (Reservation, error) {
	return transitionReservation(ctx, tx, now, tenantID, poolID, reservationID, StatusReleased)
}

func transitionReservation(
	ctx context.Context,
	q Querier,
	now time.Time,
	tenantID, poolID, reservationID, target string,
) (Reservation, error) {
	row := q.QueryRow(ctx, `
		UPDATE reservations
		SET status       = $6,
		    confirmed_at = CASE WHEN $6 = 'confirmed' THEN $5 ELSE confirmed_at END,
		    released_at  = CASE WHEN $6 = 'released'  THEN $5 ELSE released_at END,
		    updated_at   = $5
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3
		  AND status = 'pending' AND expires_at > $4
		RETURNING reservation_id, tenant_id, pool_id, amount, status, expires_at,
		          created_at, updated_at, confirmed_at, released_at`,
		tenantID, poolID, reservationID, now, now, target)
	r, err := scanReservation(row)
	if err == nil {
		return r, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, err
	}

	// No actionable row: figure out why so the caller can map it correctly,
	// and flip a stale pending to expired so its quota is visibly released.
	var status string
	var expiresAt time.Time
	err = q.QueryRow(ctx, `
		SELECT status, expires_at FROM reservations
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3`,
		tenantID, poolID, reservationID).Scan(&status, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrReservationNotFound
	}
	if err != nil {
		return Reservation{}, err
	}
	if status == StatusPending && !expiresAt.After(now) {
		if _, err := q.Exec(ctx, `
			UPDATE reservations
			SET status = 'expired', updated_at = $4
			WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3
			  AND status = 'pending' AND expires_at <= $4`,
			tenantID, poolID, reservationID, now); err != nil {
			return Reservation{}, err
		}
	}
	return Reservation{}, ErrNotPending
}

// GetReservation fetches a full reservation record. A pending reservation past
// its expiry is first transitioned to expired, so reads never report a stale
// pending state. Unknown ids and cross-tenant access both return
// ErrReservationNotFound.
func (s *Store) GetReservation(ctx context.Context, tenantID, poolID, reservationID string) (Reservation, error) {
	now := s.now()
	if _, err := s.pool.Exec(ctx, `
		UPDATE reservations
		SET status = 'expired', updated_at = $4
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3
		  AND status = 'pending' AND expires_at <= $4`,
		tenantID, poolID, reservationID, now); err != nil {
		return Reservation{}, err
	}
	row := s.pool.QueryRow(ctx, `
		SELECT reservation_id, tenant_id, pool_id, amount, status, expires_at,
		       created_at, updated_at, confirmed_at, released_at
		FROM reservations
		WHERE tenant_id = $1 AND pool_id = $2 AND reservation_id = $3`,
		tenantID, poolID, reservationID)
	r, err := scanReservation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrReservationNotFound
	}
	return r, err
}

// ExpirePending flips every stale pending reservation to expired and releases
// its held quota. Intended for periodic background housekeeping.
func (s *Store) ExpirePending(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE reservations SET status = 'expired', updated_at = $2
		WHERE status = 'pending' AND expires_at <= $1`, s.now(), s.now())
	return err
}

func insertReservation(ctx context.Context, tx pgx.Tx, r Reservation) (Reservation, error) {
	row := tx.QueryRow(ctx, `
		INSERT INTO reservations
		    (reservation_id, tenant_id, pool_id, amount, status, expires_at)
		VALUES ($1, $2, $3, $4, 'pending', $5)
		RETURNING reservation_id, tenant_id, pool_id, amount, status, expires_at,
		          created_at, updated_at, confirmed_at, released_at`,
		r.ReservationID, r.TenantID, r.PoolID, r.Amount, r.ExpiresAt)
	created, err := scanReservation(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return Reservation{}, ErrReservationExists
		}
		return Reservation{}, err
	}
	return created, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanReservation(row scannable) (Reservation, error) {
	var r Reservation
	err := row.Scan(
		&r.ReservationID, &r.TenantID, &r.PoolID, &r.Amount, &r.Status,
		&r.ExpiresAt, &r.CreatedAt, &r.UpdatedAt, &r.ConfirmedAt, &r.ReleasedAt,
	)
	return r, err
}

// fingerprint hashes the request method, path and canonical payload so replays
// with different content are rejected while identical replays return the
// original result.
func fingerprint(method, path string, payload []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(method))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(path))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// CanonicalPayload re-encodes JSON with object keys sorted and insignificant
// whitespace removed, so semantically identical bodies hash identically.
func CanonicalPayload(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("multiple JSON values")
	}
	return json.Marshal(v)
}
