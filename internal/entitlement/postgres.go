package entitlement

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore is a PostgreSQL-backed entitlement Service. Every mutating
// operation runs in a SERIALIZABLE transaction that takes the entitlement
// row lock first, so concurrent allocations and usages against the same
// entitlement are serialized and can never oversell it.
type PGStore struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// NewPGStore wraps a connection pool.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool, now: func() time.Time { return time.Now().UTC() }}
}

// Migrate creates the required schema. It is safe to call repeatedly.
func (s *PGStore) Migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS entitlements (
			id         TEXT PRIMARY KEY,
			team_id    TEXT NOT NULL,
			type       TEXT NOT NULL CHECK (type IN ('seat','quota')),
			total      INTEGER NOT NULL CHECK (total > 0),
			status     TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','cancelled')),
			expires_at TIMESTAMPTZ NOT NULL,
			used       INTEGER NOT NULL DEFAULT 0 CHECK (used >= 0),
			version    INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		// A team may only have one grant of the same type with the same
		// effective deadline; recreating it is a conflict.
		`CREATE UNIQUE INDEX IF NOT EXISTS entitlements_team_type_expires_key
			ON entitlements (team_id, type, expires_at)`,
		`CREATE TABLE IF NOT EXISTS allocations (
			entitlement_id TEXT NOT NULL REFERENCES entitlements(id) ON DELETE CASCADE,
			member_id      TEXT NOT NULL,
			allocated      INTEGER NOT NULL CHECK (allocated > 0),
			created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (entitlement_id, member_id)
		)`,
		`CREATE TABLE IF NOT EXISTS usages (
			id             BIGSERIAL PRIMARY KEY,
			entitlement_id TEXT NOT NULL REFERENCES entitlements(id) ON DELETE CASCADE,
			member_id      TEXT NOT NULL,
			quantity       INTEGER NOT NULL CHECK (quantity > 0),
			usage_key      TEXT NOT NULL,
			result_status  TEXT NOT NULL,
			result_used    INTEGER NOT NULL,
			result_version INTEGER NOT NULL,
			created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (entitlement_id, usage_key)
		)`,
		`CREATE INDEX IF NOT EXISTS usages_entitlement_member_idx
			ON usages (entitlement_id, member_id)`,
	}
	for _, stmt := range statements {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

const entitlementColumns = `id, team_id, type, total, status, expires_at, used, version`

func scanEntitlement(row pgx.Row) (*Entitlement, error) {
	var e Entitlement
	var status string
	err := row.Scan(&e.ID, &e.TeamID, &e.Type, &e.Total, &status, &e.ExpiresAt, &e.Used, &e.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}
	e.Status = Status(status)
	e.ExpiresAt = e.ExpiresAt.UTC()
	return &e, nil
}

// present applies read-time rules (expiry) to a stored row.
func (s *PGStore) present(e *Entitlement) *Entitlement {
	c := *e
	c.Status = effectiveStatus(e.Status, e.ExpiresAt, s.now())
	return &c
}

func isPGCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// transact runs fn in one transaction. Mutations take the entitlement row
// lock first, so READ COMMITTED plus FOR UPDATE fully serializes operations
// against the same entitlement; waiters see every earlier committed change
// once they acquire the lock. Any non-nil error rolls back, which is what
// makes failed requests leave no trace.
func (s *PGStore) transact(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return nil
}

// transactSnapshot runs fn against a single consistent snapshot. It is used
// by read-only detail queries so the entitlement row and its per-member
// aggregates never disagree.
func (s *PGStore) transactSnapshot(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return nil
}

func (s *PGStore) existingForConflict(ctx context.Context, tx pgx.Tx, params CreateParams) (*Entitlement, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+entitlementColumns+` FROM entitlements
		 WHERE team_id=$1 AND type=$2 AND expires_at=$3`,
		params.TeamID, params.Type, params.ExpiresAt)
	e, err := scanEntitlement(row)
	if err != nil {
		return nil, err
	}
	return s.present(e), nil
}

// Create inserts a new active entitlement. A duplicate (same team, type and
// expiry) is rejected with 409 and the record that already exists.
func (s *PGStore) Create(ctx context.Context, params CreateParams) (*Entitlement, error) {
	if err := validateCreate(params); err != nil {
		return nil, err
	}
	params.ExpiresAt = params.ExpiresAt.UTC()
	if !s.now().Before(params.ExpiresAt) {
		return nil, invalid("invalid_expiresAt", "expiresAt must be in the future")
	}

	var created *Entitlement
	err := s.transact(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SAVEPOINT before_insert`); err != nil {
			return err
		}
		row := tx.QueryRow(ctx,
			`INSERT INTO entitlements (id, team_id, type, total, status, expires_at, used, version)
			 VALUES (gen_random_uuid()::text, $1, $2, $3, 'active', $4, 0, 1)
			 RETURNING `+entitlementColumns,
			params.TeamID, params.Type, params.Total, params.ExpiresAt)
		e, err := scanEntitlement(row)
		if err != nil {
			if isPGCode(err, "23505") {
				// A unique violation aborts the current subtransaction;
				// roll back to the savepoint so the existing row can be read.
				if _, rbErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT before_insert`); rbErr != nil {
					return rbErr
				}
				existing, lookupErr := s.existingForConflict(ctx, tx, params)
				if lookupErr != nil {
					return lookupErr
				}
				return &ConflictError{
					APIError: APIError{Kind: KindConflict, Code: "entitlement_already_exists",
						Message: "an entitlement with the same team, type and expiry already exists"},
					Existing: existing,
				}
			}
			return err
		}
		created = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.present(created), nil
}

// Get returns an entitlement or a not-found error. Expiry is derived.
func (s *PGStore) Get(ctx context.Context, id string) (*Entitlement, error) {
	if err := validateTarget(id); err != nil {
		return nil, err
	}
	row := s.pool.QueryRow(ctx,
		`SELECT `+entitlementColumns+` FROM entitlements WHERE id=$1`, id)
	e, err := scanEntitlement(row)
	if err != nil {
		return nil, err
	}
	return s.present(e), nil
}

// lockEntitlement fetches an entitlement row FOR UPDATE and verifies it can
// still be operated on.
func (s *PGStore) lockEntitlement(ctx context.Context, tx pgx.Tx, id string) (*Entitlement, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+entitlementColumns+` FROM entitlements WHERE id=$1 FOR UPDATE`, id)
	e, err := scanEntitlement(row)
	if err != nil {
		return nil, err
	}
	switch effectiveStatus(e.Status, e.ExpiresAt, s.now()) {
	case Cancelled:
		return nil, conflict("entitlement_cancelled", "entitlement %s has been cancelled and can no longer be changed", id)
	case Expired:
		return nil, conflict("entitlement_expired", "entitlement %s expired at %s and can no longer be changed", id, e.ExpiresAt.Format(time.RFC3339))
	}
	return e, nil
}

// Allocate grants quantity seats/quota to a member, subject to total.
func (s *PGStore) Allocate(ctx context.Context, id, memberID string, quantity int) (*Entitlement, error) {
	if err := validateTarget(id); err != nil {
		return nil, err
	}
	if err := validateAllocate(memberID, quantity); err != nil {
		return nil, err
	}

	var updated *Entitlement
	err := s.transact(ctx, func(tx pgx.Tx) error {
		e, err := s.lockEntitlement(ctx, tx, id)
		if err != nil {
			return err
		}
		if e.Used+quantity > e.Total {
			return conflict("allocation_exceeds_total",
				"cannot allocate %d: %d of %d already allocated", quantity, e.Used, e.Total)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO allocations (entitlement_id, member_id, allocated)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (entitlement_id, member_id)
			 DO UPDATE SET allocated = allocations.allocated + EXCLUDED.allocated`,
			id, memberID, quantity); err != nil {
			return err
		}
		row := tx.QueryRow(ctx,
			`UPDATE entitlements SET used = used + $2, version = version + 1
			 WHERE id = $1 RETURNING `+entitlementColumns,
			id, quantity)
		updated, err = scanEntitlement(row)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.present(updated), nil
}

// Use consumes quantity for a member. A repeated usageKey replays the first
// result instead of accumulating again; a reused key with different request
// parameters is rejected.
func (s *PGStore) Use(ctx context.Context, id, memberID string, quantity int, usageKey string) (*Entitlement, UsageRecord, bool, error) {
	if err := validateTarget(id); err != nil {
		return nil, UsageRecord{}, false, err
	}
	if err := validateUse(memberID, quantity, usageKey); err != nil {
		return nil, UsageRecord{}, false, err
	}

	var (
		updated  *Entitlement
		record   UsageRecord
		replayed bool
	)
	err := s.transact(ctx, func(tx pgx.Tx) error {
		// Lock first to serialize against concurrent mutations...
		row0 := tx.QueryRow(ctx,
			`SELECT `+entitlementColumns+` FROM entitlements WHERE id=$1 FOR UPDATE`, id)
		e, err := scanEntitlement(row0)
		if err != nil {
			return err
		}

		// Idempotency takes precedence over lifecycle state: a repeated
		// request is a history read, so it replays the first result even
		// when the entitlement has since been cancelled or expired. The
		// key is unique per entitlement; snapshots make the replay
		// byte-for-byte identical to the first response.
		var existingMember, replayStatus string
		var existingQty, replayUsed, replayVersion int
		lookupErr := tx.QueryRow(ctx,
			`SELECT member_id, quantity, result_status, result_used, result_version
			 FROM usages WHERE entitlement_id=$1 AND usage_key=$2`,
			id, usageKey).Scan(&existingMember, &existingQty, &replayStatus, &replayUsed, &replayVersion)
		switch {
		case lookupErr == nil:
			if existingMember != memberID || existingQty != quantity {
				return &UsageKeyConflictError{
					APIError: APIError{Kind: KindConflict, Code: "usage_key_reused",
						Message: "usageKey was already used with different memberId or quantity"},
					Existing: UsageRecord{MemberID: existingMember, Quantity: existingQty, UsageKey: usageKey},
				}
			}
			record = UsageRecord{MemberID: existingMember, Quantity: existingQty, UsageKey: usageKey}
			replayed = true
			replay := *e
			replay.Status = Status(replayStatus)
			replay.Used = replayUsed
			replay.Version = replayVersion
			updated = &replay
			return nil
		case !errors.Is(lookupErr, pgx.ErrNoRows):
			return lookupErr
		}

		// A genuinely new consumption is only possible while active.
		switch effectiveStatus(e.Status, e.ExpiresAt, s.now()) {
		case Cancelled:
			return conflict("entitlement_cancelled", "entitlement %s has been cancelled and can no longer be used", id)
		case Expired:
			return conflict("entitlement_expired", "entitlement %s expired at %s and can no longer be used", id, e.ExpiresAt.Format(time.RFC3339))
		}

		// Member's unused headroom = allocated - already consumed.
		var allocated, consumed int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE((SELECT allocated FROM allocations
			                 WHERE entitlement_id=$1 AND member_id=$2), 0),
			        COALESCE((SELECT SUM(quantity) FROM usages
			                 WHERE entitlement_id=$1 AND member_id=$2), 0)`,
			id, memberID).Scan(&allocated, &consumed); err != nil {
			return err
		}
		remaining := allocated - consumed
		if remaining < quantity {
			return conflict("usage_exceeds_member_remaining",
				"member %s has %d unused but requested %d", memberID, remaining, quantity)
		}

		// Bump the entitlement first; if the usage insert below fails the
		// whole transaction rolls back, so failed requests leave no trace.
		row := tx.QueryRow(ctx,
			`UPDATE entitlements SET used = used + $2, version = version + 1
			 WHERE id = $1 RETURNING used, version`,
			id, quantity)
		var newUsed, newVersion int
		if err := row.Scan(&newUsed, &newVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO usages (entitlement_id, member_id, quantity, usage_key,
			                     result_status, result_used, result_version)
			 VALUES ($1, $2, $3, $4, 'active', $5, $6)`,
			id, memberID, quantity, usageKey, newUsed, newVersion); err != nil {
			if isPGCode(err, "23505") {
				// Defensive: the entitlement row lock normally serializes
				// this away; either way the duplicate key rolls the whole
				// transaction back so the used bump never survives.
				return conflict("usage_key_reused", "usageKey %q is already in use for this entitlement", usageKey)
			}
			return err
		}
		updated = &Entitlement{
			ID:        e.ID,
			TeamID:    e.TeamID,
			Type:      e.Type,
			Total:     e.Total,
			Status:    Active,
			ExpiresAt: e.ExpiresAt,
			Used:      newUsed,
			Version:   newVersion,
		}
		record = UsageRecord{MemberID: memberID, Quantity: quantity, UsageKey: usageKey}
		return nil
	})
	if err != nil {
		return nil, UsageRecord{}, false, err
	}
	// Replays return the stored first-response snapshot verbatim; only new
	// consumptions get read-time rules (expiry) applied.
	if replayed {
		return updated, record, true, nil
	}
	return s.present(updated), record, false, nil
}

// Cancel revokes an entitlement. Already-used quantities are preserved.
// Cancelling an already-cancelled or expired grant is rejected with 409.
func (s *PGStore) Cancel(ctx context.Context, id string) (*Entitlement, error) {
	if err := validateTarget(id); err != nil {
		return nil, err
	}

	var updated *Entitlement
	err := s.transact(ctx, func(tx pgx.Tx) error {
		if _, err := s.lockEntitlement(ctx, tx, id); err != nil {
			return err
		}
		row := tx.QueryRow(ctx,
			`UPDATE entitlements SET status='cancelled', version = version + 1
			 WHERE id = $1 RETURNING `+entitlementColumns, id)
		cancelled, scanErr := scanEntitlement(row)
		if scanErr != nil {
			return scanErr
		}
		updated = cancelled
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.present(updated), nil
}

// Detail returns an entitlement and its per-member consumption breakdown.
func (s *PGStore) Detail(ctx context.Context, id string) (*Detail, error) {
	if err := validateTarget(id); err != nil {
		return nil, err
	}

	detail := &Detail{}
	err := s.transactSnapshot(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT `+entitlementColumns+` FROM entitlements WHERE id=$1`, id)
		e, err := scanEntitlement(row)
		if err != nil {
			return err
		}
		detail.Entitlement = *s.present(e)

		rows, err := tx.Query(ctx,
			`SELECT a.member_id,
			        COALESCE(SUM(u.quantity), 0) AS used,
			        COALESCE(array_agg(u.usage_key ORDER BY u.id) FILTER (WHERE u.usage_key IS NOT NULL), ARRAY[]::text[])
			 FROM allocations a
			 LEFT JOIN usages u
			   ON u.entitlement_id = a.entitlement_id AND u.member_id = a.member_id
			 WHERE a.entitlement_id = $1
			 GROUP BY a.member_id
			 ORDER BY a.member_id`, id)
		if err != nil {
			return err
		}
		defer rows.Close()

		detail.Members = []MemberUsage{}
		for rows.Next() {
			var mu MemberUsage
			if err := rows.Scan(&mu.MemberID, &mu.Used, &mu.UsageKeys); err != nil {
				return err
			}
			if mu.UsageKeys == nil {
				mu.UsageKeys = []string{}
			}
			detail.Members = append(detail.Members, mu)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return detail, nil
}

// ConflictError carries the pre-existing entitlement back on create conflicts.
type ConflictError struct {
	APIError
	Existing *Entitlement
}

func (e *ConflictError) Unwrap() error { return &e.APIError }

// UsageKeyConflictError carries the first usage recorded for a reused key.
type UsageKeyConflictError struct {
	APIError
	Existing UsageRecord
}

func (e *UsageKeyConflictError) Unwrap() error { return &e.APIError }
