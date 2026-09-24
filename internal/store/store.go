package store

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Migrate creates the schema. It is idempotent and safe to run on every start.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

const (
	StatusActive    = "active"
	StatusCancelled = "cancelled"
	StatusExpired   = "expired"

	TypeSeat  = "seat"
	TypeQuota = "quota"
)

var (
	// ErrNotFound indicates the entitlement does not exist.
	ErrNotFound = errors.New("entitlement not found")
	// ErrAlreadyExists indicates a duplicate create; the existing record is attached.
	ErrAlreadyExists = errors.New("entitlement already exists")
	// ErrCapacity means total would be exceeded by an allocation.
	ErrCapacity = errors.New("allocation exceeds entitlement total")
	// ErrInsufficient means a member tried to use more than their unused allocation.
	ErrInsufficient = errors.New("usage exceeds member unused allowance")
	// ErrUsageKeyConflict means a usageKey was reused with different member or amount.
	ErrUsageKeyConflict = errors.New("usageKey already recorded with different parameters")
	// ErrNotOperable means the entitlement is cancelled or expired. The effective
	// status is attached for reporting.
	ErrNotOperable = errors.New("entitlement is not operable")
)

// Entitlement is the stored entitlement record. Status is effective: it is
// "expired" once now reaches ExpiresAt even though the stored value stays
// "active".
type Entitlement struct {
	ID        string    `json:"id"`
	TeamID    string    `json:"teamId"`
	Type      string    `json:"type"`
	Total     int64     `json:"total"`
	Used      int64     `json:"used"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expiresAt"`
	Version   int64     `json:"version"`
}

// MemberUsage is one member's usage detail.
type MemberUsage struct {
	MemberID  string   `json:"memberId"`
	Used      int64    `json:"used"`
	UsageKeys []string `json:"usageKeys"`
}

// Details is an entitlement together with per-member usage detail.
type Details struct {
	Entitlement Entitlement
	Members     []MemberUsage
}

// ConflictError carries the effective status for ErrNotOperable.
type ConflictError struct {
	Err      error
	Status   string
	Existing *Entitlement
}

func (e *ConflictError) Error() string { return e.Err.Error() }
func (e *ConflictError) Unwrap() error { return e.Err }

// Store is the entitlement data store.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// effectiveStatus derives the externally visible status at time now.
func effectiveStatus(stored string, expiresAt, now time.Time) string {
	if stored == StatusCancelled {
		return StatusCancelled
	}
	if !now.UTC().Before(expiresAt.UTC()) {
		return StatusExpired
	}
	return StatusActive
}

func scanEntitlement(row pgx.Row, now time.Time) (Entitlement, error) {
	var e Entitlement
	var storedStatus string
	var expiresAt time.Time
	if err := row.Scan(&e.ID, &e.TeamID, &e.Type, &e.Total, &e.Used,
		&storedStatus, &expiresAt, &e.Version); err != nil {
		return Entitlement{}, err
	}
	e.ExpiresAt = expiresAt.UTC()
	e.Status = effectiveStatus(storedStatus, e.ExpiresAt, now)
	return e, nil
}

const entitlementColumns = `id, team_id, type, total, used, status, expires_at, version`

// Create inserts a new entitlement. On a duplicate (team, type, expiresAt) it
// returns ErrAlreadyExists with the existing record.
func (s *Store) Create(ctx context.Context, teamID, typ string, total int64, expiresAt time.Time) (Entitlement, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Entitlement{}, err
	}
	row := tx.QueryRow(ctx,
		`INSERT INTO entitlements (team_id, type, total, expires_at)
		 VALUES ($1, $2, $3, $4)
		 RETURNING `+entitlementColumns,
		teamID, typ, total, expiresAt.UTC())
	e, err := scanEntitlement(row, time.Now().UTC())
	if err != nil {
		// The unique violation aborts the transaction, so it can only be
		// rolled back; look the winner up on a fresh connection.
		_ = tx.Rollback(ctx)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			existing, gerr := s.getExisting(ctx, teamID, typ, expiresAt)
			if gerr != nil {
				return Entitlement{}, gerr
			}
			return Entitlement{}, &ConflictError{Err: ErrAlreadyExists, Existing: &existing}
		}
		return Entitlement{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Entitlement{}, err
	}
	return e, nil
}

func (s *Store) getExisting(ctx context.Context, teamID, typ string, expiresAt time.Time) (Entitlement, error) {
	return scanEntitlement(s.pool.QueryRow(ctx,
		`SELECT `+entitlementColumns+`
		 FROM entitlements
		 WHERE team_id = $1 AND type = $2 AND expires_at = $3`,
		teamID, typ, expiresAt.UTC()), time.Now().UTC())
}

// lockEntitlement locks the entitlement row for the duration of tx and returns
// the record with its effective status at now.
func lockEntitlement(ctx context.Context, tx pgx.Tx, id string, now time.Time) (Entitlement, string, error) {
	var e Entitlement
	var storedStatus string
	var expiresAt time.Time
	err := tx.QueryRow(ctx,
		`SELECT `+entitlementColumns+` FROM entitlements WHERE id = $1 FOR UPDATE`, id).
		Scan(&e.ID, &e.TeamID, &e.Type, &e.Total, &e.Used,
			&storedStatus, &expiresAt, &e.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entitlement{}, "", ErrNotFound
	}
	if err != nil {
		return Entitlement{}, "", err
	}
	e.ExpiresAt = expiresAt.UTC()
	e.Status = effectiveStatus(storedStatus, e.ExpiresAt, now)
	return e, storedStatus, nil
}

// Allocate grants amount seats/quota to a member. It is rejected when the
// entitlement is not active or the total would be exceeded; in either case no
// row changes.
func (s *Store) Allocate(ctx context.Context, id, memberID string, amount int64, now time.Time) (Entitlement, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Entitlement{}, err
	}
	defer tx.Rollback(ctx)

	e, _, err := lockEntitlement(ctx, tx, id, now)
	if err != nil {
		return Entitlement{}, err
	}
	if e.Status != StatusActive {
		return Entitlement{}, &ConflictError{Err: ErrNotOperable, Status: e.Status}
	}
	if e.Used+amount > e.Total || e.Used+amount < 0 {
		return Entitlement{}, &ConflictError{Err: ErrCapacity, Status: e.Status}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO allocations (entitlement_id, member_id, amount)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (entitlement_id, member_id)
		 DO UPDATE SET amount = allocations.amount + EXCLUDED.amount`,
		id, memberID, amount); err != nil {
		return Entitlement{}, err
	}
	updated, err := scanEntitlement(tx.QueryRow(ctx,
		`UPDATE entitlements SET used = used + $2, version = version + 1
		 WHERE id = $1 RETURNING `+entitlementColumns, id, amount), now)
	if err != nil {
		return Entitlement{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Entitlement{}, err
	}
	return updated, nil
}

// Consume records a usage with its idempotency key. A repeated usageKey replays
// the first result and never accumulates twice. A reused key with a different
// member or amount is a conflict.
func (s *Store) Consume(ctx context.Context, id, memberID string, amount int64, usageKey string, now time.Time) (Entitlement, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Entitlement{}, false, err
	}
	defer tx.Rollback(ctx)

	e, _, err := lockEntitlement(ctx, tx, id, now)
	if err != nil {
		return Entitlement{}, false, err
	}

	// Idempotency check runs before status checks: a replay of a previously
	// accepted usage must always return the first result, even after expiry.
	var existingMember string
	var existingAmount, resultUsed, resultVersion int64
	err = tx.QueryRow(ctx,
		`SELECT member_id, amount, result_used, result_version
		 FROM usages WHERE entitlement_id = $1 AND usage_key = $2`,
		id, usageKey).Scan(&existingMember, &existingAmount, &resultUsed, &resultVersion)
	switch {
	case err == nil:
		if existingMember != memberID || existingAmount != amount {
			return Entitlement{}, false, &ConflictError{Err: ErrUsageKeyConflict, Status: e.Status}
		}
		first := e
		first.Status = StatusActive // a usage is only ever accepted while active
		first.Used = resultUsed
		first.Version = resultVersion
		return first, true, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Entitlement{}, false, err
	}

	if e.Status != StatusActive {
		return Entitlement{}, false, &ConflictError{Err: ErrNotOperable, Status: e.Status}
	}

	var allocated, memberUsed int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE((SELECT amount FROM allocations WHERE entitlement_id = $1 AND member_id = $2), 0),
		        COALESCE((SELECT sum(amount) FROM usages WHERE entitlement_id = $1 AND member_id = $2), 0)`,
		id, memberID).Scan(&allocated, &memberUsed); err != nil {
		return Entitlement{}, false, err
	}
	if amount > allocated-memberUsed || allocated-memberUsed < 0 {
		return Entitlement{}, false, &ConflictError{Err: ErrInsufficient, Status: e.Status}
	}

	newUsed := e.Used + amount
	newVersion := e.Version + 1
	if _, err := tx.Exec(ctx,
		`INSERT INTO usages (entitlement_id, member_id, usage_key, amount, result_used, result_version)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		id, memberID, usageKey, amount, newUsed, newVersion); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return Entitlement{}, false, &ConflictError{Err: ErrUsageKeyConflict, Status: e.Status}
		}
		return Entitlement{}, false, err
	}
	updated, err := scanEntitlement(tx.QueryRow(ctx,
		`UPDATE entitlements SET used = $2, version = $3
		 WHERE id = $1 RETURNING `+entitlementColumns, id, newUsed, newVersion), now)
	if err != nil {
		return Entitlement{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Entitlement{}, false, err
	}
	return updated, false, nil
}

// Cancel marks an active entitlement cancelled, preserving used amounts.
// Cancelling an already-cancelled entitlement is idempotent; cancelling an
// expired one is rejected.
func (s *Store) Cancel(ctx context.Context, id string, now time.Time) (Entitlement, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Entitlement{}, err
	}
	defer tx.Rollback(ctx)

	e, _, err := lockEntitlement(ctx, tx, id, now)
	if err != nil {
		return Entitlement{}, err
	}
	switch e.Status {
	case StatusCancelled:
		return e, nil
	case StatusExpired:
		return Entitlement{}, &ConflictError{Err: ErrNotOperable, Status: e.Status}
	}
	updated, err := scanEntitlement(tx.QueryRow(ctx,
		`UPDATE entitlements SET status = 'cancelled', version = version + 1
		 WHERE id = $1 RETURNING `+entitlementColumns, id), now)
	if err != nil {
		return Entitlement{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Entitlement{}, err
	}
	return updated, nil
}

// Get returns the entitlement and per-member usage detail. Expired records
// remain readable.
func (s *Store) Get(ctx context.Context, id string, now time.Time) (Details, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Details{}, err
	}
	defer tx.Rollback(ctx)

	e, _, err := lockEntitlement(ctx, tx, id, now)
	if err != nil {
		return Details{}, err
	}

	rows, err := tx.Query(ctx,
		`SELECT m.member_id,
		        COALESCE(u.used, 0) AS used,
		        COALESCE(u.keys, ARRAY[]::text[]) AS keys
		 FROM (
		     SELECT member_id FROM allocations WHERE entitlement_id = $1
		     UNION
		     SELECT member_id FROM usages WHERE entitlement_id = $1
		 ) m
		 LEFT JOIN (
		     SELECT member_id, sum(amount) AS used,
		            array_agg(usage_key ORDER BY created_at, id) AS keys
		     FROM usages WHERE entitlement_id = $1
		     GROUP BY member_id
		 ) u ON u.member_id = m.member_id
		 ORDER BY m.member_id`, id)
	if err != nil {
		return Details{}, err
	}
	defer rows.Close()

	members := []MemberUsage{}
	for rows.Next() {
		var mu MemberUsage
		if err := rows.Scan(&mu.MemberID, &mu.Used, &mu.UsageKeys); err != nil {
			return Details{}, err
		}
		members = append(members, mu)
	}
	if err := rows.Err(); err != nil {
		return Details{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Details{}, err
	}
	return Details{Entitlement: e, Members: members}, nil
}

const uniqueViolation = "23505"
