package grants

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the canonical schema for grant storage. It is idempotent and
// applied at startup and before integration tests.
//
//go:embed schema.sql
var Schema string

// EnsureSchema applies the idempotent grant schema within conn's database.
func EnsureSchema(ctx context.Context, conn *pgxpool.Pool) error {
	_, err := conn.Exec(ctx, Schema)
	return err
}

// PostgresStore persists grants in PostgreSQL.
type PostgresStore struct {
	Pool *pgxpool.Pool
}

// NewPostgresStore returns a PostgresStore backed by pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{Pool: pool}
}

// Create inserts a new grant or detects an idempotent retry / conflict.
// Everything runs in one transaction: the insert is a single atomic
// statement, so a failed request can never leave a partial row.
func (s *PostgresStore) Create(ctx context.Context, g Grant) (Grant, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Grant{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// ON CONFLICT DO NOTHING makes RETURNING yield no row when the key
	// already exists; that case is handled by the follow-up read.
	stored, err := scanGrant(tx.QueryRow(ctx, `
		INSERT INTO grants (tenant_id, grant_id, feature, amount, effective_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, grant_id) DO NOTHING
		RETURNING tenant_id, grant_id, feature, amount, effective_at, expires_at, state, cancelled_at`,
		g.Tenant, g.GrantID, string(g.Feature), g.Amount, g.EffectiveAt, g.ExpiresAt))
	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return Grant{}, false, err
		}
		return stored, false, nil
	case !errors.Is(err, ErrNotFound):
		return Grant{}, false, err
	}

	// Key already existed: compare against the authoritative stored row.
	existing, err := scanGrant(tx.QueryRow(ctx, `
		SELECT tenant_id, grant_id, feature, amount, effective_at, expires_at, state, cancelled_at
		FROM grants
		WHERE tenant_id = $1 AND grant_id = $2`, g.Tenant, g.GrantID))
	if err != nil {
		return Grant{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Grant{}, false, err
	}
	if !existing.sameAs(g) {
		return Grant{}, false, ErrConflict
	}
	return existing, true, nil
}

// ListActiveAt lists effective, non-cancelled grants at instant at.
func (s *PostgresStore) ListActiveAt(ctx context.Context, tenant string, at time.Time) ([]Grant, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT tenant_id, grant_id, feature, amount, effective_at, expires_at, state, cancelled_at
		FROM grants
		WHERE tenant_id = $1
		  AND state = 'active'
		  AND effective_at <= $2
		  AND $2 < expires_at
		ORDER BY effective_at, grant_id`, tenant, at)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Grant
	for rows.Next() {
		g, err := scanGrant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Cancel marks a grant cancelled. Cancelling an already-cancelled grant is
// idempotent; an unknown key returns ErrNotFound.
func (s *PostgresStore) Cancel(ctx context.Context, tenant, grantID string, cancelledAt time.Time) (Grant, error) {
	row := s.Pool.QueryRow(ctx, `
		UPDATE grants
		SET state = 'cancelled', cancelled_at = COALESCE(cancelled_at, $3)
		WHERE tenant_id = $1 AND grant_id = $2
		RETURNING tenant_id, grant_id, feature, amount, effective_at, expires_at, state, cancelled_at`,
		tenant, grantID, cancelledAt)
	g, err := scanGrant(row)
	if errors.Is(err, ErrNotFound) {
		return Grant{}, ErrNotFound
	}
	return g, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanGrant(row rowScanner) (Grant, error) {
	var g Grant
	var feature, state string
	if err := row.Scan(&g.Tenant, &g.GrantID, &feature, &g.Amount,
		&g.EffectiveAt, &g.ExpiresAt, &state, &g.CancelledAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Grant{}, ErrNotFound
		}
		return Grant{}, err
	}
	g.Feature = Feature(feature)
	g.State = State(state)
	return g, nil
}

// sameAs compares the four idempotency-key payload fields plus tenant
// (tenant is fixed by the key anyway). Times compare at the stored
// precision; callers normalize input to UTC seconds before persisting.
func (g Grant) sameAs(other Grant) bool {
	return g.Feature == other.Feature &&
		g.Amount == other.Amount &&
		g.EffectiveAt.Equal(other.EffectiveAt) &&
		g.ExpiresAt.Equal(other.ExpiresAt)
}
