// Package pgstore implements entitlement.Store on top of PostgreSQL.
package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

// Store is a PostgreSQL-backed entitlement.Store.
type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// New wraps a connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, now: func() time.Time { return time.Now().UTC() }}
}

// EnsureSchema creates the entitlements table and its unique constraint if
// they do not exist yet. The unique (tenant, key) index is the guarantee
// that concurrent creates can never persist duplicate rows.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS entitlements (
	id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
	tenant      text        NOT NULL,
	key         text        NOT NULL,
	quantity    bigint      NOT NULL CHECK (quantity BETWEEN 1 AND 1000000),
	consumed    bigint      NOT NULL DEFAULT 0 CHECK (consumed >= 0),
	reserved    bigint      NOT NULL DEFAULT 0 CHECK (reserved >= 0),
	status      text        NOT NULL DEFAULT 'active',
	created_at  timestamptz NOT NULL DEFAULT now(),
	expires_at  timestamptz NOT NULL,
	UNIQUE (tenant, key)
);`)
	return err
}

const selectColumns = `id, tenant, key, quantity, consumed, reserved, status, created_at, expires_at`

func scanEntitlement(row pgx.Row) (*entitlement.Entitlement, error) {
	var e entitlement.Entitlement
	if err := row.Scan(
		&e.ID, &e.Tenant, &e.Key, &e.Quantity, &e.Consumed, &e.Reserved,
		&e.Status, &e.CreatedAt, &e.ExpiresAt,
	); err != nil {
		return nil, err
	}
	e.CreatedAt = e.CreatedAt.UTC()
	e.ExpiresAt = e.ExpiresAt.UTC()
	return &e, nil
}

// Create inserts an entitlement atomically. The unique (tenant, key)
// constraint makes the insert the single decision point: under concurrent
// submissions exactly one transaction inserts and the others take the
// conflict path, so no half-written or duplicate rows can survive.
func (s *Store) Create(ctx context.Context, tenant string, input entitlement.CreateInput) (*entitlement.Entitlement, bool, error) {
	// timestamptz stores microsecond precision; truncate before insert so a
	// repeated nanosecond-precision expiry compares equal to the stored row.
	expiresAt := input.ExpiresAt.UTC().Truncate(time.Microsecond)
	createdAt := s.now().UTC()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)

	inserted, err := scanEntitlement(tx.QueryRow(ctx, `
INSERT INTO entitlements (tenant, key, quantity, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant, key) DO NOTHING
RETURNING `+selectColumns, tenant, input.Key, input.Quantity, createdAt, expiresAt))
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return inserted, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}

	// The INSERT only reports zero rows after a conflict once the winning
	// transaction has committed or aborted, so this read is always current.
	existing, err := scanEntitlement(tx.QueryRow(ctx,
		`SELECT `+selectColumns+` FROM entitlements WHERE tenant = $1 AND key = $2`,
		tenant, input.Key))
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	if existing.Quantity == input.Quantity && existing.ExpiresAt.Equal(expiresAt) {
		return existing, false, nil
	}
	return nil, false, &entitlement.ConflictError{Existing: existing}
}

// List returns all entitlements for a tenant ordered by key ascending.
func (s *Store) List(ctx context.Context, tenant string) ([]*entitlement.Entitlement, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+selectColumns+` FROM entitlements WHERE tenant = $1 ORDER BY key ASC`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*entitlement.Entitlement
	for rows.Next() {
		e, err := scanEntitlement(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	return items, rows.Err()
}

// Get returns one entitlement or entitlement.ErrNotFound.
func (s *Store) Get(ctx context.Context, tenant, key string) (*entitlement.Entitlement, error) {
	e, err := scanEntitlement(s.pool.QueryRow(ctx,
		`SELECT `+selectColumns+` FROM entitlements WHERE tenant = $1 AND key = $2`,
		tenant, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, entitlement.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return e, nil
}
