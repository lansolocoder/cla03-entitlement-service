// Package pgstore implements entitlement.Store on PostgreSQL.
package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

// Schema is the database schema required by Store. It is safe to apply
// repeatedly.
const Schema = `
CREATE TABLE IF NOT EXISTS entitlements (
    tenant       text        NOT NULL,
    grant_id     text        NOT NULL,
    feature      text        NOT NULL,
    amount       bigint      NOT NULL,
    effective_at timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL,
    state        text        NOT NULL DEFAULT 'active',
    cancelled_at timestamptz,
    PRIMARY KEY (tenant, grant_id),
    CHECK (expires_at > effective_at),
    CHECK (amount > 0),
    CHECK (feature IN ('seats', 'quota')),
    CHECK (state IN ('active', 'cancelled'))
);
`

const grantColumns = `tenant, grant_id, feature, amount, effective_at, expires_at, state, cancelled_at`

// Store is a PostgreSQL-backed entitlement.Store.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// EnsureSchema applies the required schema.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, Schema)
	return err
}

func scanGrant(row pgx.Row) (entitlement.Grant, error) {
	var g entitlement.Grant
	var state string
	var cancelledAt *time.Time
	if err := row.Scan(&g.Tenant, &g.GrantID, &g.Feature, &g.Amount,
		&g.EffectiveAt, &g.ExpiresAt, &state, &cancelledAt); err != nil {
		return entitlement.Grant{}, err
	}
	g.State = entitlement.State(state)
	return g, nil
}

// Put inserts g, or handles an existing grant under the same (tenant, grantId)
// key per entitlement.Store.
func (s *Store) Put(ctx context.Context, g entitlement.Grant) (entitlement.Grant, bool, error) {
	var result entitlement.Grant
	created := false
	err := s.withSerializable(ctx, func(tx pgx.Tx) error {
		inserted, err := tx.Query(ctx,
			`INSERT INTO entitlements (`+grantColumns+`)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)
			 ON CONFLICT (tenant, grant_id) DO NOTHING
			 RETURNING `+grantColumns,
			g.Tenant, g.GrantID, g.Feature, g.Amount, g.EffectiveAt, g.ExpiresAt, string(g.State))
		if err != nil {
			return err
		}
		if inserted.Next() {
			result, err = scanGrant(inserted)
			inserted.Close()
			if err != nil {
				return err
			}
			created = true
			return nil
		}
		inserted.Close()

		existing, err := scanGrant(tx.QueryRow(ctx,
			`SELECT `+grantColumns+`
			 FROM entitlements WHERE tenant = $1 AND grant_id = $2 FOR UPDATE`,
			g.Tenant, g.GrantID))
		if err != nil {
			return err
		}
		result = existing
		if existing.Feature != g.Feature ||
			existing.Amount != g.Amount ||
			!existing.EffectiveAt.Equal(g.EffectiveAt) ||
			!existing.ExpiresAt.Equal(g.ExpiresAt) {
			return entitlement.ErrConflict
		}
		return nil
	})
	if errors.Is(err, entitlement.ErrConflict) {
		return result, false, err
	}
	if err != nil {
		return entitlement.Grant{}, false, err
	}
	return result, created, nil
}

// ActiveAt returns active grants of tenant valid at at.
func (s *Store) ActiveAt(ctx context.Context, tenant string, at time.Time) ([]entitlement.Grant, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+grantColumns+`
		 FROM entitlements
		 WHERE tenant = $1
		   AND state = 'active'
		   AND effective_at <= $2
		   AND expires_at > $2
		 ORDER BY grant_id`,
		tenant, at)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	grants := make([]entitlement.Grant, 0)
	for rows.Next() {
		g, err := scanGrant(rows)
		if err != nil {
			return nil, err
		}
		grants = append(grants, g)
	}
	return grants, rows.Err()
}

// Cancel marks the grant cancelled and records the cancellation time.
func (s *Store) Cancel(ctx context.Context, tenant, grantID string, cancelledAt time.Time) (entitlement.Grant, error) {
	g, err := scanGrant(s.pool.QueryRow(ctx,
		`UPDATE entitlements
		 SET state = 'cancelled', cancelled_at = $3
		 WHERE tenant = $1 AND grant_id = $2
		 RETURNING `+grantColumns,
		tenant, grantID, cancelledAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return entitlement.Grant{}, entitlement.ErrNotFound
	}
	if err != nil {
		return entitlement.Grant{}, err
	}
	return g, nil
}

// withSerializable runs fn inside a SERIALIZABLE transaction, retrying on
// serialization failures or deadlocks.
func (s *Store) withSerializable(ctx context.Context, fn func(pgx.Tx) error) error {
	for {
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			if err := tx.Commit(ctx); err != nil {
				_ = tx.Rollback(ctx)
				if isRetryable(err) {
					continue
				}
				return err
			}
			return nil
		}
		_ = tx.Rollback(ctx)
		if isRetryable(err) {
			continue
		}
		return err
	}
}

func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 40001 serialization_failure, 40P01 deadlock_detected.
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}
