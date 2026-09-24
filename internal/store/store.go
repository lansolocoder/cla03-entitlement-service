// Package store persists entitlements in PostgreSQL.
package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrConflict reports that an entitlement with the same tenant and key
// already exists but with different quantity or expiry.
var ErrConflict = errors.New("entitlement already exists with different parameters")

// ErrNotFound reports that no entitlement exists for the tenant and key.
var ErrNotFound = errors.New("entitlement not found")

// Entitlement is one tenant entitlement record.
type Entitlement struct {
	ID        string
	Tenant    string
	Key       string
	Quantity  int64
	Consumed  int64
	Reserved  int64
	Status    string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// Store wraps the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

const schema = `
CREATE TABLE IF NOT EXISTS entitlements (
    id UUID PRIMARY KEY,
    tenant TEXT NOT NULL,
    key TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity BETWEEN 1 AND 1000000),
    consumed BIGINT NOT NULL DEFAULT 0 CHECK (consumed >= 0),
    reserved BIGINT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant, key)
)`

// EnsureSchema creates the entitlements table when missing.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

const columns = `id::text, tenant, key, quantity, consumed, reserved, status, created_at, expires_at`

// CreateEntitlement inserts a new entitlement in a single atomic statement.
// It returns created=true when a new record was written. When a record with
// the same tenant and key already exists, nothing is modified: identical
// quantity and expiry yield the existing record with created=false, otherwise
// ErrConflict.
func (s *Store) CreateEntitlement(ctx context.Context, tenant, key string, quantity int64, expiresAt time.Time) (Entitlement, bool, error) {
	id, err := newUUID()
	if err != nil {
		return Entitlement{}, false, err
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiresAt = expiresAt.UTC().Truncate(time.Microsecond)
	var ent Entitlement
	err = s.pool.QueryRow(ctx,
		`INSERT INTO entitlements (id, tenant, key, quantity, expires_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (tenant, key) DO NOTHING
		 RETURNING `+columns,
		id, tenant, key, quantity, expiresAt, now,
	).Scan(&ent.ID, &ent.Tenant, &ent.Key, &ent.Quantity, &ent.Consumed, &ent.Reserved, &ent.Status, &ent.CreatedAt, &ent.ExpiresAt)
	if err == nil {
		return ent, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Entitlement{}, false, err
	}
	existing, err := s.GetEntitlement(ctx, tenant, key)
	if err != nil {
		return Entitlement{}, false, err
	}
	if existing.Quantity == quantity && existing.ExpiresAt.Equal(expiresAt) {
		return existing, false, nil
	}
	return Entitlement{}, false, ErrConflict
}

// GetEntitlement returns one entitlement or ErrNotFound.
func (s *Store) GetEntitlement(ctx context.Context, tenant, key string) (Entitlement, error) {
	var ent Entitlement
	err := s.pool.QueryRow(ctx,
		`SELECT `+columns+` FROM entitlements WHERE tenant = $1 AND key = $2`,
		tenant, key,
	).Scan(&ent.ID, &ent.Tenant, &ent.Key, &ent.Quantity, &ent.Consumed, &ent.Reserved, &ent.Status, &ent.CreatedAt, &ent.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entitlement{}, ErrNotFound
	}
	if err != nil {
		return Entitlement{}, err
	}
	return ent, nil
}

// ListEntitlements returns all entitlements of a tenant ordered by key.
func (s *Store) ListEntitlements(ctx context.Context, tenant string) ([]Entitlement, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+columns+` FROM entitlements WHERE tenant = $1 ORDER BY key ASC`,
		tenant,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ents := []Entitlement{}
	for rows.Next() {
		var ent Entitlement
		if err := rows.Scan(&ent.ID, &ent.Tenant, &ent.Key, &ent.Quantity, &ent.Consumed, &ent.Reserved, &ent.Status, &ent.CreatedAt, &ent.ExpiresAt); err != nil {
			return nil, err
		}
		ents = append(ents, ent)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ents, nil
}

// newUUID generates a random RFC 4122 version 4 UUID.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate uuid: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
