// Package entitlement holds the entitlement domain model, request validation
// and the storage contract used by the HTTP layer.
package entitlement

import (
	"context"
	"errors"
	"regexp"
	"time"
)

// KeyPattern constrains entitlement keys: 2-32 characters, starting with a
// lowercase letter, followed by lowercase letters, digits, underscores or
// hyphens.
var KeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,31}$`)

const (
	MinQuantity int64 = 1
	MaxQuantity int64 = 1_000_000
)

// Entitlement is a stored tenant entitlement record.
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

// CreateInput carries a validated creation request.
type CreateInput struct {
	Key       string
	Quantity  int64
	ExpiresAt time.Time
}

// ErrNotFound signals that no entitlement exists for the tenant/key pair.
var ErrNotFound = errors.New("entitlement not found")

// ConflictError is returned when an entitlement with the same key already
// exists but with a different quantity or expiry. Existing holds the
// conflicting record, which must never be modified.
type ConflictError struct {
	Existing *Entitlement
}

func (e *ConflictError) Error() string {
	return "entitlement with this key already exists with different parameters"
}

// Store is the persistence contract for entitlements. Implementations must
// make Create atomic: a unique (tenant, key) constraint guarantees that
// concurrent identical submissions persist exactly one row and never fail
// with a mid-write error.
type Store interface {
	// Create inserts a new entitlement. If an identical record (same key,
	// quantity and expiry) already exists it returns that record with
	// created=false. If a record with the same key but different parameters
	// exists it returns a *ConflictError.
	Create(ctx context.Context, tenant string, input CreateInput) (entitlement *Entitlement, created bool, err error)
	// List returns all entitlements for a tenant ordered by key ascending.
	List(ctx context.Context, tenant string) ([]*Entitlement, error)
	// Get returns a single entitlement or ErrNotFound.
	Get(ctx context.Context, tenant, key string) (*Entitlement, error)
}

// Validate validates a creation request against now (the server's current
// time). Expiry must be an RFC3339 UTC instant strictly in the future.
func Validate(input CreateInput, now time.Time) error {
	if !KeyPattern.MatchString(input.Key) {
		return errors.New("key must match ^[a-z][a-z0-9_-]{1,31}$")
	}
	if input.Quantity < MinQuantity || input.Quantity > MaxQuantity {
		return errors.New("quantity must be an integer between 1 and 1000000")
	}
	if input.ExpiresAt.Location() != time.UTC {
		return errors.New("expiresAt must be UTC")
	}
	if !input.ExpiresAt.After(now) {
		return errors.New("expiresAt must be later than the current time")
	}
	return nil
}
