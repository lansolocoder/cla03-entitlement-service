// Package grants holds the grant-based entitlement domain model and storage.
package grants

import (
	"context"
	"errors"
	"time"
)

// Feature is the granted feature family.
type Feature string

const (
	FeatureSeats Feature = "seats"
	FeatureQuota Feature = "quota"
)

// Valid reports whether f is an accepted feature value.
func (f Feature) Valid() bool {
	return f == FeatureSeats || f == FeatureQuota
}

// State is the lifecycle state of a grant.
type State string

const (
	StateActive    State = "active"
	StateCancelled State = "cancelled"
)

// Grant is one granted entitlement window. The validity interval is
// half-open: [EffectiveAt, ExpiresAt).
type Grant struct {
	Tenant      string
	GrantID     string
	Feature     Feature
	Amount      int64
	EffectiveAt time.Time
	ExpiresAt   time.Time
	State       State
	CancelledAt *time.Time
}

// Sentinel storage errors.
var (
	// ErrConflict means a grant with the same (tenant, grantId) already
	// exists but carries different grant parameters.
	ErrConflict = errors.New("grant id already used with different parameters")
	// ErrNotFound means no grant exists for the (tenant, grantId) pair.
	ErrNotFound = errors.New("grant not found")
)

// Store persists grants.
type Store interface {
	// Create inserts a new grant. If a grant with the same key already
	// exists it returns the existing grant and true for reused (identical
	// parameters: idempotent retry), or ErrConflict when any parameter
	// differs. A new grant is returned with reused == false.
	Create(ctx context.Context, g Grant) (stored Grant, reused bool, err error)

	// ListActiveAt returns non-cancelled grants of the tenant whose
	// half-open interval contains at: effective_at <= at < expires_at,
	// ordered by effective_at then grant_id for stable output.
	ListActiveAt(ctx context.Context, tenant string, at time.Time) ([]Grant, error)

	// Cancel marks the grant cancelled at cancelledAt and returns the
	// stored grant. It returns ErrNotFound when no grant exists for the
	// key. Cancelling an already-cancelled grant is idempotent.
	Cancel(ctx context.Context, tenant, grantID string, cancelledAt time.Time) (Grant, error)
}
