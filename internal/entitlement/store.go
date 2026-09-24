package entitlement

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors returned by Store implementations.
var (
	// ErrConflict indicates that a grant with the same (tenant, grantId)
	// already exists but carries different grant contents.
	ErrConflict = errors.New("grant already exists with different contents")
	// ErrNotFound indicates that no grant matches the given key.
	ErrNotFound = errors.New("grant not found")
)

// Store persists grants. All operations must be safe for concurrent use.
type Store interface {
	// Put inserts the grant when its (tenant, grantId) key is unused and
	// reports created=true. If a grant with the same key exists, it returns
	// that grant unchanged with created=false: ErrConflict when its contents
	// differ, or nil on an idempotent retry whose four content fields are
	// identical.
	Put(ctx context.Context, g Grant) (grant Grant, created bool, err error)
	// ActiveAt returns the non-cancelled grants of tenant whose half-open
	// validity interval contains at: EffectiveAt <= at < ExpiresAt.
	ActiveAt(ctx context.Context, tenant string, at time.Time) ([]Grant, error)
	// Cancel marks the grant identified by (tenant, grantId) cancelled and
	// records the cancellation time. It returns ErrNotFound when no such
	// grant exists.
	Cancel(ctx context.Context, tenant, grantID string, cancelledAt time.Time) (Grant, error)
}
