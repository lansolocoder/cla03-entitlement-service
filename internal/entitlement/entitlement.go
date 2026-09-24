// Package entitlement implements the lifecycle of team service entitlements:
// seat and quota grants, member allocations, idempotent usage, cancellation
// and expiry. All state changes happen inside serializable database
// transactions so that concurrent requests and partial failures never leave
// an entitlement, allocation or usage record half-updated.
package entitlement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Type identifies what an entitlement grants.
type Type string

const (
	// Seat grants a fixed number of seats that members occupy.
	Seat Type = "seat"
	// Quota grants a fixed amount of metered quota.
	Quota Type = "quota"
)

// Status is the lifecycle state of an entitlement.
type Status string

const (
	Active    Status = "active"
	Cancelled Status = "cancelled"
	Expired   Status = "expired"
)

// Entitlement is a team grant as exposed by the API.
type Entitlement struct {
	ID        string    `json:"id"`
	TeamID    string    `json:"teamId"`
	Type      Type      `json:"type"`
	Total     int       `json:"total"`
	Status    Status    `json:"status"`
	ExpiresAt time.Time `json:"expiresAt"`
	Used      int       `json:"used"`
	Version   int       `json:"version"`
}

// MemberUsage is the per-member breakdown returned by the detail endpoint.
type MemberUsage struct {
	MemberID  string   `json:"memberId"`
	Used      int      `json:"used"`
	UsageKeys []string `json:"usageKeys"`
}

// Detail is an entitlement together with its allocations and usages.
type Detail struct {
	Entitlement
	Members []MemberUsage `json:"members"`
}

// UsageRecord is one successful consumption of an entitlement.
type UsageRecord struct {
	MemberID string
	Quantity int
	UsageKey string
}

// CreateParams carries a create-entitlement request.
type CreateParams struct {
	TeamID    string
	Type      Type
	Total     int
	ExpiresAt time.Time
}

// Service is the entitlement lifecycle boundary used by the HTTP layer.
type Service interface {
	Create(ctx context.Context, params CreateParams) (*Entitlement, error)
	Get(ctx context.Context, id string) (*Entitlement, error)
	Allocate(ctx context.Context, id, memberID string, quantity int) (*Entitlement, error)
	Use(ctx context.Context, id, memberID string, quantity int, usageKey string) (*Entitlement, UsageRecord, bool, error)
	Cancel(ctx context.Context, id string) (*Entitlement, error)
	Detail(ctx context.Context, id string) (*Detail, error)
}

// ErrorKind categorizes service errors for HTTP mapping.
type ErrorKind int

const (
	KindInvalid  ErrorKind = iota // bad input (maps to 400)
	KindNotFound                  // unknown entitlement id (maps to 404)
	KindConflict                  // business rule violation (maps to 409)
)

// APIError is an explainable service failure. Failed operations never mutate
// state; Code identifies the exact rule that was violated.
type APIError struct {
	Kind    ErrorKind
	Code    string
	Message string
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func invalid(code, format string, args ...any) error {
	return &APIError{Kind: KindInvalid, Code: code, Message: fmt.Sprintf(format, args...)}
}

func conflict(code, format string, args ...any) error {
	return &APIError{Kind: KindConflict, Code: code, Message: fmt.Sprintf(format, args...)}
}

var errNotFound = &APIError{Kind: KindNotFound, Code: "entitlement_not_found", Message: "entitlement does not exist"}

// validateKey rejects empty keys and normalizes surrounding whitespace away
// from callers: keys are compared exactly as supplied, but request bodies
// containing only whitespace are treated as missing rather than as a valid
// identifier.
func validateKey(field, value string) error {
	if value == "" {
		return invalid("invalid_"+field, "%s is required", field)
	}
	if strings.TrimSpace(value) == "" {
		return invalid("invalid_"+field, "%s must not be blank", field)
	}
	return nil
}

func validateCreate(params CreateParams) error {
	if err := validateKey("teamId", params.TeamID); err != nil {
		return err
	}
	if params.Type != Seat && params.Type != Quota {
		return invalid("invalid_type", "type must be seat or quota")
	}
	if params.Total <= 0 {
		return invalid("invalid_total", "total must be a positive integer")
	}
	if params.ExpiresAt.Location() != time.UTC {
		return invalid("invalid_expiresAt", "expiresAt must be RFC3339 UTC")
	}
	return nil
}

func validateTarget(id string) error {
	return validateKey("id", id)
}

func validateAllocate(memberID string, quantity int) error {
	if err := validateKey("memberId", memberID); err != nil {
		return err
	}
	if quantity <= 0 {
		return invalid("invalid_quantity", "quantity must be a positive integer")
	}
	return nil
}

func validateUse(memberID string, quantity int, usageKey string) error {
	if err := validateAllocate(memberID, quantity); err != nil {
		return err
	}
	return validateKey("usageKey", usageKey)
}

// effectiveStatus derives the externally visible status. Expiry is computed
// on read so no background job is required and the rule cannot lag reality.
func effectiveStatus(status Status, expiresAt, now time.Time) Status {
	if status == Active && !now.Before(expiresAt) {
		return Expired
	}
	return status
}

// AsAPIError unwraps an error chain to its APIError, if any.
func AsAPIError(err error) (*APIError, bool) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr, true
	}
	return nil, false
}
