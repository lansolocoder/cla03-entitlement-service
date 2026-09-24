// Package entitlement defines the grant domain model, request validation and
// the persistence interface used by the HTTP layer.
package entitlement

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// State is the lifecycle state of a grant.
type State string

const (
	StateActive    State = "active"
	StateCancelled State = "cancelled"
)

// Grant is a persisted entitlement grant. Validity is the half-open interval
// [EffectiveAt, ExpiresAt).
type Grant struct {
	Tenant      string
	GrantID     string
	Feature     string
	Amount      int64
	EffectiveAt time.Time
	ExpiresAt   time.Time
	State       State
}

// FormatTime renders t as an RFC 3339 UTC timestamp with second precision and
// a trailing Z.
func FormatTime(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(time.RFC3339)
}

// normalizeRFC3339 parses s as strict RFC 3339 and returns it as UTC truncated
// to whole seconds, so timestamps echo back with second precision and a Z.
func normalizeRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC().Truncate(time.Second), nil
}

// CreateInput is the validated body of a grant creation request.
type CreateInput struct {
	Tenant      string
	GrantID     string
	Feature     string
	Amount      int64
	EffectiveAt time.Time
	ExpiresAt   time.Time
}

// Grant builds the domain grant from the validated input.
func (in CreateInput) Grant() Grant {
	return Grant{
		Tenant:      in.Tenant,
		GrantID:     in.GrantID,
		Feature:     in.Feature,
		Amount:      in.Amount,
		EffectiveAt: in.EffectiveAt,
		ExpiresAt:   in.ExpiresAt,
		State:       StateActive,
	}
}

type createRequest struct {
	GrantID     string          `json:"grantId"`
	Feature     string          `json:"feature"`
	Amount      json.RawMessage `json:"amount"`
	EffectiveAt string          `json:"effectiveAt"`
	ExpiresAt   string          `json:"expiresAt"`
}

// parseAmount validates that raw carries a JSON integer. It rejects missing
// values, fractional/exponent notation, strings and booleans.
func parseAmount(raw json.RawMessage) (int64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return 0, fmt.Errorf("amount is required")
	}
	// A JSON number token starts with '-' or a digit; this rejects strings,
	// booleans and other types that would otherwise unmarshal into
	// json.Number (which is itself a string type).
	if first := trimmed[0]; first != '-' && (first < '0' || first > '9') {
		return 0, fmt.Errorf("amount must be a positive integer")
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, fmt.Errorf("amount must be a positive integer")
	}
	amount, err := number.Int64()
	if err != nil {
		return 0, fmt.Errorf("amount must be a positive integer")
	}
	if amount <= 0 {
		return 0, fmt.Errorf("amount must be a positive integer")
	}
	return amount, nil
}

// ParseCreateRequest validates raw JSON for the create endpoint. It returns a
// user-facing error message for every validation failure.
func ParseCreateRequest(tenant string, raw []byte) (CreateInput, error) {
	var req createRequest
	if err := decodeStrict(raw, &req); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return CreateInput{}, fmt.Errorf("invalid JSON body: %v", err)
		}
		return CreateInput{}, fmt.Errorf("invalid JSON body")
	}
	if tenant == "" {
		return CreateInput{}, fmt.Errorf("tenant is required")
	}
	if strings.TrimSpace(req.GrantID) == "" {
		return CreateInput{}, fmt.Errorf("grantId is required")
	}
	if req.Feature != "seats" && req.Feature != "quota" {
		return CreateInput{}, fmt.Errorf("feature must be one of: seats, quota")
	}
	amount, err := parseAmount(req.Amount)
	if err != nil {
		return CreateInput{}, err
	}
	if strings.TrimSpace(req.EffectiveAt) == "" {
		return CreateInput{}, fmt.Errorf("effectiveAt is required")
	}
	effectiveAt, err := normalizeRFC3339(req.EffectiveAt)
	if err != nil {
		return CreateInput{}, fmt.Errorf("effectiveAt must be an RFC 3339 timestamp")
	}
	if strings.TrimSpace(req.ExpiresAt) == "" {
		return CreateInput{}, fmt.Errorf("expiresAt is required")
	}
	expiresAt, err := normalizeRFC3339(req.ExpiresAt)
	if err != nil {
		return CreateInput{}, fmt.Errorf("expiresAt must be an RFC 3339 timestamp")
	}
	if !expiresAt.After(effectiveAt) {
		return CreateInput{}, fmt.Errorf("expiresAt must be later than effectiveAt")
	}
	return CreateInput{
		Tenant:      tenant,
		GrantID:     req.GrantID,
		Feature:     req.Feature,
		Amount:      amount,
		EffectiveAt: effectiveAt,
		ExpiresAt:   expiresAt,
	}, nil
}

type cancelRequest struct {
	CancelledAt string `json:"cancelledAt"`
}

// ParseCancelRequest validates raw JSON for the cancel endpoint.
func ParseCancelRequest(raw []byte) (time.Time, error) {
	var req cancelRequest
	if err := decodeStrict(raw, &req); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return time.Time{}, fmt.Errorf("invalid JSON body: %v", err)
		}
		return time.Time{}, fmt.Errorf("invalid JSON body")
	}
	if strings.TrimSpace(req.CancelledAt) == "" {
		return time.Time{}, fmt.Errorf("cancelledAt is required")
	}
	at, err := normalizeRFC3339(req.CancelledAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("cancelledAt must be an RFC 3339 timestamp")
	}
	return at, nil
}

// ParseAtParam validates the ?at= query parameter.
func ParseAtParam(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("at query parameter is required")
	}
	at, err := normalizeRFC3339(value)
	if err != nil {
		return time.Time{}, fmt.Errorf("at must be an RFC 3339 timestamp")
	}
	return at, nil
}
