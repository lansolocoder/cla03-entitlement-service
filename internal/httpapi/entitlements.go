package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

// entitlementAPI wires the entitlement service to HTTP routes.
type entitlementAPI struct {
	service entitlement.Service
}

func registerEntitlementRoutes(mux *http.ServeMux, service entitlement.Service) {
	api := &entitlementAPI{service: service}
	mux.HandleFunc("POST /entitlements", api.create)
	mux.HandleFunc("GET /entitlements/{id}", api.detail)
	mux.HandleFunc("POST /entitlements/{id}/allocations", api.allocate)
	mux.HandleFunc("POST /entitlements/{id}/usages", api.use)
	mux.HandleFunc("POST /entitlements/{id}/cancel", api.cancel)
}

type createRequest struct {
	TeamID    string `json:"teamId"`
	Type      string `json:"type"`
	Total     *int   `json:"total"`
	ExpiresAt string `json:"expiresAt"`
}

type allocateRequest struct {
	MemberID string `json:"memberId"`
	Quantity *int   `json:"quantity"`
}

type useRequest struct {
	MemberID string `json:"memberId"`
	Quantity *int   `json:"quantity"`
	UsageKey string `json:"usageKey"`
}

// errorBody is the public shape of every non-2xx response.
type errorBody struct {
	Error errorPayload `json:"error"`
}

type errorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to encode response: %v", err)
		respond(w, http.StatusInternalServerError, `{"error":{"code":"internal_error","message":"internal server error"}}`)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func writeError(w http.ResponseWriter, status int, code, message string, extras map[string]any) {
	payload := map[string]any{"error": errorPayload{Code: code, Message: message}}
	for k, v := range extras {
		payload[k] = v
	}
	writeJSON(w, status, payload)
}

func badRequest(w http.ResponseWriter, code, message string) {
	writeError(w, http.StatusBadRequest, code, message, nil)
}

// decodeRequest parses a JSON body into dst, rejecting unknown fields,
// trailing data and empty bodies.
func decodeRequest(w http.ResponseWriter, r *http.Request, dst any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		badRequest(w, "invalid_json", "request body must be valid JSON matching the API schema")
		return false
	}
	if decoder.More() {
		badRequest(w, "invalid_json", "request body must contain a single JSON object")
		return false
	}
	return true
}

// parseExpiresAt accepts RFC3339 timestamps expressed in UTC ("Z" or a zero
// offset) and normalizes them to UTC, as required by the API contract.
func parseExpiresAt(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Location() != time.UTC {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func (a *entitlementAPI) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.TeamID == "" {
		badRequest(w, "invalid_teamId", "teamId is required")
		return
	}
	if req.Type == "" {
		badRequest(w, "invalid_type", "type is required and must be seat or quota")
		return
	}
	if req.Type != string(entitlement.Seat) && req.Type != string(entitlement.Quota) {
		badRequest(w, "invalid_type", "type must be seat or quota")
		return
	}
	if req.Total == nil {
		badRequest(w, "invalid_total", "total is required")
		return
	}
	if *req.Total <= 0 {
		badRequest(w, "invalid_total", "total must be a positive integer")
		return
	}
	expiresAt, ok := parseExpiresAt(req.ExpiresAt)
	if !ok {
		badRequest(w, "invalid_expiresAt", "expiresAt is required and must be an RFC3339 UTC timestamp")
		return
	}

	result, err := a.service.Create(r.Context(), entitlement.CreateParams{
		TeamID:    req.TeamID,
		Type:      entitlement.Type(req.Type),
		Total:     *req.Total,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		a.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *entitlementAPI) detail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := a.service.Detail(r.Context(), id)
	if err != nil {
		a.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *entitlementAPI) allocate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req allocateRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.MemberID == "" {
		badRequest(w, "invalid_memberId", "memberId is required")
		return
	}
	if req.Quantity == nil {
		badRequest(w, "invalid_quantity", "quantity is required")
		return
	}
	if *req.Quantity <= 0 {
		badRequest(w, "invalid_quantity", "quantity must be a positive integer")
		return
	}

	result, err := a.service.Allocate(r.Context(), id, req.MemberID, *req.Quantity)
	if err != nil {
		a.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// useResponse is the entitlement record with the idempotent usage outcome
// alongside it. Repeated requests return the same record snapshot.
type useResponse struct {
	*entitlement.Entitlement
	MemberID string `json:"memberId"`
	Quantity int    `json:"quantity"`
	UsageKey string `json:"usageKey"`
	Replayed bool   `json:"replayed"`
}

func (a *entitlementAPI) use(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req useRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.MemberID == "" {
		badRequest(w, "invalid_memberId", "memberId is required")
		return
	}
	if req.Quantity == nil {
		badRequest(w, "invalid_quantity", "quantity is required")
		return
	}
	if *req.Quantity <= 0 {
		badRequest(w, "invalid_quantity", "quantity must be a positive integer")
		return
	}
	if req.UsageKey == "" {
		badRequest(w, "invalid_usageKey", "usageKey is required")
		return
	}

	result, record, replayed, err := a.service.Use(r.Context(), id, req.MemberID, *req.Quantity, req.UsageKey)
	if err != nil {
		a.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, useResponse{
		Entitlement: result,
		MemberID:    record.MemberID,
		Quantity:    record.Quantity,
		UsageKey:    record.UsageKey,
		Replayed:    replayed,
	})
}

func (a *entitlementAPI) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := a.service.Cancel(r.Context(), id)
	if err != nil {
		a.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// writeServiceError maps domain failures onto the HTTP contract:
// 400 invalid input, 404 unknown entitlement, 409 business/state conflict.
// Everything else is an opaque 500; no internal details leak out.
func (a *entitlementAPI) writeServiceError(w http.ResponseWriter, err error) {
	var apiErr *entitlement.APIError
	if !errors.As(err, &apiErr) {
		log.Printf("entitlement operation failed: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	switch apiErr.Kind {
	case entitlement.KindInvalid:
		writeError(w, http.StatusBadRequest, apiErr.Code, apiErr.Message, nil)
	case entitlement.KindNotFound:
		writeError(w, http.StatusNotFound, apiErr.Code, apiErr.Message, nil)
	case entitlement.KindConflict:
		extras := map[string]any{}
		var createConflict *entitlement.ConflictError
		if errors.As(err, &createConflict) && createConflict.Existing != nil {
			extras["entitlement"] = createConflict.Existing
		}
		var keyConflict *entitlement.UsageKeyConflictError
		if errors.As(err, &keyConflict) {
			extras["existingUsage"] = map[string]any{
				"memberId": keyConflict.Existing.MemberID,
				"quantity": keyConflict.Existing.Quantity,
				"usageKey": keyConflict.Existing.UsageKey,
			}
		}
		writeError(w, http.StatusConflict, apiErr.Code, apiErr.Message, extras)
	default:
		log.Printf("entitlement operation failed: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
	}
}
