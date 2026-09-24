package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

// Store is the subset of *store.Store used by the handlers.
type Store interface {
	Create(ctx context.Context, teamID, typ string, total int64, expiresAt time.Time) (store.Entitlement, error)
	Allocate(ctx context.Context, id, memberID string, amount int64, now time.Time) (store.Entitlement, error)
	Consume(ctx context.Context, id, memberID string, amount int64, usageKey string, now time.Time) (store.Entitlement, bool, error)
	Cancel(ctx context.Context, id string, now time.Time) (store.Entitlement, error)
	Get(ctx context.Context, id string, now time.Time) (store.Details, error)
}

func New(checkDatabase func(context.Context) error, st Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := checkDatabase(ctx); err != nil {
			respond(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
			return
		}
		respond(w, http.StatusOK, `{"status":"ready"}`)
	})

	mux.HandleFunc("POST /entitlements", createEntitlement(st))
	// The collection is not listable: GET stays 404 like any unknown path.
	mux.HandleFunc("GET /entitlements", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "unknown path")
	})
	mux.HandleFunc("POST /entitlements/{id}/allocations", allocate(st))
	mux.HandleFunc("POST /entitlements/{id}/usages", consume(st))
	mux.HandleFunc("POST /entitlements/{id}/cancel", cancelEntitlement(st))
	mux.HandleFunc("GET /entitlements/{id}", getEntitlement(st))

	return jsonErrors(mux)
}

// jsonErrors replaces the mux's plain-text 404/405 responses with JSON bodies so
// every response the service emits is JSON. Application responses already set
// application/json and pass through untouched.
func jsonErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if rec.sawJSON {
			return
		}
		switch rec.status {
		case http.StatusNotFound:
			writeError(w, http.StatusNotFound, "not_found", "unknown path")
		case http.StatusMethodNotAllowed:
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		default:
			w.WriteHeader(rec.status)
			_, _ = w.Write(rec.body)
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status  int
	body    []byte
	sawJSON bool
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	if strings.HasPrefix(r.Header().Get("Content-Type"), "application/json") {
		r.sawJSON = true
		r.ResponseWriter.WriteHeader(status)
	}
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.sawJSON {
		return r.ResponseWriter.Write(p)
	}
	r.body = append(r.body, p...)
	return len(p), nil
}

// ---- request / response shapes ----

type createRequest struct {
	TeamID    string `json:"teamId"`
	Type      string `json:"type"`
	Total     int64  `json:"total"`
	ExpiresAt string `json:"expiresAt"`
}

type allocateRequest struct {
	MemberID string `json:"memberId"`
	Amount   int64  `json:"amount"`
}

type usageRequest struct {
	MemberID string `json:"memberId"`
	Amount   int64  `json:"amount"`
	UsageKey string `json:"usageKey"`
}

type entitlementResponse struct {
	ID        string    `json:"id"`
	TeamID    string    `json:"teamId"`
	Type      string    `json:"type"`
	Total     int64     `json:"total"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expiresAt"`
	Used      int64     `json:"used"`
	Version   int64     `json:"version"`
}

func toResponse(e store.Entitlement) entitlementResponse {
	return entitlementResponse{
		ID: e.ID, TeamID: e.TeamID, Type: e.Type, Total: e.Total,
		Status: e.Status, ExpiresAt: e.ExpiresAt.UTC(), Used: e.Used, Version: e.Version,
	}
}

type detailsResponse struct {
	entitlementResponse
	Members []store.MemberUsage `json:"members"`
}

// ---- handlers ----

func createEntitlement(st Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createRequest
		if !decode(w, r, &req) {
			return
		}
		if req.TeamID == "" || len(req.TeamID) > 256 {
			writeError(w, http.StatusBadRequest, "invalid_team", "teamId is required and must be at most 256 characters")
			return
		}
		if req.Type != store.TypeSeat && req.Type != store.TypeQuota {
			writeError(w, http.StatusBadRequest, "invalid_type", "type must be \"seat\" or \"quota\"")
			return
		}
		if req.Total <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_total", "total must be a positive integer")
			return
		}
		expiresAt, ok := parseExpiresAt(w, req.ExpiresAt)
		if !ok {
			return
		}
		if !expiresAt.After(time.Now().UTC()) {
			writeError(w, http.StatusBadRequest, "invalid_expires_at", "expiresAt must be in the future")
			return
		}

		created, err := st.Create(r.Context(), req.TeamID, req.Type, req.Total, expiresAt)
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		writeJSON(w, http.StatusCreated, toResponse(created))
	}
}

func allocate(st Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		var req allocateRequest
		if !decode(w, r, &req) {
			return
		}
		if !validMember(w, req.MemberID) || !validAmount(w, req.Amount) {
			return
		}
		updated, err := st.Allocate(r.Context(), id, req.MemberID, req.Amount, time.Now().UTC())
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, toResponse(updated))
	}
}

func consume(st Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		var req usageRequest
		if !decode(w, r, &req) {
			return
		}
		if !validMember(w, req.MemberID) || !validAmount(w, req.Amount) {
			return
		}
		if req.UsageKey == "" || len(req.UsageKey) > 256 {
			writeError(w, http.StatusBadRequest, "invalid_usage_key", "usageKey is required and must be at most 256 characters")
			return
		}
		updated, replayed, err := st.Consume(r.Context(), id, req.MemberID, req.Amount, req.UsageKey, time.Now().UTC())
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		if replayed {
			// The body is the first result verbatim; the header is advisory.
			w.Header().Set("Idempotency-Replayed", "true")
		}
		writeJSON(w, http.StatusOK, toResponse(updated))
	}
}

func cancelEntitlement(st Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		updated, err := st.Cancel(r.Context(), id, time.Now().UTC())
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, toResponse(updated))
	}
}

func getEntitlement(st Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		details, err := st.Get(r.Context(), id, time.Now().UTC())
		if err != nil {
			writeStoreError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, detailsResponse{
			entitlementResponse: toResponse(details.Entitlement),
			Members:             details.Members,
		})
	}
}

// ---- helpers ----

func validMember(w http.ResponseWriter, memberID string) bool {
	if memberID == "" || len(memberID) > 256 {
		writeError(w, http.StatusBadRequest, "invalid_member", "memberId is required and must be at most 256 characters")
		return false
	}
	return true
}

func validAmount(w http.ResponseWriter, amount int64) bool {
	if amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_amount", "amount must be a positive integer")
		return false
	}
	return true
}

func parseExpiresAt(w http.ResponseWriter, raw string) (time.Time, bool) {
	if raw == "" {
		writeError(w, http.StatusBadRequest, "invalid_expires_at", "expiresAt is required")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_expires_at", "expiresAt must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	// Timestamptz stores microsecond precision; truncate so that duplicate
	// detection matches regardless of fractional-second formatting.
	return t.UTC().Truncate(time.Microsecond), true
}

// pathID validates the entitlement id path value. A malformed id addresses no
// resource, so it is reported as 404 like any unknown path.
func pathID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !isUUID(id) {
		writeError(w, http.StatusNotFound, "not_found", "entitlement not found")
		return "", false
	}
	return id, true
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "request body must be a valid JSON object with the required fields")
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_body", "request body must contain a single JSON object")
		return false
	}
	return true
}

func writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	var conflict *store.ConflictError
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "entitlement not found")
	case errors.As(err, &conflict) && errors.Is(conflict, store.ErrAlreadyExists):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":       "entitlement already exists for teamId, type and expiresAt",
			"code":        "already_exists",
			"entitlement": toResponse(*conflict.Existing),
		})
	case errors.As(err, &conflict) && errors.Is(conflict, store.ErrCapacity):
		writeError(w, http.StatusConflict, "capacity_exceeded", "allocation would exceed the entitlement total")
	case errors.As(err, &conflict) && errors.Is(conflict, store.ErrInsufficient):
		writeError(w, http.StatusConflict, "insufficient_allowance", "usage exceeds the member's unused allowance")
	case errors.As(err, &conflict) && errors.Is(conflict, store.ErrUsageKeyConflict):
		writeError(w, http.StatusConflict, "usage_key_conflict", "usageKey was already recorded with different memberId or amount")
	case errors.As(err, &conflict) && errors.Is(conflict, store.ErrNotOperable):
		switch conflict.Status {
		case store.StatusCancelled:
			writeError(w, http.StatusConflict, "entitlement_cancelled", "entitlement is cancelled and can no longer be allocated or used")
		case store.StatusExpired:
			writeError(w, http.StatusConflict, "entitlement_expired", "entitlement has expired and can no longer be allocated or used")
		default:
			writeError(w, http.StatusConflict, "entitlement_inactive", "entitlement is not operable")
		}
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "the request could not be completed")
	}
}

type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: message, Code: code})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"the request could not be completed","code":"internal_error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
