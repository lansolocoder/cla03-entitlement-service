// Package httpapi exposes the entitlement service over HTTP. Every response
// is a single JSON object terminated by a newline.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

const maxCreateBodyBytes = 1 << 20

// New builds the HTTP handler. checkDatabase backs the readiness probe and
// store persists entitlements; it must be safe for concurrent use.
func New(checkDatabase func(context.Context) error, store entitlement.Store) http.Handler {
	h := &handler{store: store}
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

	// Manual dispatch for the entitlement routes: it keeps tenant segment
	// validation (missing or empty tenant -> 404) deterministic and avoids
	// the net/http mux rewriting paths with empty segments via redirects.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/readyz":
			if r.Method != http.MethodGet {
				allow(w, http.MethodGet)
				errorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			mux.ServeHTTP(w, r)
			return
		}
		if rest, ok := strings.CutPrefix(r.URL.Path, "/v1/tenants/"); ok {
			h.serveTenantRoute(w, r, rest)
			return
		}
		errorJSON(w, http.StatusNotFound, "not found")
	})
}

type handler struct {
	store entitlement.Store
}

// serveTenantRoute routes paths shaped:
//
//	/v1/tenants/{tenant}/entitlements
//	/v1/tenants/{tenant}/entitlements/{key}
func (h *handler) serveTenantRoute(w http.ResponseWriter, r *http.Request, rest string) {
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) == 2 && parts[1] == "entitlements":
		tenant := parts[0]
		if tenant == "" {
			errorJSON(w, http.StatusNotFound, "not found")
			return
		}
		switch r.Method {
		case http.MethodPost:
			h.create(w, r, tenant)
		case http.MethodGet:
			h.list(w, r, tenant)
		default:
			allow(w, http.MethodGet, http.MethodPost)
			errorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case len(parts) == 3 && parts[1] == "entitlements":
		tenant, key := parts[0], parts[2]
		if tenant == "" || key == "" {
			errorJSON(w, http.StatusNotFound, "not found")
			return
		}
		if r.Method != http.MethodGet {
			allow(w, http.MethodGet)
			errorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		h.getOne(w, r, tenant, key)
	default:
		errorJSON(w, http.StatusNotFound, "not found")
	}
}

// createRequest uses pointers so missing fields are distinguishable from
// zero values and typed decoding rejects fractions and exponent notation.
type createRequest struct {
	Key       *string `json:"key"`
	Quantity  *int64  `json:"quantity"`
	ExpiresAt *string `json:"expiresAt"`
}

func (h *handler) create(w http.ResponseWriter, r *http.Request, tenant string) {
	var req createRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCreateBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		errorJSON(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	if err := ensureSingleJSONValue(decoder); err != nil {
		errorJSON(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	if req.Key == nil || req.Quantity == nil || req.ExpiresAt == nil {
		errorJSON(w, http.StatusBadRequest, "key, quantity and expiresAt are required")
		return
	}
	expiresAt, err := parseUTCRFC3339(*req.ExpiresAt)
	if err != nil {
		errorJSON(w, http.StatusBadRequest, "expiresAt must be an RFC3339 UTC timestamp")
		return
	}
	input := entitlement.CreateInput{Key: *req.Key, Quantity: *req.Quantity, ExpiresAt: expiresAt}
	if err := entitlement.Validate(input, time.Now().UTC()); err != nil {
		errorJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	created, isNew, err := h.store.Create(r.Context(), tenant, input)
	if err != nil {
		var conflict *entitlement.ConflictError
		if errors.As(err, &conflict) {
			// The existing record is never modified and never echoed back.
			errorJSON(w, http.StatusConflict, "entitlement with this key already exists with different parameters")
			return
		}
		internalError(w, err)
		return
	}
	status := http.StatusOK
	if isNew {
		status = http.StatusCreated
	}
	writeJSON(w, status, newEntitlementJSON(created))
}

func (h *handler) list(w http.ResponseWriter, r *http.Request, tenant string) {
	if r.URL.RawQuery != "" {
		errorJSON(w, http.StatusBadRequest, "request does not accept query parameters")
		return
	}
	items, err := h.store.List(r.Context(), tenant)
	if err != nil {
		internalError(w, err)
		return
	}
	encoded := make([]entitlementJSON, 0, len(items))
	for _, item := range items {
		encoded = append(encoded, newEntitlementJSON(item))
	}
	writeJSON(w, http.StatusOK, struct {
		Items []entitlementJSON `json:"items"`
	}{Items: encoded})
}

func (h *handler) getOne(w http.ResponseWriter, r *http.Request, tenant, key string) {
	item, err := h.store.Get(r.Context(), tenant, key)
	if errors.Is(err, entitlement.ErrNotFound) {
		errorJSON(w, http.StatusNotFound, "entitlement not found")
		return
	}
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newEntitlementJSON(item))
}

// entitlementJSON is the wire representation. Times render as RFC3339
// (ending in Z because values are normalized to UTC) and the quantity fields
// marshal as JSON integers.
type entitlementJSON struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Key       string    `json:"key"`
	Quantity  int64     `json:"quantity"`
	Consumed  int64     `json:"consumed"`
	Reserved  int64     `json:"reserved"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func newEntitlementJSON(e *entitlement.Entitlement) entitlementJSON {
	return entitlementJSON{
		ID:        e.ID,
		Tenant:    e.Tenant,
		Key:       e.Key,
		Quantity:  e.Quantity,
		Consumed:  e.Consumed,
		Reserved:  e.Reserved,
		Status:    e.Status,
		CreatedAt: e.CreatedAt.UTC(),
		ExpiresAt: e.ExpiresAt.UTC(),
	}
}

// parseUTCRFC3339 accepts RFC3339 timestamps at UTC offset (Z or +00:00) and
// rejects every other zone.
func parseUTCRFC3339(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, err
	}
	if _, offset := t.Zone(); offset != 0 {
		return time.Time{}, errors.New("timestamp must be UTC")
	}
	return t.UTC(), nil
}

func ensureSingleJSONValue(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		// Reachable only on marshalling bugs in our own types; avoid
		// emitting a partial response.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}` + "\n"))
		return
	}
	respond(w, status, string(body))
}

func errorJSON(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

func internalError(w http.ResponseWriter, err error) {
	// Details are intentionally kept out of the public response.
	_ = err
	errorJSON(w, http.StatusInternalServerError, "internal server error")
}

func allow(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
}
