package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/grants"
)

const maxBodyBytes = 1 << 20

// New builds the HTTP handler. checkDatabase backs the readiness probe and
// store persists entitlement grants.
func New(store grants.Store, checkDatabase func(context.Context) error) http.Handler {
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

	api := &entitlementAPI{store: store}
	mux.HandleFunc("POST /tenants/{tenant}/entitlements", api.createGrant)
	mux.HandleFunc("GET /tenants/{tenant}/entitlements", api.listGrants)
	mux.HandleFunc("POST /tenants/{tenant}/entitlements/{grantId}/cancel", api.cancelGrant)
	return mux
}

type entitlementAPI struct {
	store grants.Store
}

// grantView is the public JSON representation of a grant. Times are UTC,
// second precision, RFC 3339 with a trailing Z.
type grantView struct {
	Tenant      string `json:"tenant"`
	GrantID     string `json:"grantId"`
	Feature     string `json:"feature"`
	Amount      int64  `json:"amount"`
	EffectiveAt string `json:"effectiveAt"`
	ExpiresAt   string `json:"expiresAt"`
	State       string `json:"state"`
}

func viewGrant(g grants.Grant) grantView {
	return grantView{
		Tenant:      g.Tenant,
		GrantID:     g.GrantID,
		Feature:     string(g.Feature),
		Amount:      g.Amount,
		EffectiveAt: formatTimestamp(g.EffectiveAt),
		ExpiresAt:   formatTimestamp(g.ExpiresAt),
		State:       string(g.State),
	}
}

type grantRequest struct {
	GrantID     string          `json:"grantId"`
	Feature     string          `json:"feature"`
	Amount      json.RawMessage `json:"amount"`
	EffectiveAt string          `json:"effectiveAt"`
	ExpiresAt   string          `json:"expiresAt"`
}

func (api *entitlementAPI) createGrant(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")

	var req grantRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	g, ok := validateGrantRequest(w, tenant, req)
	if !ok {
		return
	}

	stored, reused, err := api.store.Create(r.Context(), g)
	if err != nil {
		if errors.Is(err, grants.ErrConflict) {
			writeError(w, http.StatusConflict, "grantId already exists with different parameters")
			return
		}
		writeInternal(w)
		return
	}

	status := http.StatusCreated
	if reused {
		status = http.StatusOK
	}
	writeJSON(w, status, viewGrant(stored))
}

func (api *entitlementAPI) listGrants(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	raw := r.URL.Query().Get("at")
	at, err := parseTimestamp(raw, "at")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	found, err := api.store.ListActiveAt(r.Context(), tenant, at)
	if err != nil {
		writeInternal(w)
		return
	}

	views := make([]grantView, 0, len(found))
	for _, g := range found {
		views = append(views, viewGrant(g))
	}
	writeJSON(w, http.StatusOK, struct {
		Tenant string      `json:"tenant"`
		AsOf   string      `json:"asOf"`
		Grants []grantView `json:"grants"`
	}{tenant, formatTimestamp(at), views})
}

type cancelRequest struct {
	CancelledAt string `json:"cancelledAt"`
}

func (api *entitlementAPI) cancelGrant(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	grantID := r.PathValue("grantId")

	var req cancelRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cancelledAt, err := parseTimestamp(req.CancelledAt, "cancelledAt")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	stored, err := api.store.Cancel(r.Context(), tenant, grantID, cancelledAt)
	if err != nil {
		if errors.Is(err, grants.ErrNotFound) {
			writeError(w, http.StatusNotFound, "grant not found")
			return
		}
		writeInternal(w)
		return
	}
	writeJSON(w, http.StatusOK, viewGrant(stored))
}

func validateGrantRequest(w http.ResponseWriter, tenant string, req grantRequest) (grants.Grant, bool) {
	if req.GrantID == "" {
		writeError(w, http.StatusBadRequest, "grantId is required")
		return grants.Grant{}, false
	}
	feature := grants.Feature(req.Feature)
	if !feature.Valid() {
		writeError(w, http.StatusBadRequest, "feature must be seats or quota")
		return grants.Grant{}, false
	}
	amount, err := parseAmount(req.Amount)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return grants.Grant{}, false
	}
	effectiveAt, err := parseTimestamp(req.EffectiveAt, "effectiveAt")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return grants.Grant{}, false
	}
	expiresAt, err := parseTimestamp(req.ExpiresAt, "expiresAt")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return grants.Grant{}, false
	}
	if !expiresAt.After(effectiveAt) {
		writeError(w, http.StatusBadRequest, "expiresAt must be later than effectiveAt")
		return grants.Grant{}, false
	}
	return grants.Grant{
		Tenant:      tenant,
		GrantID:     req.GrantID,
		Feature:     feature,
		Amount:      amount,
		EffectiveAt: effectiveAt,
		ExpiresAt:   expiresAt,
		State:       grants.StateActive,
	}, true
}

// parseAmount accepts only positive JSON integers. Strings, fractions,
// exponents and booleans are rejected as non-integers.
func parseAmount(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, errors.New("amount is required")
	}
	token := strings.TrimSpace(string(raw))
	if token == "" || token[0] == '"' {
		return 0, errors.New("amount must be a positive integer")
	}
	v, err := strconv.ParseInt(token, 10, 64)
	if err != nil || v <= 0 {
		return 0, errors.New("amount must be a positive integer")
	}
	return v, nil
}

// parseTimestamp parses RFC 3339 and normalizes to whole-second UTC. The
// validity interval is stored at second precision per the API contract.
func parseTimestamp(value, field string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New(field + " is required")
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, errors.New(field + " must be an RFC 3339 timestamp")
	}
	return t.UTC().Truncate(time.Second), nil
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(time.RFC3339)
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("request body is required")
		}
		return errors.New("invalid JSON request body")
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON request body")
	}
	return nil
}

func writeInternal(w http.ResponseWriter) {
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"internal error"}`)
	}
	respond(w, status, string(body))
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
