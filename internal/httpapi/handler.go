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

// maxBodyBytes caps request body size.
const maxBodyBytes = 1 << 20

// New builds the HTTP handler. checkDatabase backs GET /readyz and grants
// backs the entitlement endpoints.
func New(checkDatabase func(context.Context) error, grants entitlement.Store) http.Handler {
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

	mux.HandleFunc("POST /tenants/{tenant}/entitlements", func(w http.ResponseWriter, r *http.Request) {
		createGrant(w, r, grants, r.PathValue("tenant"))
	})
	mux.HandleFunc("GET /tenants/{tenant}/entitlements", func(w http.ResponseWriter, r *http.Request) {
		listGrants(w, r, grants, r.PathValue("tenant"))
	})
	mux.HandleFunc("POST /tenants/{tenant}/entitlements/{grantId}/cancel", func(w http.ResponseWriter, r *http.Request) {
		cancelGrant(w, r, grants, r.PathValue("tenant"), r.PathValue("grantId"))
	})
	return mux
}

func createGrant(w http.ResponseWriter, r *http.Request, grants entitlement.Store, tenant string) {
	body, err := readBody(w, r)
	if err != nil {
		return
	}
	input, err := entitlement.ParseCreateRequest(tenant, body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	grant, created, err := grants.Put(ctx, input.Grant())
	if err != nil {
		if errors.Is(err, entitlement.ErrConflict) {
			writeError(w, http.StatusConflict, "grantId already exists with different grant contents")
			return
		}
		writeInternalError(w)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, grantResponse{
		Tenant:      grant.Tenant,
		GrantID:     grant.GrantID,
		Feature:     grant.Feature,
		Amount:      grant.Amount,
		EffectiveAt: entitlement.FormatTime(grant.EffectiveAt),
		ExpiresAt:   entitlement.FormatTime(grant.ExpiresAt),
		State:       string(grant.State),
	})
}

func listGrants(w http.ResponseWriter, r *http.Request, grants entitlement.Store, tenant string) {
	at, err := entitlement.ParseAtParam(r.URL.Query().Get("at"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	found, err := grants.ActiveAt(ctx, tenant, at)
	if err != nil {
		writeInternalError(w)
		return
	}
	items := make([]grantResponse, 0, len(found))
	for _, g := range found {
		items = append(items, grantResponse{
			Tenant:      g.Tenant,
			GrantID:     g.GrantID,
			Feature:     g.Feature,
			Amount:      g.Amount,
			EffectiveAt: entitlement.FormatTime(g.EffectiveAt),
			ExpiresAt:   entitlement.FormatTime(g.ExpiresAt),
			State:       string(g.State),
		})
	}
	writeJSON(w, http.StatusOK, listResponse{
		Tenant: tenant,
		AsOf:   entitlement.FormatTime(at),
		Grants: items,
	})
}

func cancelGrant(w http.ResponseWriter, r *http.Request, grants entitlement.Store, tenant, grantID string) {
	if strings.TrimSpace(grantID) == "" {
		writeError(w, http.StatusNotFound, "grant not found")
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		return
	}
	cancelledAt, err := entitlement.ParseCancelRequest(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	grant, err := grants.Cancel(ctx, tenant, grantID, cancelledAt)
	if err != nil {
		if errors.Is(err, entitlement.ErrNotFound) {
			writeError(w, http.StatusNotFound, "grant not found")
			return
		}
		writeInternalError(w)
		return
	}
	// Echo the grant with state=cancelled so the cancellation is directly
	// observable; it no longer appears in listing responses.
	writeJSON(w, http.StatusOK, grantResponse{
		Tenant:      grant.Tenant,
		GrantID:     grant.GrantID,
		Feature:     grant.Feature,
		Amount:      grant.Amount,
		EffectiveAt: entitlement.FormatTime(grant.EffectiveAt),
		ExpiresAt:   entitlement.FormatTime(grant.ExpiresAt),
		State:       string(grant.State),
	})
}

// grantResponse is the JSON representation of a grant with the exact field
// names pinned by the API contract.
type grantResponse struct {
	Tenant      string `json:"tenant"`
	GrantID     string `json:"grantId"`
	Feature     string `json:"feature"`
	Amount      int64  `json:"amount"`
	EffectiveAt string `json:"effectiveAt"`
	ExpiresAt   string `json:"expiresAt"`
	State       string `json:"state"`
}

type listResponse struct {
	Tenant string          `json:"tenant"`
	AsOf   string          `json:"asOf"`
	Grants []grantResponse `json:"grants"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		writeInternalError(w)
		return nil, err
	}
	if len(body) > maxBodyBytes {
		writeError(w, http.StatusBadRequest, "request body too large")
		return nil, errors.New("request body too large")
	}
	return body, nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeInternalError(w http.ResponseWriter) {
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"internal server error"}`)
	}
	respond(w, status, string(body))
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
