package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

// Store is the persistence the HTTP layer needs.
type Store interface {
	Ping(ctx context.Context) error
	CreateEntitlement(ctx context.Context, tenant, key string, quantity int64, expiresAt time.Time) (store.Entitlement, bool, error)
	GetEntitlement(ctx context.Context, tenant, key string) (store.Entitlement, error)
	ListEntitlements(ctx context.Context, tenant string) ([]store.Entitlement, error)
}

// New builds the HTTP handler for the service.
func New(s Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.Ping(ctx); err != nil {
			respond(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
			return
		}
		respond(w, http.StatusOK, `{"status":"ready"}`)
	})
	mux.HandleFunc("POST /v1/tenants/{tenant}/entitlements", createEntitlement(s))
	mux.HandleFunc("GET /v1/tenants/{tenant}/entitlements", listEntitlements(s))
	mux.HandleFunc("GET /v1/tenants/{tenant}/entitlements/{key}", getEntitlement(s))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reject unclean paths (e.g. an empty {tenant} segment) with 404
		// instead of letting ServeMux issue a redirect.
		if path.Clean(r.URL.Path) != r.URL.Path {
			http.NotFound(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}

func respondJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respond(w, status, string(body))
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
