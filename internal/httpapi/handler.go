package httpapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

func New(checkDatabase func(context.Context) error, service entitlement.Service) http.Handler {
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
	registerEntitlementRoutes(mux, service)

	// Known paths with the wrong method answer 405 (with an Allow header);
	// every truly unknown path answers 404. Both bodies are JSON.
	methodNotAllowed := func(allowed ...string) http.HandlerFunc {
		allow := strings.Join(allowed, ", ")
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Allow", allow)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": errorPayload{Code: "method_not_allowed", Message: "method not allowed for this path"},
			})
		}
	}
	mux.HandleFunc("/healthz", methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/readyz", methodNotAllowed(http.MethodGet))
	// GET/PUT/... on the collection stay 404: the collection itself is not a
	// readable path.
	mux.HandleFunc("/entitlements", func(w http.ResponseWriter, _ *http.Request) { writeNotFound(w) })
	mux.HandleFunc("/entitlements/{id}", methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/entitlements/{id}/allocations", methodNotAllowed(http.MethodPost))
	mux.HandleFunc("/entitlements/{id}/usages", methodNotAllowed(http.MethodPost))
	mux.HandleFunc("/entitlements/{id}/cancel", methodNotAllowed(http.MethodPost))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeNotFound(w)
	})
	return mux
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}

func writeNotFound(w http.ResponseWriter) {
	writeJSON(w, http.StatusNotFound, map[string]any{
		"error": errorPayload{Code: "not_found", Message: "unknown path"},
	})
}
