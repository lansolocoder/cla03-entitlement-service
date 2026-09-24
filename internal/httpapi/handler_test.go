package httpapi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/grants"
)

func TestHealthAndReadiness(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		dbError                  error
		status                   int
		calls                    int
	}{
		{"liveness", "GET", "/healthz", "ok", errors.New("database offline"), 200, 0},
		{"ready", "GET", "/readyz", "ready", nil, 200, 1},
		{"unavailable", "GET", "/readyz", "unavailable", errors.New("private connection details"), 503, 1},
		{"unknown path", "GET", "/entitlements", "404", nil, 404, 0},
		{"wrong method", "POST", "/readyz", "Method Not Allowed", nil, 405, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			handler := New(newFakeStore(), func(ctx context.Context) error {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Error("readiness database check must have a deadline")
				}
				return tc.dbError
			})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.status || !strings.Contains(response.Body.String(), tc.body) || calls != tc.calls {
				t.Fatalf("status=%d body=%q database calls=%d", response.Code, response.Body.String(), calls)
			}
			if strings.Contains(response.Body.String(), "private connection details") {
				t.Fatal("database error leaked into public response")
			}
		})
	}
}

// fakeStore is an in-memory grants.Store for handler tests.
type fakeStore struct {
	mu          sync.Mutex
	grants      map[string]grants.Grant // key: tenant + "/" + grantID
	failCreate  bool
	failList    bool
	failCancel  bool
	cancelCalls int
}

func newFakeStore() *fakeStore {
	return &fakeStore{grants: map[string]grants.Grant{}}
}

func key(tenant, grantID string) string { return tenant + "/" + grantID }

func (f *fakeStore) Create(_ context.Context, g grants.Grant) (grants.Grant, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreate {
		return grants.Grant{}, false, errors.New("storage unavailable")
	}
	k := key(g.Tenant, g.GrantID)
	if existing, ok := f.grants[k]; ok {
		if existing.Feature == g.Feature && existing.Amount == g.Amount &&
			existing.EffectiveAt.Equal(g.EffectiveAt) && existing.ExpiresAt.Equal(g.ExpiresAt) {
			return existing, true, nil
		}
		return grants.Grant{}, false, grants.ErrConflict
	}
	g.State = grants.StateActive
	f.grants[k] = g
	return g, false, nil
}

func (f *fakeStore) ListActiveAt(_ context.Context, tenant string, at time.Time) ([]grants.Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failList {
		return nil, errors.New("storage unavailable")
	}
	var out []grants.Grant
	for _, g := range f.grants {
		if g.Tenant != tenant || g.State != grants.StateActive {
			continue
		}
		if (g.EffectiveAt.Equal(at) || g.EffectiveAt.Before(at)) && at.Before(g.ExpiresAt) {
			out = append(out, g)
		}
	}
	return out, nil
}

func (f *fakeStore) Cancel(_ context.Context, tenant, grantID string, cancelledAt time.Time) (grants.Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls++
	if f.failCancel {
		return grants.Grant{}, errors.New("storage unavailable")
	}
	k := key(tenant, grantID)
	g, ok := f.grants[k]
	if !ok {
		return grants.Grant{}, grants.ErrNotFound
	}
	if g.State != grants.StateCancelled {
		g.State = grants.StateCancelled
		c := cancelledAt
		g.CancelledAt = &c
		f.grants[k] = g
	}
	return g, nil
}
