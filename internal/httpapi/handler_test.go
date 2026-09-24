package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

// fakeStore is an in-memory entitlement.Store for handler tests.
type fakeStore struct {
	mu      sync.Mutex
	grants  map[string]map[string]entitlement.Grant // tenant -> grantId -> grant
	fail    error
	putN    int
	cancelN int
}

func newFakeStore() *fakeStore {
	return &fakeStore{grants: map[string]map[string]entitlement.Grant{}}
}

func (f *fakeStore) Put(_ context.Context, g entitlement.Grant) (entitlement.Grant, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putN++
	if f.fail != nil {
		return entitlement.Grant{}, false, f.fail
	}
	byTenant := f.grants[g.Tenant]
	if byTenant == nil {
		byTenant = map[string]entitlement.Grant{}
		f.grants[g.Tenant] = byTenant
	}
	if existing, ok := byTenant[g.GrantID]; ok {
		same := existing.Feature == g.Feature &&
			existing.Amount == g.Amount &&
			existing.EffectiveAt.Equal(g.EffectiveAt) &&
			existing.ExpiresAt.Equal(g.ExpiresAt)
		if !same {
			return existing, false, entitlement.ErrConflict
		}
		return existing, false, nil
	}
	byTenant[g.GrantID] = g
	return g, true, nil
}

func (f *fakeStore) ActiveAt(_ context.Context, tenant string, at time.Time) ([]entitlement.Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return nil, f.fail
	}
	var out []entitlement.Grant
	for _, g := range f.grants[tenant] {
		if g.State == entitlement.StateActive &&
			!at.Before(g.EffectiveAt) && at.Before(g.ExpiresAt) {
			out = append(out, g)
		}
	}
	return out, nil
}

func (f *fakeStore) Cancel(_ context.Context, tenant, grantID string, cancelledAt time.Time) (entitlement.Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return entitlement.Grant{}, f.fail
	}
	byTenant := f.grants[tenant]
	g, ok := byTenant[grantID]
	if !ok {
		return entitlement.Grant{}, entitlement.ErrNotFound
	}
	f.cancelN++
	g.State = entitlement.StateCancelled
	byTenant[grantID] = g
	return g, nil
}

func newHandler(store entitlement.Store) http.Handler {
	return New(func(context.Context) error { return nil }, store)
}

func do(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, reader))
	return rec
}

const validCreateBody = `{"grantId":"g1","feature":"seats","amount":5,` +
	`"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-12-31T23:59:59Z"}`

func TestCreateGrant(t *testing.T) {
	t.Run("created", func(t *testing.T) {
		store := newFakeStore()
		rec := do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", validCreateBody)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		want := map[string]any{
			"tenant": "acme", "grantId": "g1", "feature": "seats",
			"amount":      float64(5),
			"effectiveAt": "2026-01-01T00:00:00Z",
			"expiresAt":   "2026-12-31T23:59:59Z",
			"state":       "active",
		}
		for k, v := range want {
			if resp[k] != v {
				t.Errorf("field %s = %v, want %v", k, resp[k], v)
			}
		}
	})

	t.Run("normalizes timestamps to UTC seconds with Z", func(t *testing.T) {
		store := newFakeStore()
		body := `{"grantId":"g2","feature":"quota","amount":1,` +
			`"effectiveAt":"2026-01-01T10:30:45.9+02:00","expiresAt":"2026-02-01T00:00:00Z"}`
		rec := do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", body)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
		}
		var resp struct {
			EffectiveAt string `json:"effectiveAt"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.EffectiveAt != "2026-01-01T08:30:45Z" {
			t.Fatalf("effectiveAt=%q, want 2026-01-01T08:30:45Z", resp.EffectiveAt)
		}
	})

	t.Run("idempotent retry returns 200 and original", func(t *testing.T) {
		store := newFakeStore()
		first := do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", validCreateBody)
		if first.Code != http.StatusCreated {
			t.Fatalf("first status=%d", first.Code)
		}
		// Same four fields, timestamps expressed in another offset.
		retryBody := `{"grantId":"g1","feature":"seats","amount":5,` +
			`"effectiveAt":"2026-01-01T02:00:00+02:00","expiresAt":"2027-01-01T01:59:59+02:00"}`
		rec := do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", retryBody)
		if rec.Code != http.StatusOK {
			t.Fatalf("retry status=%d body=%s", rec.Code, rec.Body)
		}
		var resp struct {
			Amount      float64 `json:"amount"`
			EffectiveAt string  `json:"effectiveAt"`
			ExpiresAt   string  `json:"expiresAt"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Amount != 5 || resp.EffectiveAt != "2026-01-01T00:00:00Z" ||
			resp.ExpiresAt != "2026-12-31T23:59:59Z" {
			t.Fatalf("retry did not return the original record: %+v", resp)
		}
		if len(store.grants["acme"]) != 1 {
			t.Fatalf("idempotent retry created extra data: %d grants", len(store.grants["acme"]))
		}
	})

	t.Run("same grantId different content returns 409", func(t *testing.T) {
		store := newFakeStore()
		_ = do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", validCreateBody)
		body := `{"grantId":"g1","feature":"seats","amount":99,` +
			`"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-12-31T23:59:59Z"}`
		rec := do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", body)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
		}
		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &errResp)
		if errResp.Error == "" {
			t.Fatal("409 must carry an error message")
		}
		g := store.grants["acme"]["g1"]
		if g.Amount != 5 {
			t.Fatalf("409 mutated stored grant: amount=%d", g.Amount)
		}
	})

	t.Run("grantId unique per tenant", func(t *testing.T) {
		store := newFakeStore()
		_ = do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", validCreateBody)
		rec := do(t, newHandler(store), http.MethodPost, "/tenants/globex/entitlements", validCreateBody)
		if rec.Code != http.StatusCreated {
			t.Fatalf("same grantId under another tenant status=%d body=%s", rec.Code, rec.Body)
		}
	})
}

func TestCreateGrantValidation(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"not json", `{not json`},
		{"missing grantId", `{"feature":"seats","amount":1,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"blank grantId", `{"grantId":"  ","feature":"seats","amount":1,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"bad feature", `{"grantId":"g","feature":"gold","amount":1,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"missing amount", `{"grantId":"g","feature":"seats","effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"zero amount", `{"grantId":"g","feature":"seats","amount":0,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"negative amount", `{"grantId":"g","feature":"seats","amount":-3,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"fractional amount", `{"grantId":"g","feature":"seats","amount":1.5,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"string amount", `{"grantId":"g","feature":"seats","amount":"1","effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"missing effectiveAt", `{"grantId":"g","feature":"seats","amount":1,"expiresAt":"2026-02-01T00:00:00Z"}`},
		{"missing expiresAt", `{"grantId":"g","feature":"seats","amount":1,"effectiveAt":"2026-01-01T00:00:00Z"}`},
		{"bad time format", `{"grantId":"g","feature":"seats","amount":1,"effectiveAt":"2026/01/01","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"expires before effective", `{"grantId":"g","feature":"seats","amount":1,"effectiveAt":"2026-02-01T00:00:00Z","expiresAt":"2026-01-01T00:00:00Z"}`},
		{"expires equals effective", `{"grantId":"g","feature":"seats","amount":1,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:00:00Z"}`},
		{"unknown field", `{"grantId":"g","feature":"seats","amount":1,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z","extra":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			rec := do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
			}
			var errResp struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil || errResp.Error == "" {
				t.Fatalf("want {\"error\":...}, got %s", rec.Body)
			}
			if len(store.grants) != 0 {
				t.Fatal("validation failure must not persist data")
			}
		})
	}
}

func TestListGrants(t *testing.T) {
	store := newFakeStore()
	must := func(body, tenant string) {
		t.Helper()
		rec := do(t, newHandler(store), http.MethodPost, "/tenants/"+tenant+"/entitlements", body)
		if rec.Code != http.StatusCreated {
			t.Fatalf("seed status=%d body=%s", rec.Code, rec.Body)
		}
	}
	must(`{"grantId":"early","feature":"seats","amount":2,`+
		`"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`, "acme")
	must(`{"grantId":"mid","feature":"quota","amount":10,`+
		`"effectiveAt":"2026-02-01T00:00:00Z","expiresAt":"2026-03-01T00:00:00Z"}`, "acme")
	must(`{"grantId":"other","feature":"seats","amount":7,`+
		`"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-12-31T00:00:00Z"}`, "globex")

	t.Run("asOf normalized and only active grants returned", func(t *testing.T) {
		// Boundary: [effective, expire) — at exactly expiresAt the grant is gone.
		rec := do(t, newHandler(store), http.MethodGet,
			"/tenants/acme/entitlements?at=2026-02-01T00:00:00Z", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
		}
		var resp struct {
			Tenant string `json:"tenant"`
			AsOf   string `json:"asOf"`
			Grants []struct {
				GrantID string  `json:"grantId"`
				State   string  `json:"state"`
				Amount  float64 `json:"amount"`
			} `json:"grants"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Tenant != "acme" || resp.AsOf != "2026-02-01T00:00:00Z" {
			t.Fatalf("unexpected envelope: %+v", resp)
		}
		if len(resp.Grants) != 1 || resp.Grants[0].GrantID != "mid" ||
			resp.Grants[0].State != "active" || resp.Grants[0].Amount != 10 {
			t.Fatalf("expected only the mid grant, got %+v", resp.Grants)
		}
	})

	t.Run("left boundary is inclusive", func(t *testing.T) {
		rec := do(t, newHandler(store), http.MethodGet,
			"/tenants/acme/entitlements?at=2026-01-01T00:00:00Z", "")
		if !strings.Contains(rec.Body.String(), `"grantId":"early"`) {
			t.Fatalf("early grant should be active at effectiveAt: %s", rec.Body)
		}
	})

	t.Run("empty interval yields empty grants array", func(t *testing.T) {
		rec := do(t, newHandler(store), http.MethodGet,
			"/tenants/acme/entitlements?at=2030-01-01T00:00:00Z", "")
		if !strings.Contains(rec.Body.String(), `"grants":[]`) {
			t.Fatalf("want empty grants array, got %s", rec.Body)
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		rec := do(t, newHandler(store), http.MethodGet,
			"/tenants/acme/entitlements?at=2026-01-15T00:00:00Z", "")
		if strings.Contains(rec.Body.String(), "other") {
			t.Fatalf("other tenant's grant leaked: %s", rec.Body)
		}
	})

	t.Run("missing or bad at is 400", func(t *testing.T) {
		if rec := do(t, newHandler(store), http.MethodGet, "/tenants/acme/entitlements", ""); rec.Code != http.StatusBadRequest {
			t.Fatalf("missing at status=%d", rec.Code)
		}
		if rec := do(t, newHandler(store), http.MethodGet, "/tenants/acme/entitlements?at=soon", ""); rec.Code != http.StatusBadRequest {
			t.Fatalf("bad at status=%d", rec.Code)
		}
	})
}

func TestCancelGrant(t *testing.T) {
	store := newFakeStore()
	_ = do(t, newHandler(store), http.MethodPost, "/tenants/acme/entitlements", validCreateBody)

	t.Run("cancel returns updated grant", func(t *testing.T) {
		rec := do(t, newHandler(store), http.MethodPost,
			"/tenants/acme/entitlements/g1/cancel", `{"cancelledAt":"2026-06-01T12:00:00+02:00"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
		}
		var resp struct {
			State string `json:"state"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.State != "cancelled" {
			t.Fatalf("state=%q, want cancelled", resp.State)
		}
	})

	t.Run("cancelled grant never listed again", func(t *testing.T) {
		for _, at := range []string{
			"2025-01-01T00:00:00Z",
			"2026-06-01T00:00:00Z",
			"2026-06-15T00:00:00Z",
		} {
			rec := do(t, newHandler(store), http.MethodGet, "/tenants/acme/entitlements?at="+at, "")
			if strings.Contains(rec.Body.String(), "g1") {
				t.Fatalf("cancelled grant listed at %s: %s", at, rec.Body)
			}
		}
	})

	t.Run("unknown grant is 404 and does not change state", func(t *testing.T) {
		before := store.cancelN
		rec := do(t, newHandler(store), http.MethodPost,
			"/tenants/acme/entitlements/nope/cancel", `{"cancelledAt":"2026-06-01T00:00:00Z"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
		}
		if store.cancelN != before {
			t.Fatal("404 path must not mutate state")
		}
	})

	t.Run("invalid body is 400", func(t *testing.T) {
		if rec := do(t, newHandler(store), http.MethodPost,
			"/tenants/acme/entitlements/g1/cancel", `{"cancelledAt":"soon"}`); rec.Code != http.StatusBadRequest {
			t.Fatalf("bad cancelledAt status=%d", rec.Code)
		}
		if rec := do(t, newHandler(store), http.MethodPost,
			"/tenants/acme/entitlements/g1/cancel", `{}`); rec.Code != http.StatusBadRequest {
			t.Fatalf("missing cancelledAt status=%d", rec.Code)
		}
	})
}

func TestRouting(t *testing.T) {
	h := newHandler(newFakeStore())
	for _, tc := range []struct {
		method, target string
		status         int
	}{
		{http.MethodGet, "/tenants/acme/entitlements?at=2026-01-01T00:00:00Z", http.StatusOK},
		{http.MethodPut, "/tenants/acme/entitlements", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/tenants/acme/entitlements", http.StatusMethodNotAllowed},
		{http.MethodGet, "/tenants/acme/entitlements/g1/cancel", http.StatusMethodNotAllowed},
		{http.MethodGet, "/tenants", http.StatusNotFound},
		{http.MethodGet, "/unknown", http.StatusNotFound},
	} {
		rec := do(t, h, tc.method, tc.target, "")
		if rec.Code != tc.status {
			t.Errorf("%s %s = %d, want %d (body=%s)", tc.method, tc.target, rec.Code, tc.status, rec.Body)
		}
	}
}

func TestStoreFailuresAre500(t *testing.T) {
	store := newFakeStore()
	store.fail = errors.New("database unavailable")
	h := newHandler(store)

	if rec := do(t, h, http.MethodPost, "/tenants/acme/entitlements", validCreateBody); rec.Code != http.StatusInternalServerError {
		t.Fatalf("create status=%d", rec.Code)
	} else if strings.Contains(rec.Body.String(), "database unavailable") {
		t.Fatal("store error leaked into response")
	}
	if rec := do(t, h, http.MethodGet, "/tenants/acme/entitlements?at=2026-01-01T00:00:00Z", ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("list status=%d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/tenants/acme/entitlements/g1/cancel",
		`{"cancelledAt":"2026-06-01T00:00:00Z"}`); rec.Code != http.StatusInternalServerError {
		t.Fatalf("cancel status=%d", rec.Code)
	}
}

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
			handler := New(func(ctx context.Context) error {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Error("readiness database check must have a deadline")
				}
				return tc.dbError
			}, newFakeStore())
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
