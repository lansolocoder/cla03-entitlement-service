package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

// fakeStore is an in-memory Store with the same idempotency semantics as the
// PostgreSQL implementation.
type fakeStore struct {
	pingErr error

	mu     sync.Mutex
	nextID int
	ents   map[string]store.Entitlement
}

func newFakeStore() *fakeStore {
	return &fakeStore{ents: map[string]store.Entitlement{}}
}

func (f *fakeStore) Ping(context.Context) error {
	return f.pingErr
}

func (f *fakeStore) CreateEntitlement(_ context.Context, tenant, key string, quantity int64, expiresAt time.Time) (store.Entitlement, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := tenant + "\x00" + key
	if existing, ok := f.ents[index]; ok {
		if existing.Quantity == quantity && existing.ExpiresAt.Equal(expiresAt.UTC().Truncate(time.Microsecond)) {
			return existing, false, nil
		}
		return store.Entitlement{}, false, store.ErrConflict
	}
	f.nextID++
	ent := store.Entitlement{
		ID:        fmt.Sprintf("00000000-0000-4000-8000-%012d", f.nextID),
		Tenant:    tenant,
		Key:       key,
		Quantity:  quantity,
		Status:    "active",
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		ExpiresAt: expiresAt.UTC().Truncate(time.Microsecond),
	}
	f.ents[index] = ent
	return ent, true, nil
}

func (f *fakeStore) GetEntitlement(_ context.Context, tenant, key string) (store.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ent, ok := f.ents[tenant+"\x00"+key]
	if !ok {
		return store.Entitlement{}, store.ErrNotFound
	}
	return ent, nil
}

func (f *fakeStore) ListEntitlements(_ context.Context, tenant string) ([]store.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ents := []store.Entitlement{}
	for index, ent := range f.ents {
		if strings.SplitN(index, "\x00", 2)[0] == tenant {
			ents = append(ents, ent)
		}
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].Key < ents[j].Key })
	return ents, nil
}

func (f *fakeStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ents)
}

func TestHealthAndReadiness(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		dbError                  error
		status                   int
	}{
		{"liveness", "GET", "/healthz", "ok", errors.New("database offline"), 200},
		{"ready", "GET", "/readyz", "ready", nil, 200},
		{"unavailable", "GET", "/readyz", "unavailable", errors.New("private connection details"), 503},
		{"unknown path", "GET", "/entitlements", "404", nil, 404},
		{"wrong method", "POST", "/readyz", "Method Not Allowed", nil, 405},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := New(&fakeStore{pingErr: tc.dbError, ents: map[string]store.Entitlement{}})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.status || !strings.Contains(response.Body.String(), tc.body) {
				t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "private connection details") {
				t.Fatal("database error leaked into public response")
			}
		})
	}
}

const validBody = `{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`

func TestCreateEntitlement(t *testing.T) {
	backend := newFakeStore()
	handler := New(backend)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("POST", "/v1/tenants/acme/entitlements", strings.NewReader(validBody)))
	if response.Code != 201 {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
	if !strings.HasSuffix(response.Body.String(), "}\n") {
		t.Fatalf("body must be a single JSON object ending with newline: %q", response.Body.String())
	}
	if ct := response.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q", ct)
	}
	var got map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["tenant"] != "acme" || got["key"] != "seats" || got["status"] != "active" {
		t.Fatalf("unexpected body %v", got)
	}
	for _, field := range []string{"quantity", "consumed", "reserved"} {
		if _, ok := got[field].(float64); !ok {
			t.Fatalf("%s must be a JSON integer, got %v", field, got[field])
		}
	}
	if got["quantity"] != float64(10) || got["consumed"] != float64(0) || got["reserved"] != float64(0) {
		t.Fatalf("unexpected quantities %v", got)
	}
	if got["expiresAt"] != "2030-01-01T00:00:00Z" {
		t.Fatalf("expiresAt %v", got["expiresAt"])
	}
	createdAt, err := time.Parse(time.RFC3339, got["createdAt"].(string))
	if err != nil || time.Since(createdAt) > time.Minute {
		t.Fatalf("createdAt %v err %v", got["createdAt"], err)
	}
	id, ok := got["id"].(string)
	if !ok || len(id) != 36 {
		t.Fatalf("id %v", got["id"])
	}
}

func TestCreateEntitlementValidation(t *testing.T) {
	future := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	for _, tc := range []struct {
		name, method, target, body string
		status                     int
	}{
		{"key uppercase", "POST", "/v1/tenants/acme/entitlements", `{"key":"Seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"key too short pattern", "POST", "/v1/tenants/acme/entitlements", `{"key":"a","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"key too long", "POST", "/v1/tenants/acme/entitlements", `{"key":"a` + strings.Repeat("b", 32) + `","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"key leading digit", "POST", "/v1/tenants/acme/entitlements", `{"key":"1seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"quantity zero", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":0,"expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"quantity too large", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":1000001,"expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"quantity fractional", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":10.5,"expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"quantity string", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":"10","expiresAt":"2030-01-01T00:00:00Z"}`, 400},
		{"quantity boundary", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":1000000,"expiresAt":"` + future + `"}`, 201},
		{"expiresAt past", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":10,"expiresAt":"2020-01-01T00:00:00Z"}`, 400},
		{"expiresAt not utc", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":10,"expiresAt":"2030-01-01T01:00:00+01:00"}`, 400},
		{"expiresAt malformed", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":10,"expiresAt":"2030-01-01"}`, 400},
		{"expiresAt missing", "POST", "/v1/tenants/acme/entitlements", `{"key":"seats","quantity":10}`, 400},
		{"body not json", "POST", "/v1/tenants/acme/entitlements", `not json`, 400},
		{"body trailing data", "POST", "/v1/tenants/acme/entitlements", validBody + ` {}`, 400},
		{"tenant empty", "POST", "/v1/tenants//entitlements", validBody, 404},
		{"tenant missing", "POST", "/v1/tenants/", validBody, 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := New(newFakeStore())
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body)))
			if response.Code != tc.status {
				t.Fatalf("status=%d want %d body=%q", response.Code, tc.status, response.Body.String())
			}
		})
	}
}

func TestCreateEntitlementIdempotency(t *testing.T) {
	backend := newFakeStore()
	handler := New(backend)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest("POST", "/v1/tenants/acme/entitlements", strings.NewReader(validBody)))
	if first.Code != 201 {
		t.Fatalf("first create status=%d", first.Code)
	}

	same := httptest.NewRecorder()
	handler.ServeHTTP(same, httptest.NewRequest("POST", "/v1/tenants/acme/entitlements", strings.NewReader(validBody)))
	if same.Code != 200 {
		t.Fatalf("idempotent replay status=%d body=%q", same.Code, same.Body.String())
	}
	var firstBody, sameBody map[string]any
	_ = json.Unmarshal(first.Body.Bytes(), &firstBody)
	_ = json.Unmarshal(same.Body.Bytes(), &sameBody)
	if firstBody["id"] != sameBody["id"] || firstBody["createdAt"] != sameBody["createdAt"] {
		t.Fatal("idempotent replay must return the original record")
	}

	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, httptest.NewRequest("POST", "/v1/tenants/acme/entitlements", strings.NewReader(`{"key":"seats","quantity":11,"expiresAt":"2030-01-01T00:00:00Z"}`)))
	if conflict.Code != 409 {
		t.Fatalf("conflict status=%d", conflict.Code)
	}
	conflictExpiry := httptest.NewRecorder()
	handler.ServeHTTP(conflictExpiry, httptest.NewRequest("POST", "/v1/tenants/acme/entitlements", strings.NewReader(`{"key":"seats","quantity":10,"expiresAt":"2031-01-01T00:00:00Z"}`)))
	if conflictExpiry.Code != 409 {
		t.Fatalf("conflict expiry status=%d", conflictExpiry.Code)
	}

	if backend.count() != 1 {
		t.Fatalf("expected exactly one record, got %d", backend.count())
	}
	stored, _ := backend.GetEntitlement(context.Background(), "acme", "seats")
	if stored.Quantity != 10 || stored.ExpiresAt.Format(time.RFC3339) != "2030-01-01T00:00:00Z" {
		t.Fatal("existing record must not be modified")
	}
}

func TestListAndGetEntitlements(t *testing.T) {
	backend := newFakeStore()
	handler := New(backend)
	for _, key := range []string{"seats", "api-calls", "storage_gb"} {
		body := fmt.Sprintf(`{"key":%q,"quantity":5,"expiresAt":"2030-01-01T00:00:00Z"}`, key)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("POST", "/v1/tenants/acme/entitlements", strings.NewReader(body)))
		if response.Code != 201 {
			t.Fatalf("create %s status=%d", key, response.Code)
		}
	}
	other := httptest.NewRecorder()
	handler.ServeHTTP(other, httptest.NewRequest("POST", "/v1/tenants/other/entitlements", strings.NewReader(validBody)))

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest("GET", "/v1/tenants/acme/entitlements", nil))
	if list.Code != 200 {
		t.Fatalf("list status=%d", list.Code)
	}
	var listBody struct {
		Items []entitlementJSON `json:"items"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &listBody); err != nil {
		t.Fatal(err)
	}
	if len(listBody.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(listBody.Items))
	}
	keys := []string{listBody.Items[0].Key, listBody.Items[1].Key, listBody.Items[2].Key}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("items not sorted by key: %v", keys)
	}
	for _, item := range listBody.Items {
		if item.Tenant != "acme" {
			t.Fatalf("cross-tenant record leaked: %v", item)
		}
	}

	withQuery := httptest.NewRecorder()
	handler.ServeHTTP(withQuery, httptest.NewRequest("GET", "/v1/tenants/acme/entitlements?key=seats", nil))
	if withQuery.Code != 400 {
		t.Fatalf("query parameters must be rejected, status=%d", withQuery.Code)
	}

	single := httptest.NewRecorder()
	handler.ServeHTTP(single, httptest.NewRequest("GET", "/v1/tenants/acme/entitlements/seats", nil))
	if single.Code != 200 {
		t.Fatalf("get status=%d", single.Code)
	}
	var singleBody entitlementJSON
	if err := json.Unmarshal(single.Body.Bytes(), &singleBody); err != nil {
		t.Fatal(err)
	}
	if singleBody.Key != "seats" || singleBody.Quantity != 5 {
		t.Fatalf("unexpected record %+v", singleBody)
	}

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest("GET", "/v1/tenants/acme/entitlements/unknown", nil))
	if missing.Code != 404 {
		t.Fatalf("unknown key status=%d", missing.Code)
	}
}

func TestEntitlementMethodNotAllowed(t *testing.T) {
	handler := New(newFakeStore())
	for _, tc := range []struct{ method, target string }{
		{"PUT", "/v1/tenants/acme/entitlements"},
		{"DELETE", "/v1/tenants/acme/entitlements/seats"},
		{"PATCH", "/v1/tenants/acme/entitlements/seats"},
		{"POST", "/v1/tenants/acme/entitlements/seats"},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.target, nil))
		if response.Code != 405 {
			t.Fatalf("%s %s status=%d want 405", tc.method, tc.target, response.Code)
		}
	}
}
