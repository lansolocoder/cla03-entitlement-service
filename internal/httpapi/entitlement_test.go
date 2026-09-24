package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

// fakeStore is an in-memory Store that models the database unique constraint
// so handler tests exercise the same idempotent/conflict contract.
type fakeStore struct {
	mu    sync.Mutex
	rows  map[string]map[string]*entitlement.Entitlement // tenant -> key
	idSeq int
	err   error // forced error, if any
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: make(map[string]map[string]*entitlement.Entitlement)}
}

func (f *fakeStore) Create(_ context.Context, tenant string, input entitlement.CreateInput) (*entitlement.Entitlement, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, false, f.err
	}
	byKey := f.rows[tenant]
	if byKey == nil {
		byKey = make(map[string]*entitlement.Entitlement)
		f.rows[tenant] = byKey
	}
	if existing, ok := byKey[input.Key]; ok {
		if existing.Quantity == input.Quantity && existing.ExpiresAt.Equal(input.ExpiresAt) {
			return existing, false, nil
		}
		return nil, false, &entitlement.ConflictError{Existing: existing}
	}
	f.idSeq++
	record := &entitlement.Entitlement{
		ID:        fmt.Sprintf("00000000-0000-0000-0000-%012d", f.idSeq),
		Tenant:    tenant,
		Key:       input.Key,
		Quantity:  input.Quantity,
		Status:    "active",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: input.ExpiresAt,
	}
	byKey[input.Key] = record
	return record, true, nil
}

func (f *fakeStore) List(_ context.Context, tenant string) ([]*entitlement.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var items []*entitlement.Entitlement
	for _, e := range f.rows[tenant] {
		items = append(items, e)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil
}

func (f *fakeStore) Get(_ context.Context, tenant, key string) (*entitlement.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if e, ok := f.rows[tenant][key]; ok {
		return e, nil
	}
	return nil, entitlement.ErrNotFound
}

func (f *fakeStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, byKey := range f.rows {
		n += len(byKey)
	}
	return n
}

func newTestHandler(store entitlement.Store) http.Handler {
	return New(func(context.Context) error { return nil }, store)
}

func doRequest(t *testing.T, handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestCreateEntitlementSuccess(t *testing.T) {
	store := newFakeStore()
	handler := newTestHandler(store)
	body := `{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`
	rec := doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements", body)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	raw := rec.Body.String()
	if !strings.HasSuffix(raw, "\n") {
		t.Fatalf("body must end with newline: %q", raw)
	}
	var got struct {
		ID        string `json:"id"`
		Tenant    string `json:"tenant"`
		Key       string `json:"key"`
		Quantity  int64  `json:"quantity"`
		Consumed  int64  `json:"consumed"`
		Reserved  int64  `json:"reserved"`
		Status    string `json:"status"`
		CreatedAt string `json:"createdAt"`
		ExpiresAt string `json:"expiresAt"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.Tenant != "acme" || got.Key != "seats" || got.Quantity != 10 {
		t.Fatalf("unexpected record: %+v", got)
	}
	if got.Consumed != 0 || got.Reserved != 0 || got.Status != "active" {
		t.Fatalf("defaults wrong: %+v", got)
	}
	if got.ExpiresAt != "2030-01-01T00:00:00Z" {
		t.Fatalf("expiresAt = %q", got.ExpiresAt)
	}
	if !strings.HasSuffix(got.CreatedAt, "Z") {
		t.Fatalf("createdAt must be UTC RFC3339, got %q", got.CreatedAt)
	}
	createdAt, err := time.Parse(time.RFC3339, got.CreatedAt)
	if err != nil {
		t.Fatalf("createdAt not RFC3339: %v", err)
	}
	if now := time.Now().UTC(); createdAt.After(now.Add(2*time.Second)) || createdAt.Before(now.Add(-2*time.Minute)) {
		t.Fatalf("createdAt %v not near server time %v", createdAt, now)
	}
	if strings.Count(raw, `"quantity":10`) != 1 {
		t.Fatalf("quantity must serialize as integer: %q", raw)
	}
}

func TestCreateIdempotentAndConflict(t *testing.T) {
	store := newFakeStore()
	handler := newTestHandler(store)

	first := doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements",
		`{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`)
	if first.Code != 201 {
		t.Fatalf("first create: %d %s", first.Code, first.Body.String())
	}

	identical := doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements",
		`{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`)
	if identical.Code != 200 {
		t.Fatalf("identical duplicate should be 200, got %d: %s", identical.Code, identical.Body.String())
	}
	if store.count() != 1 {
		t.Fatalf("identical duplicate must not create a row, count=%d", store.count())
	}

	differentQty := doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements",
		`{"key":"seats","quantity":11,"expiresAt":"2030-01-01T00:00:00Z"}`)
	if differentQty.Code != 409 {
		t.Fatalf("different quantity should be 409, got %d", differentQty.Code)
	}
	differentExpiry := doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements",
		`{"key":"seats","quantity":10,"expiresAt":"2031-01-01T00:00:00Z"}`)
	if differentExpiry.Code != 409 {
		t.Fatalf("different expiry should be 409, got %d", differentExpiry.Code)
	}
	if store.count() != 1 {
		t.Fatalf("conflict must leave the original record untouched, count=%d", store.count())
	}

	// Same key for a different tenant is independent.
	other := doRequest(t, handler, http.MethodPost, "/v1/tenants/globex/entitlements",
		`{"key":"seats","quantity":11,"expiresAt":"2030-01-01T00:00:00Z"}`)
	if other.Code != 201 {
		t.Fatalf("other tenant create: %d", other.Code)
	}
}

func TestCreateValidationFailures(t *testing.T) {
	store := newFakeStore()
	handler := newTestHandler(store)
	future := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	post := func(body string) *httptest.ResponseRecorder {
		return doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements", body)
	}
	bad := map[string]string{
		"bad key uppercase":     `{"key":"Seats","quantity":10,"expiresAt":"` + future + `"}`,
		"bad key short":         `{"key":"a","quantity":10,"expiresAt":"` + future + `"}`,
		"bad key leading digit": `{"key":"1seats","quantity":10,"expiresAt":"` + future + `"}`,
		"key too long":          `{"key":"` + strings.Repeat("a", 33) + `","quantity":10,"expiresAt":"` + future + `"}`,
		"quantity zero":         `{"key":"seats","quantity":0,"expiresAt":"` + future + `"}`,
		"quantity negative":     `{"key":"seats","quantity":-1,"expiresAt":"` + future + `"}`,
		"quantity too large":    `{"key":"seats","quantity":1000001,"expiresAt":"` + future + `"}`,
		"quantity fraction":     `{"key":"seats","quantity":1.5,"expiresAt":"` + future + `"}`,
		"quantity string":       `{"key":"seats","quantity":"10","expiresAt":"` + future + `"}`,
		"expiry in past":        `{"key":"seats","quantity":10,"expiresAt":"2000-01-01T00:00:00Z"}`,
		"expiry non-UTC":        `{"key":"seats","quantity":10,"expiresAt":"2030-01-01T02:00:00+02:00"}`,
		"expiry malformed":      `{"key":"seats","quantity":10,"expiresAt":"not-a-time"}`,
		"missing key":           `{"quantity":10,"expiresAt":"` + future + `"}`,
		"missing quantity":      `{"key":"seats","expiresAt":"` + future + `"}`,
		"missing expiry":        `{"key":"seats","quantity":10}`,
		"empty body":            ``,
		"not json":              `not-json`,
		"unknown field":         `{"key":"seats","quantity":10,"expiresAt":"` + future + `","extra":true}`,
		"null body":             `null`,
	}
	for name, body := range bad {
		t.Run(name, func(t *testing.T) {
			rec := post(body)
			if rec.Code != 400 {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			if !strings.HasSuffix(rec.Body.String(), "\n") {
				t.Fatal("body must end with newline")
			}
		})
	}
	// expiresAt exactly equal to now cannot be in the future; allow a tiny
	// tolerance by asserting an explicit past-equal boundary is rejected.
	rec := post(`{"key":"seats","quantity":10,"expiresAt":"1970-01-01T00:00:00Z"}`)
	if rec.Code != 400 {
		t.Fatalf("epoch expiry should be 400, got %d", rec.Code)
	}
	if store.count() != 0 {
		t.Fatalf("failed validations must not persist anything, count=%d", store.count())
	}
}

func TestTenantAndRouting(t *testing.T) {
	store := newFakeStore()
	handler := newTestHandler(store)
	body := `{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`

	for _, tc := range []struct {
		name, method, target string
		body                 string
		status               int
	}{
		{"missing tenant segment", http.MethodPost, "/v1/tenants//entitlements", body, 404},
		{"no tenant path", http.MethodGet, "/v1/tenants/", "", 404},
		{"unknown api path", http.MethodGet, "/v1/tenants/acme/other", "", 404},
		{"deep unknown path", http.MethodGet, "/v1/tenants/acme/entitlements/seats/extra", "", 404},
		{"unrelated path", http.MethodGet, "/nope", "", 404},
		{"delete not allowed", http.MethodDelete, "/v1/tenants/acme/entitlements", "", 405},
		{"put not allowed", http.MethodPut, "/v1/tenants/acme/entitlements/seats", "", 405},
		{"post single not allowed", http.MethodPost, "/v1/tenants/acme/entitlements/seats", body, 405},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, handler, tc.method, tc.target, tc.body)
			if rec.Code != tc.status {
				t.Fatalf("expected %d, got %d: %s", tc.status, rec.Code, rec.Body.String())
			}
			if !strings.HasSuffix(rec.Body.String(), "\n") {
				t.Fatalf("error response must end with newline: %q", rec.Body.String())
			}
			var envelope map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("error response must be a JSON object: %v", err)
			}
		})
	}
}

func TestListAndGet(t *testing.T) {
	store := newFakeStore()
	handler := newTestHandler(store)
	for _, key := range []string{"zebra", "alpha", "seats"} {
		rec := doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements",
			fmt.Sprintf(`{"key":%q,"quantity":5,"expiresAt":"2030-01-01T00:00:00Z"}`, key))
		if rec.Code != 201 {
			t.Fatalf("create %s: %d %s", key, rec.Code, rec.Body.String())
		}
	}

	t.Run("list sorted by key", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/v1/tenants/acme/entitlements", "")
		if rec.Code != 200 {
			t.Fatalf("status %d", rec.Code)
		}
		var got struct {
			Items []struct {
				Key string `json:"key"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		keys := []string{"alpha", "seats", "zebra"}
		if len(got.Items) != len(keys) {
			t.Fatalf("got %d items", len(got.Items))
		}
		for i, want := range keys {
			if got.Items[i].Key != want {
				t.Fatalf("position %d = %q, want %q", i, got.Items[i].Key, want)
			}
		}
	})

	t.Run("list rejects query params", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/v1/tenants/acme/entitlements?key=seats", "")
		if rec.Code != 400 {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("empty tenant list", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/v1/tenants/newco/entitlements", "")
		if rec.Code != 200 || rec.Body.String() != `{"items":[]}`+"\n" {
			t.Fatalf("empty list wrong: %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("get one", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/v1/tenants/acme/entitlements/seats", "")
		if rec.Code != 200 {
			t.Fatalf("status %d", rec.Code)
		}
		var got map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got["key"] != "seats" || got["tenant"] != "acme" {
			t.Fatalf("wrong record: %v", got)
		}
	})

	t.Run("unknown key 404", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/v1/tenants/acme/entitlements/nope", "")
		if rec.Code != 404 {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("unknown tenant 404", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/v1/tenants/newco/entitlements/seats", "")
		if rec.Code != 404 {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})
}

func TestStoreErrorsAre500WithoutLeak(t *testing.T) {
	store := newFakeStore()
	store.err = errors.New("dial tcp: secret database hostname")
	handler := newTestHandler(store)
	body := `{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`

	rec := doRequest(t, handler, http.MethodPost, "/v1/tenants/acme/entitlements", body)
	if rec.Code != 500 {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("internal error leaked: %s", rec.Body.String())
	}
}
