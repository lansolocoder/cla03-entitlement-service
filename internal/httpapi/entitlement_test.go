package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lansolocoder/cla03-entitlement-service/internal/httpapi"
	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

func testServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Skip("TEST_DATABASE_URL or DATABASE_URL must be set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE usages, allocations, entitlements RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	st := store.New(pool)
	srv := httptest.NewServer(httpapi.New(pool.Ping, st))
	t.Cleanup(srv.Close)
	return srv, st
}

func doJSON(t *testing.T, method, url string, body any, headers ...map[string]string) (int, map[string]any, http.Header) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for _, h := range headers {
		for k, v := range h {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed, resp.Header
}

func mustCreate(t *testing.T, srv *httptest.Server, teamID, typ string, total int64, expiresAt time.Time) map[string]any {
	t.Helper()
	status, body, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements", map[string]any{
		"teamId": teamID, "type": typ, "total": total,
		"expiresAt": expiresAt.UTC().Format(time.RFC3339Nano),
	})
	if status != http.StatusCreated {
		t.Fatalf("create status=%d body=%v", status, body)
	}
	return body
}

func TestHTTPCreateEntitlement(t *testing.T) {
	srv, _ := testServer(t)
	expires := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	body := mustCreate(t, srv, "team-x", "seat", 3, expires)
	if body["teamId"] != "team-x" || body["type"] != "seat" ||
		body["total"].(float64) != 3 || body["status"] != "active" ||
		body["used"].(float64) != 0 || body["version"].(float64) != 1 ||
		body["id"].(string) == "" {
		t.Fatalf("unexpected body: %v", body)
	}
	expStr, _ := body["expiresAt"].(string)
	parsed, err := time.Parse(time.RFC3339, expStr)
	if err != nil || !parsed.Equal(expires) {
		t.Fatalf("expiresAt=%s err=%v", expStr, err)
	}

	// Duplicate create -> 409 and the existing record comes back unchanged.
	status, dup, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements", map[string]any{
		"teamId": "team-x", "type": "seat", "total": 77,
		"expiresAt": expires.Format(time.RFC3339Nano),
	})
	if status != http.StatusConflict {
		t.Fatalf("dup status=%d body=%v", status, dup)
	}
	existing, _ := dup["entitlement"].(map[string]any)
	if existing["id"] != body["id"] || existing["total"].(float64) != 3 {
		t.Fatalf("existing record mismatch: %v", existing)
	}
}

func TestHTTPValidationErrors(t *testing.T) {
	srv, _ := testServer(t)
	future := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)

	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing teamId", map[string]any{"type": "seat", "total": 1, "expiresAt": future}},
		{"bad type", map[string]any{"teamId": "t", "type": "token", "total": 1, "expiresAt": future}},
		{"zero total", map[string]any{"teamId": "t", "type": "seat", "total": 0, "expiresAt": future}},
		{"bad expiresAt", map[string]any{"teamId": "t", "type": "seat", "total": 1, "expiresAt": "nope"}},
		{"past expiresAt", map[string]any{"teamId": "t", "type": "seat", "total": 1, "expiresAt": time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements", tc.body)
			if status != http.StatusBadRequest || body["code"] == nil {
				t.Fatalf("status=%d body=%v, want 400 with code", status, body)
			}
		})
	}

	// Malformed JSON.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/entitlements", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed json status=%d, want 400", resp.StatusCode)
	}

	// Allocation validation.
	created := mustCreate(t, srv, "team-v", "seat", 1, time.Now().UTC().Add(time.Hour))
	id := created["id"].(string)
	status, body, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/allocations",
		map[string]any{"memberId": "m1", "amount": 0})
	if status != http.StatusBadRequest {
		t.Fatalf("zero amount status=%d body=%v", status, body)
	}
	status, _, _ = doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/usages",
		map[string]any{"memberId": "m1", "amount": 1})
	if status != http.StatusBadRequest {
		t.Fatalf("missing usageKey status=%d, want 400", status)
	}
}

func TestHTTPFullLifecycle(t *testing.T) {
	srv, _ := testServer(t)
	created := mustCreate(t, srv, "team-life", "quota", 10, time.Now().UTC().Add(2*time.Hour))
	id := created["id"].(string)

	// Allocate 4 to m1.
	status, alloc, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/allocations",
		map[string]any{"memberId": "m1", "amount": 4})
	if status != http.StatusOK || alloc["used"].(float64) != 4 || alloc["version"].(float64) != 2 {
		t.Fatalf("allocate: status=%d body=%v", status, alloc)
	}

	// Over-capacity allocation -> 409, record unchanged.
	status, body, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/allocations",
		map[string]any{"memberId": "m2", "amount": 99})
	if status != http.StatusConflict || body["code"] != "capacity_exceeded" {
		t.Fatalf("over capacity: status=%d body=%v", status, body)
	}

	// Consume 3.
	status, usage, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/usages",
		map[string]any{"memberId": "m1", "amount": 3, "usageKey": "u1"})
	if status != http.StatusOK || usage["used"].(float64) != 7 || usage["version"].(float64) != 3 {
		t.Fatalf("consume: status=%d body=%v", status, usage)
	}

	// Replay same key: identical body, no double count.
	status, replay, hdr := doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/usages",
		map[string]any{"memberId": "m1", "amount": 3, "usageKey": "u1"})
	if status != http.StatusOK || replay["used"].(float64) != 7 || replay["version"].(float64) != 3 {
		t.Fatalf("replay: status=%d body=%v", status, replay)
	}
	if hdr.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("missing replay header: %v", hdr)
	}

	// Usage beyond member allowance -> 409.
	status, body, _ = doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/usages",
		map[string]any{"memberId": "m1", "amount": 2, "usageKey": "u2"})
	if status != http.StatusConflict || body["code"] != "insufficient_allowance" {
		t.Fatalf("over allowance: status=%d body=%v", status, body)
	}

	// Details grouped by member.
	status, details, _ := doJSON(t, http.MethodGet, srv.URL+"/entitlements/"+id, nil)
	if status != http.StatusOK {
		t.Fatalf("get: status=%d body=%v", status, details)
	}
	members, _ := details["members"].([]any)
	if len(members) != 1 {
		t.Fatalf("members=%v", members)
	}
	m := members[0].(map[string]any)
	if m["memberId"] != "m1" || m["used"].(float64) != 3 {
		t.Fatalf("member detail=%v", m)
	}
	keys, _ := m["usageKeys"].([]any)
	if len(keys) != 1 || keys[0] != "u1" {
		t.Fatalf("usageKeys=%v", keys)
	}

	// Cancel then operations refused.
	status, cancelled, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+id+"/cancel", nil)
	if status != http.StatusOK || cancelled["status"] != "cancelled" || cancelled["used"].(float64) != 7 {
		t.Fatalf("cancel: status=%d body=%v", status, cancelled)
	}
	for _, op := range []string{"allocations", "usages"} {
		payload := map[string]any{"memberId": "m1", "amount": 1}
		if op == "usages" {
			payload["usageKey"] = "after-cancel"
		}
		status, body, _ = doJSON(t, http.MethodPost,
			fmt.Sprintf("%s/entitlements/%s/%s", srv.URL, id, op), payload)
		if status != http.StatusConflict {
			t.Fatalf("%s after cancel status=%d body=%v", op, status, body)
		}
	}
	// Still readable.
	status, details, _ = doJSON(t, http.MethodGet, srv.URL+"/entitlements/"+id, nil)
	if status != http.StatusOK || details["status"] != "cancelled" {
		t.Fatalf("read after cancel: status=%d body=%v", status, details)
	}
}

func TestHTTPExpiredIsReadOnly(t *testing.T) {
	srv, st := testServer(t)
	// Create directly with a past expiry (the API rejects past expiries).
	e, err := st.Create(context.Background(), "team-exp", "seat", 5, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatalf("seed expired: %v", err)
	}
	status, body, _ := doJSON(t, http.MethodGet, srv.URL+"/entitlements/"+e.ID, nil)
	if status != http.StatusOK || body["status"] != "expired" {
		t.Fatalf("read expired: status=%d body=%v", status, body)
	}
	status, body, _ = doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+e.ID+"/allocations",
		map[string]any{"memberId": "m1", "amount": 1})
	if status != http.StatusConflict || body["code"] != "entitlement_expired" {
		t.Fatalf("allocate expired: status=%d body=%v", status, body)
	}
	status, body, _ = doJSON(t, http.MethodPost, srv.URL+"/entitlements/"+e.ID+"/usages",
		map[string]any{"memberId": "m1", "amount": 1, "usageKey": "k"})
	if status != http.StatusConflict || body["code"] != "entitlement_expired" {
		t.Fatalf("use expired: status=%d body=%v", status, body)
	}
}

func TestHTTPUnknownRoutes(t *testing.T) {
	srv, _ := testServer(t)
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/entitlements", http.StatusNotFound},
		{http.MethodGet, "/entitlements/not-a-uuid", http.StatusNotFound},
		{http.MethodGet, "/entitlements/00000000-0000-0000-0000-000000000000", http.StatusNotFound},
		{http.MethodDelete, "/entitlements/00000000-0000-0000-0000-000000000000", http.StatusMethodNotAllowed},
		{http.MethodPost, "/entitlements/00000000-0000-0000-0000-000000000000/allocations/extra", http.StatusNotFound},
	} {
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, bytes.NewReader([]byte("{}")))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("%s %s = %d, want %d", tc.method, tc.path, resp.StatusCode, tc.want)
		}
	}
}

func TestHTTPConcurrentCreateIsAtomic(t *testing.T) {
	srv, _ := testServer(t)
	expires := time.Now().UTC().Add(3 * time.Hour).Truncate(time.Microsecond).Format(time.RFC3339Nano)
	const n = 8
	var wg sync.WaitGroup
	statuses := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, _ := doJSON(t, http.MethodPost, srv.URL+"/entitlements", map[string]any{
				"teamId": "team-race", "type": "seat", "total": 5, "expiresAt": expires,
			})
			statuses <- status
		}()
	}
	wg.Wait()
	close(statuses)
	var created, conflicts int
	for s := range statuses {
		switch s {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("unexpected status %d", s)
		}
	}
	if created != 1 || conflicts != n-1 {
		t.Fatalf("created=%d conflicts=%d, want exactly one 201", created, conflicts)
	}
}
