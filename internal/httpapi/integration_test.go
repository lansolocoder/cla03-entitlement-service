package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

const defaultDBURL = "postgres://127.0.0.1:15432/entitlements?sslmode=disable"

type harness struct {
	t       *testing.T
	pool    *pgxpool.Pool
	st      *store.Store
	server  *httptest.Server
	baseURL string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = defaultDBURL
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("database unavailable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("database unreachable: %v", err)
	}
	t.Cleanup(pool.Close)

	st := store.New(pool)
	if err := st.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`TRUNCATE idempotency_records, reservations, quota_pools`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	server := httptest.NewServer(httpapi.New(pool.Ping, st))
	t.Cleanup(server.Close)
	return &harness{t: t, pool: pool, st: st, server: server, baseURL: server.URL}
}

func poolPath(tenant, pool string) string {
	return fmt.Sprintf("/v1/tenants/%s/quota-pools/%s", tenant, pool)
}

func reservationsPath(tenant, pool string) string {
	return poolPath(tenant, pool) + "/reservations"
}

func reservationPath(tenant, pool, reservation string) string {
	return reservationsPath(tenant, pool) + "/" + reservation
}

type apiResponse struct {
	status int
	body   []byte
	header http.Header
}

func (h *harness) do(method, path string, body any, key string) apiResponse {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.baseURL+path, reader)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return apiResponse{status: resp.StatusCode, body: raw, header: resp.Header}
}

func (h *harness) raw(method, path string, rawBody string, key string) apiResponse {
	h.t.Helper()
	var reader io.Reader
	if rawBody != "" {
		reader = bytes.NewReader([]byte(rawBody))
	}
	req, err := http.NewRequest(method, h.baseURL+path, reader)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return apiResponse{status: resp.StatusCode, body: data}
}

func (h *harness) createPool(tenant, pool string, limit int, from, until time.Time) {
	h.t.Helper()
	resp := h.do(http.MethodPut, poolPath(tenant, pool), map[string]any{
		"limit":      limit,
		"validFrom":  from.Format(time.RFC3339),
		"validUntil": until.Format(time.RFC3339),
	}, "")
	if resp.status != http.StatusCreated {
		h.t.Fatalf("create pool: status=%d body=%s", resp.status, resp.body)
	}
}

func activeWindow() (time.Time, time.Time) {
	now := time.Now().UTC()
	return now.Add(-time.Hour), now.Add(24 * time.Hour)
}

func (r apiResponse) decode(t *testing.T, dst any) {
	t.Helper()
	if err := json.Unmarshal(r.body, dst); err != nil {
		t.Fatalf("decode %q: %v", r.body, err)
	}
}

func TestPutPoolCreateAndConflict(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()

	resp := h.do(http.MethodPut, poolPath("tA", "pA"), map[string]any{
		"limit": 100, "validFrom": from.Format(time.RFC3339), "validUntil": until.Format(time.RFC3339),
	}, "")
	if resp.status != http.StatusCreated {
		t.Fatalf("first PUT: %d %s", resp.status, resp.body)
	}
	var pool store.Pool
	resp.decode(t, &pool)
	if pool.Limit != 100 || pool.TenantID != "tA" || pool.PoolID != "pA" {
		t.Fatalf("unexpected pool: %+v", pool)
	}

	again := h.do(http.MethodPut, poolPath("tA", "pA"), map[string]any{
		"limit": 5, "validFrom": from.Format(time.RFC3339), "validUntil": until.Format(time.RFC3339),
	}, "")
	if again.status != http.StatusConflict {
		t.Fatalf("second PUT: got %d, want 409", again.status)
	}
}

func TestReservationLifecycleAndIdempotency(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "p1", 10, from, until)

	body := map[string]any{
		"reservationId": "r1",
		"amount":        4,
		"expiresAt":     until.Add(-time.Hour).Format(time.RFC3339),
	}
	first := h.do(http.MethodPost, reservationsPath("t1", "p1"), body, "key-1")
	if first.status != http.StatusCreated {
		t.Fatalf("create: %d %s", first.status, first.body)
	}
	var created store.Reservation
	first.decode(t, &created)
	if created.Status != store.StatusPending || created.Amount != 4 {
		t.Fatalf("unexpected reservation: %+v", created)
	}

	// Identical replay: 200 and the exact original record.
	replay := h.do(http.MethodPost, reservationsPath("t1", "p1"), body, "key-1")
	if replay.status != http.StatusOK {
		t.Fatalf("replay: got %d, want 200", replay.status)
	}
	if !bytes.Equal(replay.body, first.body) {
		t.Fatalf("replay body %s != original %s", replay.body, first.body)
	}

	// Same key, different payload: 409.
	different := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r1",
		"amount":        5,
		"expiresAt":     until.Add(-time.Hour).Format(time.RFC3339),
	}, "key-1")
	if different.status != http.StatusConflict {
		t.Fatalf("same key different body: got %d, want 409", different.status)
	}

	// Whitespace-only JSON reformatting is the same request.
	rawReplay := h.raw(http.MethodPost, reservationsPath("t1", "p1"),
		`{ "reservationId" : "r1" , "amount" : 4 , "expiresAt" : `+
			`"`+until.Add(-time.Hour).Format(time.RFC3339)+`" }`, "key-1")
	if rawReplay.status != http.StatusOK {
		t.Fatalf("whitespace replay: got %d, want 200", rawReplay.status)
	}

	// GET returns the full record.
	got := h.do(http.MethodGet, reservationPath("t1", "p1", "r1"), nil, "")
	if got.status != http.StatusOK {
		t.Fatalf("GET: %d %s", got.status, got.body)
	}
	var fetched store.Reservation
	got.decode(t, &fetched)
	if fetched.ReservationID != "r1" || fetched.Status != store.StatusPending {
		t.Fatalf("unexpected fetched record: %+v", fetched)
	}
}

func TestAvailabilityAndConflicts(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "p1", 10, from, until)
	path := reservationsPath("t1", "p1")
	expiry := until.Add(-time.Hour).Format(time.RFC3339)

	mk := func(id string, amount int, key string) int {
		return h.do(http.MethodPost, path, map[string]any{
			"reservationId": id, "amount": amount, "expiresAt": expiry,
		}, key).status
	}

	if s := mk("a", 6, "k-a"); s != http.StatusCreated {
		t.Fatalf("reservation of 6: %d", s)
	}
	// 6 used, only 4 available: 5 does not fit.
	if s := mk("b", 5, "k-b"); s != http.StatusConflict {
		t.Fatalf("reservation of 5 against 4 free: got %d, want 409", s)
	}
	// The failed request is itself an idempotent result: replaying it still
	// answers 200 with the original conflict body.
	replay := h.do(http.MethodPost, path, map[string]any{
		"reservationId": "b", "amount": 5, "expiresAt": expiry,
	}, "k-b")
	if replay.status != http.StatusOK || !bytes.Contains(replay.body, []byte("conflict")) {
		t.Fatalf("replay of failed request: %d %s, want 200 + original conflict", replay.status, replay.body)
	}
	// Reusing the same key with a different (fitting) amount is a conflict,
	// because it is a different request under the same key.
	if s := mk("b", 4, "k-b"); s != http.StatusConflict {
		t.Fatalf("same key different content: got %d, want 409", s)
	}
	// A fresh key with a fitting amount succeeds.
	if s := mk("b", 4, "k-b2"); s != http.StatusCreated {
		t.Fatalf("fitting amount under a new key: got %d, want 201", s)
	}
	// Pool is now fully consumed.
	if s := mk("c", 1, "k-c"); s != http.StatusConflict {
		t.Fatalf("reservation against exhausted pool: got %d, want 409", s)
	}

	// Reusing a reservation id also conflicts, independent of idempotency key.
	if s := mk("a", 1, "k-other"); s != http.StatusConflict {
		t.Fatalf("duplicate reservationId: got %d, want 409", s)
	}
}

func TestConcurrentReservationsNeverOversell(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	const limit = 10
	h.createPool("t1", "pC", limit, from, until)
	path := reservationsPath("t1", "pC")
	expiry := until.Add(-time.Hour).Format(time.RFC3339)

	const n = 40
	start := make(chan struct{})
	var wg sync.WaitGroup
	statuses := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := map[string]any{
				"reservationId": fmt.Sprintf("r-%d", i),
				"amount":        1,
				"expiresAt":     expiry,
			}
			<-start
			statuses[i] = h.do(http.MethodPost, path, body, fmt.Sprintf("k-%d", i)).status
		}(i)
	}
	close(start)
	wg.Wait()

	created, conflicts := 0, 0
	for _, s := range statuses {
		switch s {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("unexpected status %d", s)
		}
	}
	if created != limit {
		t.Fatalf("created=%d want exactly %d (conflicts=%d)", created, limit, conflicts)
	}

	var held int
	h.pool.QueryRow(context.Background(), `
		SELECT COALESCE(SUM(amount), 0) FROM reservations
		WHERE tenant_id='t1' AND pool_id='pC' AND status IN ('pending','confirmed')`,
	).Scan(&held)
	if held != limit {
		t.Fatalf("held quota=%d want %d", held, limit)
	}
}

func TestConcurrentSameKeySingleOutcome(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "pK", 10, from, until)
	path := reservationsPath("t1", "pK")
	body := map[string]any{
		"reservationId": "only",
		"amount":        3,
		"expiresAt":     until.Add(-time.Hour).Format(time.RFC3339),
	}

	const n = 20
	start := make(chan struct{})
	var wg sync.WaitGroup
	created, replayed := 0, 0
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp := h.do(http.MethodPost, path, body, "shared-key")
			mu.Lock()
			defer mu.Unlock()
			switch resp.status {
			case http.StatusCreated:
				created++
			case http.StatusOK:
				replayed++
			default:
				t.Errorf("unexpected status %d body=%s", resp.status, resp.body)
			}
		}()
	}
	close(start)
	wg.Wait()

	if created != 1 || replayed != n-1 {
		t.Fatalf("created=%d replayed=%d, want exactly one 201 and %d replays(200)", created, replayed, n-1)
	}

	// Exactly one reservation row exists and holds the expected quota.
	var count, held int
	h.pool.QueryRow(context.Background(), `
		SELECT count(*), COALESCE(SUM(amount), 0) FROM reservations
		WHERE tenant_id='t1' AND pool_id='pK'`).Scan(&count, &held)
	if count != 1 || held != 3 {
		t.Fatalf("rows=%d held=%d, want 1 row holding 3", count, held)
	}
}

func TestPoolValidityWindow(t *testing.T) {
	h := newHarness(t)
	now := time.Now().UTC()
	expiry := now.Add(time.Hour).Format(time.RFC3339)

	// Not yet valid.
	h.createPool("t1", "future", 10, now.Add(time.Hour), now.Add(2*time.Hour))
	if s := h.do(http.MethodPost, reservationsPath("t1", "future"), map[string]any{
		"reservationId": "r", "amount": 1, "expiresAt": expiry,
	}, "k").status; s != http.StatusConflict {
		t.Fatalf("future pool: got %d, want 409", s)
	}

	// Already expired.
	h.createPool("t1", "past", 10, now.Add(-2*time.Hour), now.Add(-time.Hour))
	if s := h.do(http.MethodPost, reservationsPath("t1", "past"), map[string]any{
		"reservationId": "r", "amount": 1, "expiresAt": expiry,
	}, "k2").status; s != http.StatusConflict {
		t.Fatalf("expired pool: got %d, want 409", s)
	}

	// Active pool, but reservation expiry beyond pool end or in the past.
	h.createPool("t1", "active", 10, now.Add(-time.Hour), now.Add(time.Hour))
	for _, bad := range []string{
		now.Add(2 * time.Hour).Format(time.RFC3339), // after pool end
		now.Add(-time.Minute).Format(time.RFC3339),  // in the past
	} {
		if s := h.do(http.MethodPost, reservationsPath("t1", "active"), map[string]any{
			"reservationId": "r", "amount": 1, "expiresAt": bad,
		}, "k-"+bad).status; s != http.StatusConflict {
			t.Fatalf("expiresAt=%s: got %d, want 409", bad, s)
		}
	}
	// Exactly at pool end is permitted ("not later than pool end").
	if s := h.do(http.MethodPost, reservationsPath("t1", "active"), map[string]any{
		"reservationId": "r-edge", "amount": 1,
		"expiresAt": now.Add(time.Hour).Format(time.RFC3339),
	}, "k-edge").status; s != http.StatusCreated {
		t.Fatalf("expiresAt == pool end: got %d, want 201", s)
	}

	// Missing pool: 409 (precondition fails), never a leak.
	if s := h.do(http.MethodPost, reservationsPath("t1", "ghost"), map[string]any{
		"reservationId": "r", "amount": 1, "expiresAt": expiry,
	}, "k3").status; s != http.StatusConflict {
		t.Fatalf("missing pool: got %d, want 409", s)
	}
}

func TestConfirmAndRelease(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "p1", 10, from, until)
	expiry := until.Add(-time.Hour).Format(time.RFC3339)
	mk := func(id, key string, amount int) {
		if s := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
			"reservationId": id, "amount": amount, "expiresAt": expiry,
		}, key).status; s != http.StatusCreated {
			t.Fatalf("create %s: %d", id, s)
		}
	}

	// Confirm succeeds and returns the updated record.
	mk("r-confirm", "kc", 3)
	resp := h.do(http.MethodPost, reservationPath("t1", "p1", "r-confirm")+"/confirm", nil, "kc-confirm")
	if resp.status != http.StatusOK {
		t.Fatalf("confirm: %d %s", resp.status, resp.body)
	}
	var confirmed store.Reservation
	resp.decode(t, &confirmed)
	if confirmed.Status != store.StatusConfirmed || confirmed.ConfirmedAt == nil {
		t.Fatalf("not confirmed: %+v", confirmed)
	}
	// Confirmed quota still counts against availability: 7 remain.
	if s := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r-over", "amount": 8, "expiresAt": expiry,
	}, "k-over").status; s != http.StatusConflict {
		t.Fatalf("confirmed quota not held: got %d, want 409", s)
	}
	// Double confirm is a state conflict.
	if s := h.do(http.MethodPost, reservationPath("t1", "p1", "r-confirm")+"/confirm", nil, "kc-confirm-2").status; s != http.StatusConflict {
		t.Fatalf("double confirm: got %d, want 409", s)
	}
	// Releasing a confirmed reservation is a state conflict.
	if s := h.do(http.MethodPost, reservationPath("t1", "p1", "r-confirm")+"/release", nil, "kr-release-2").status; s != http.StatusConflict {
		t.Fatalf("release confirmed: got %d, want 409", s)
	}

	// Release a pending reservation; quota returns to the pool.
	mk("r-release", "kr", 7)
	if s := h.do(http.MethodPost, reservationPath("t1", "p1", "r-release")+"/release", nil, "kr-release").status; s != http.StatusOK {
		t.Fatalf("release: got %d, want 200", s)
	}
	// 3 confirmed, 7 released => 7 free again.
	if s := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r-after", "amount": 7, "expiresAt": expiry,
	}, "k-after").status; s != http.StatusCreated {
		t.Fatalf("quota not freed after release: got %d, want 201", s)
	}
	// Confirming a released reservation is a state conflict.
	if s := h.do(http.MethodPost, reservationPath("t1", "p1", "r-release")+"/confirm", nil, "kr-confirm-3").status; s != http.StatusConflict {
		t.Fatalf("confirm released: got %d, want 409", s)
	}

	// Actions on unknown / cross-tenant records are 404.
	if s := h.do(http.MethodPost, reservationPath("t1", "p1", "ghost")+"/confirm", nil, "k-ghost").status; s != http.StatusNotFound {
		t.Fatalf("confirm unknown: got %d, want 404", s)
	}
	if s := h.do(http.MethodPost, reservationPath("other", "p1", "r-confirm")+"/confirm", nil, "k-cross").status; s != http.StatusNotFound {
		t.Fatalf("confirm cross-tenant: got %d, want 404", s)
	}
}

func TestConcurrentConfirmReleaseAtMostOne(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "p1", 10, from, until)
	mk := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r1", "amount": 1,
		"expiresAt": until.Add(-time.Hour).Format(time.RFC3339),
	}, "k-create")
	if mk.status != http.StatusCreated {
		t.Fatalf("create: %d", mk.status)
	}

	target := reservationPath("t1", "p1", "r1")
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]int, 2)
	for i, action := range []string{"confirm", "release"} {
		wg.Add(1)
		go func(i int, action string) {
			defer wg.Done()
			<-start
			results[i] = h.do(http.MethodPost, target+"/"+action, nil,
				"k-"+action).status
		}(i, action)
	}
	close(start)
	wg.Wait()

	ok := 0
	for _, s := range results {
		if s != http.StatusOK && s != http.StatusConflict {
			t.Fatalf("unexpected status %d", s)
		}
		if s == http.StatusOK {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("exactly one action must succeed, got %d", ok)
	}

	got := h.do(http.MethodGet, target, nil, "")
	var final store.Reservation
	got.decode(t, &final)
	if final.Status != store.StatusConfirmed && final.Status != store.StatusReleased {
		t.Fatalf("final status %q", final.Status)
	}
}

func TestExpiredPendingReleasesQuota(t *testing.T) {
	h := newHarness(t)
	now := time.Now().UTC()
	// Pool window accommodates the short-lived reservation.
	h.createPool("t1", "p1", 10, now.Add(-time.Hour), now.Add(time.Hour))

	created := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "short",
		"amount":        10,
		"expiresAt":     now.Add(700 * time.Millisecond).Format(time.RFC3339Nano),
	}, "k-short")
	if created.status != http.StatusCreated {
		t.Fatalf("create: %d %s", created.status, created.body)
	}
	// Pool is full.
	full := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "other",
		"amount":        1,
		"expiresAt":     now.Add(30 * time.Minute).Format(time.RFC3339),
	}, "k-other")
	if full.status != http.StatusConflict {
		t.Fatalf("pool should be full: %d", full.status)
	}

	time.Sleep(1100 * time.Millisecond)

	// GET transitions the stale pending record to expired.
	got := h.do(http.MethodGet, reservationPath("t1", "p1", "short"), nil, "")
	var expired store.Reservation
	got.decode(t, &expired)
	if expired.Status != store.StatusExpired {
		t.Fatalf("expected expired, got %q", expired.Status)
	}

	// Confirming the expired pending reservation is rejected.
	if s := h.do(http.MethodPost, reservationPath("t1", "p1", "short")+"/confirm", nil, "k-exp-confirm").status; s != http.StatusConflict {
		t.Fatalf("confirm expired: got %d, want 409", s)
	}

	// Its quota is released: the full limit fits again.
	retry := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "other2",
		"amount":        10,
		"expiresAt":     now.Add(30 * time.Minute).Format(time.RFC3339),
	}, "k-other2")
	if retry.status != http.StatusCreated {
		t.Fatalf("quota should be released: %d %s", retry.status, retry.body)
	}
}

func TestGetReservationNotFoundAndTenantIsolation(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "p1", 5, from, until)
	created := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r1", "amount": 1,
		"expiresAt": until.Add(-time.Hour).Format(time.RFC3339),
	}, "k")
	if created.status != http.StatusCreated {
		t.Fatalf("create: %d", created.status)
	}

	for _, path := range []string{
		reservationPath("t1", "p1", "missing"),
		reservationPath("t2", "p1", "r1"),
		reservationPath("t1", "other-pool", "r1"),
	} {
		if s := h.do(http.MethodGet, path, nil, "").status; s != http.StatusNotFound {
			t.Fatalf("GET %s: got %d, want 404", path, s)
		}
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "p1", 10, from, until)
	created := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r1", "amount": 2,
		"expiresAt": until.Add(-time.Hour).Format(time.RFC3339),
	}, "k1")
	if created.status != http.StatusCreated {
		t.Fatalf("create: %d", created.status)
	}

	// Simulate a process restart: a brand-new pool/store/handler over the
	// same database.
	st2 := store.New(h.pool)
	if err := st2.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("re-ensure schema: %v", err)
	}
	server2 := httptest.NewServer(httpapi.New(h.pool.Ping, st2))
	defer server2.Close()

	req, _ := http.NewRequest(http.MethodGet,
		server2.URL+reservationPath("t1", "p1", "r1"), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("after restart: %d", resp.StatusCode)
	}
	var r store.Reservation
	json.NewDecoder(resp.Body).Decode(&r)
	if r.Amount != 2 || r.Status != store.StatusPending {
		t.Fatalf("data did not survive restart: %+v", r)
	}

	// The idempotency record survives too: replay on the new instance is 200.
	replay := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r1", "amount": 2,
		"expiresAt": until.Add(-time.Hour).Format(time.RFC3339),
	}, "k1")
	if replay.status != http.StatusOK {
		t.Fatalf("replay after restart: got %d, want 200", replay.status)
	}
}

func TestHealthRemainsAvailable(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct {
		path   string
		status int
		body   string
	}{
		{"/healthz", http.StatusOK, "ok"},
		{"/readyz", http.StatusOK, "ready"},
	} {
		resp, err := http.Get(h.baseURL + tc.path)
		if err != nil {
			t.Fatalf("get %s: %v", tc.path, err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.status || !bytes.Contains(data, []byte(tc.body)) {
			t.Fatalf("%s: %d %s", tc.path, resp.StatusCode, data)
		}
	}
}

func TestInternalErrorsDoNotLeak(t *testing.T) {
	h := newHarness(t)
	from, until := activeWindow()
	h.createPool("t1", "p1", 10, from, until)

	// Force storage to fail by closing the pool mid-request; the public
	// response must be a generic 503 with no driver internals.
	h.pool.Close()
	resp := h.do(http.MethodPost, reservationsPath("t1", "p1"), map[string]any{
		"reservationId": "r1", "amount": 1,
		"expiresAt": until.Add(-time.Hour).Format(time.RFC3339),
	}, "k1")
	if resp.status != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", resp.status)
	}
	lower := bytes.ToLower(resp.body)
	if bytes.Contains(lower, []byte("pgx")) || bytes.Contains(lower, []byte("sql")) ||
		bytes.Contains(lower, []byte("connection")) {
		t.Fatalf("internal detail leaked: %s", resp.body)
	}
}
