package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testDatabaseURL() string {
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://127.0.0.1:15432/entitlements_test?sslmode=disable"
}

type env struct {
	t       *testing.T
	pool    *pgxpool.Pool
	handler http.Handler
	seq     int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testDatabaseURL())
	if err != nil {
		t.Skipf("test database unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("test database unreachable: %v", err)
	}
	// Recreate the schema so test runs track the current table definitions.
	e := &env{t: t, pool: pool, handler: New(NewStore(pool))}
	e.exec(`DROP TABLE IF EXISTS idempotency_records, reservations, quota_pools CASCADE`)
	if err := EnsureSchema(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("ensure schema: %v", err)
	}
	t.Cleanup(pool.Close)
	return e
}

func (e *env) exec(sql string, args ...any) {
	if _, err := e.pool.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("exec %q: %v", sql, err)
	}
}

func (e *env) queryInt(sql string, args ...any) int {
	var n int
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

func (e *env) unique(base string) string {
	e.seq++
	return fmt.Sprintf("%s-%d", base, e.seq)
}

// do issues a request through the full HTTP stack.
func (e *env) do(method, target, key, body string) *httptest.ResponseRecorder {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, r)
	return rec
}

func (e *env) createPool(tenant, pool string, limit int, from, until time.Time) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`{"limit":%d,"validFrom":%q,"validUntil":%q}`,
		limit, from.Format(time.RFC3339), until.Format(time.RFC3339))
	return e.do(http.MethodPut, "/v1/tenants/"+tenant+"/quota-pools/"+pool, "", body)
}

func (e *env) createReservationRaw(tenant, pool, key, body string) *httptest.ResponseRecorder {
	return e.do(http.MethodPost, "/v1/tenants/"+tenant+"/quota-pools/"+pool+"/reservations", key, body)
}

func (e *env) createReservation(tenant, pool, reservation, key string, amount int, expiresAt time.Time) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`{"reservationId":%q,"amount":%d,"expiresAt":%q}`,
		reservation, amount, expiresAt.Format(time.RFC3339Nano))
	return e.createReservationRaw(tenant, pool, key, body)
}

func TestIntegrationPoolLifecycle(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	rec := e.createPool("tA", "pA", 10, now.Add(-time.Hour), now.Add(time.Hour))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created poolResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Limit != 10 || created.TenantID != "tA" {
		t.Fatalf("unexpected payload %+v", created)
	}

	rec = e.createPool("tA", "pA", 20, now.Add(-time.Hour), now.Add(time.Hour))
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate pool status=%d", rec.Code)
	}
}

func TestIntegrationReservationHappyPathAndReplay(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("t1", "p1", 10, now.Add(-time.Hour), now.Add(2*time.Hour))

	expiry := now.Add(time.Hour)
	body := fmt.Sprintf(`{"reservationId":"r1","amount":4,"expiresAt":%q}`, expiry.Format(time.RFC3339Nano))

	first := e.createReservationRaw("t1", "p1", "key-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	// Identical replay: 200 with the original result.
	replay := e.createReservationRaw("t1", "p1", "key-1", body)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status=%d", replay.Code)
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body %s != original %s", replay.Body.String(), first.Body.String())
	}
	if e.queryInt(`SELECT count(*) FROM reservations WHERE tenant_id='t1' AND pool_id='p1'`) != 1 {
		t.Fatal("replay must not create a second reservation")
	}

	// Same key, different content: 409.
	different := fmt.Sprintf(`{"reservationId":"r1","amount":5,"expiresAt":%q}`, expiry.Format(time.RFC3339Nano))
	if rec := e.createReservationRaw("t1", "p1", "key-1", different); rec.Code != http.StatusConflict {
		t.Fatalf("mismatch status=%d", rec.Code)
	}

	// Same key under a different tenant is independent.
	e.createPool("t2", "p1", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	other := e.createReservationRaw("t2", "p1", "key-1", body)
	if other.Code != http.StatusCreated {
		t.Fatalf("cross-tenant same key status=%d body=%s", other.Code, other.Body.String())
	}
}

func TestIntegrationQuotaEnforcedAndConcurrentSafe(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tc", "pc", 100, now.Add(-time.Hour), now.Add(2*time.Hour))

	var wg sync.WaitGroup
	statuses := make(chan int, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"reservationId":"r%02d","amount":10,"expiresAt":%q}`,
				i, now.Add(time.Hour).Format(time.RFC3339Nano))
			rec := e.createReservationRaw("tc", "pc", fmt.Sprintf("key-%02d", i), body)
			statuses <- rec.Code
		}(i)
	}
	wg.Wait()
	close(statuses)

	created, conflicts := 0, 0
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
	if created != 10 || conflicts != 20 {
		t.Fatalf("created=%d conflicts=%d", created, conflicts)
	}
	reserved := e.queryInt(`
		SELECT COALESCE(SUM(amount),0) FROM reservations
		WHERE tenant_id='tc' AND pool_id='pc' AND status IN ('pending','confirmed')`)
	if reserved != 100 {
		t.Fatalf("reserved=%d, quota must never be exceeded", reserved)
	}

	// One more request over the remaining zero balance fails.
	rec := e.createReservation("tc", "pc", "rover", "k-over", 1, now.Add(time.Hour))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "insufficient") {
		t.Fatalf("over-limit status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestIntegrationReplayOfBusinessConflictReturns200(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tr", "pr", 5, now.Add(-time.Hour), now.Add(2*time.Hour))

	// Occupy the whole pool.
	full := e.createReservation("tr", "pr", "full", "kfull", 5, now.Add(time.Hour))
	if full.Code != http.StatusCreated {
		t.Fatalf("setup status=%d %s", full.Code, full.Body.String())
	}
	body := fmt.Sprintf(`{"reservationId":"r2","amount":1,"expiresAt":%q}`, now.Add(time.Hour).Format(time.RFC3339Nano))
	first := e.createReservationRaw("tr", "pr", "kfail", body)
	if first.Code != http.StatusConflict {
		t.Fatalf("first status=%d", first.Code)
	}

	// Replay: 200 carrying the original result, even though quota later frees
	// up (expire the holder); the stored outcome must not be re-evaluated.
	e.exec(`UPDATE reservations SET status='expired' WHERE reservation_id='full'`)
	replay := e.createReservationRaw("tr", "pr", "kfail", body)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status=%d", replay.Code)
	}
	if !strings.Contains(replay.Body.String(), "insufficient") {
		t.Fatalf("replay must carry original result, got %s", replay.Body.String())
	}
	if n := e.queryInt(`SELECT count(*) FROM reservations WHERE reservation_id='r2'`); n != 0 {
		t.Fatal("replay must not create the reservation")
	}
}

func TestIntegrationDecisionReplay(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tq", "pq", 5, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.createReservation("tq", "pq", "r1", "kc", 2, now.Add(time.Hour))

	first := e.do(http.MethodPost, "/v1/tenants/tq/quota-pools/pq/reservations/r1/confirm", "k1", "")
	if first.Code != http.StatusOK {
		t.Fatalf("confirm status=%d", first.Code)
	}
	replay := e.do(http.MethodPost, "/v1/tenants/tq/quota-pools/pq/reservations/r1/confirm", "k1", "")
	if replay.Code != http.StatusOK || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay status=%d body=%s original=%s", replay.Code, replay.Body.String(), first.Body.String())
	}

	// Same key reused for a different request content is a conflict.
	other := e.do(http.MethodPost, "/v1/tenants/tq/quota-pools/pq/reservations/r1/release", "k1", "")
	if other.Code != http.StatusConflict {
		t.Fatalf("cross-operation key reuse status=%d", other.Code)
	}
}

func TestIntegrationPoolValidityAndExpiryWindow(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()

	// Future pool.
	e.createPool("tf", "future", 10, now.Add(time.Hour), now.Add(2*time.Hour))
	if rec := e.createReservation("tf", "future", "r1", "k1", 1, now.Add(90*time.Minute)); rec.Code != http.StatusConflict {
		t.Fatalf("future pool status=%d", rec.Code)
	}

	// Expired pool.
	e.createPool("tp", "past", 10, now.Add(-2*time.Hour), now.Add(-time.Hour))
	if rec := e.createReservation("tp", "past", "r1", "k1", 1, now.Add(-30*time.Minute)); rec.Code != http.StatusConflict {
		t.Fatalf("past pool status=%d", rec.Code)
	}

	// Valid pool but expiresAt beyond pool end, and expiresAt in the past.
	e.createPool("tv", "ok", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	if rec := e.createReservation("tv", "ok", "r1", "k1", 1, now.Add(3*time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("expiry beyond pool status=%d", rec.Code)
	}
	if rec := e.createReservation("tv", "ok", "r2", "k2", 1, now.Add(-time.Minute)); rec.Code != http.StatusConflict {
		t.Fatalf("expiry in past status=%d", rec.Code)
	}
	if rec := e.createReservation("tv", "ok", "r3", "k3", 1, now.Add(time.Hour)); rec.Code != http.StatusCreated {
		t.Fatalf("valid reservation status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestIntegrationReservationExpiryReleasesQuota(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("te", "pe", 10, now.Add(-time.Hour), now.Add(2*time.Hour))

	deadline := time.Now().Add(1100 * time.Millisecond)
	rec := e.createReservation("te", "pe", "r1", "k1", 10, deadline)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	// While pending, quota is exhausted.
	if rec := e.createReservation("te", "pe", "r2", "k2", 1, now.Add(time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("expected exhaustion, status=%d", rec.Code)
	}

	time.Sleep(1300 * time.Millisecond)

	// GET reflects lazy expiry.
	get := e.do(http.MethodGet, "/v1/tenants/te/quota-pools/pe/reservations/r1", "", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"status":"expired"`) {
		t.Fatalf("get after expiry status=%d body=%s", get.Code, get.Body.String())
	}
	// Freed quota is bookable again.
	rec = e.createReservation("te", "pe", "r3", "k3", 10, now.Add(time.Hour))
	if rec.Code != http.StatusCreated {
		t.Fatalf("rebook after expiry status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestIntegrationConfirmAndRelease(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("td", "pd", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.createReservation("td", "pd", "rc", "kc", 4, now.Add(time.Hour))
	e.createReservation("td", "pd", "rr", "kr", 3, now.Add(time.Hour))

	confirm := e.do(http.MethodPost, "/v1/tenants/td/quota-pools/pd/reservations/rc/confirm", "ck", "")
	if confirm.Code != http.StatusOK || !strings.Contains(confirm.Body.String(), `"status":"confirmed"`) {
		t.Fatalf("confirm status=%d body=%s", confirm.Code, confirm.Body.String())
	}
	if rec := e.do(http.MethodPost, "/v1/tenants/td/quota-pools/pd/reservations/rc/confirm", "ck2", ""); rec.Code != http.StatusConflict {
		t.Fatalf("second confirm status=%d", rec.Code)
	}

	release := e.do(http.MethodPost, "/v1/tenants/td/quota-pools/pd/reservations/rr/release", "rk", "")
	if release.Code != http.StatusOK || !strings.Contains(release.Body.String(), `"status":"released"`) {
		t.Fatalf("release status=%d body=%s", release.Code, release.Body.String())
	}
	if rec := e.do(http.MethodPost, "/v1/tenants/td/quota-pools/pd/reservations/rr/release", "rk2", ""); rec.Code != http.StatusConflict {
		t.Fatalf("second release status=%d", rec.Code)
	}

	// Released quota becomes available (4 confirmed + 0 pending of a limit 10).
	if rec := e.createReservation("td", "pd", "rnew", "knew", 6, now.Add(time.Hour)); rec.Code != http.StatusCreated {
		t.Fatalf("booking freed quota status=%d body=%s", rec.Code, rec.Body.String())
	}
	// Confirmed quota stays consumed (4+6=10).
	if rec := e.createReservation("td", "pd", "ragain", "kagain", 1, now.Add(time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("confirmed quota must stay consumed, status=%d", rec.Code)
	}

	// Confirm/release on a missing reservation is a conflict.
	if rec := e.do(http.MethodPost, "/v1/tenants/td/quota-pools/pd/reservations/nope/confirm", "kx", ""); rec.Code != http.StatusConflict {
		t.Fatalf("missing decision status=%d", rec.Code)
	}
}

func TestIntegrationConfirmReleaseConcurrentAtMostOne(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tx", "px", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.createReservation("tx", "px", "r1", "create-key", 5, now.Add(time.Hour))

	var wg sync.WaitGroup
	results := make(chan int, 2)
	for _, action := range []string{"confirm", "release"} {
		wg.Add(1)
		go func(action string) {
			defer wg.Done()
			key := "key-" + action
			rec := e.do(http.MethodPost, "/v1/tenants/tx/quota-pools/px/reservations/r1/"+action, key, "")
			results <- rec.Code
		}(action)
	}
	wg.Wait()
	close(results)

	ok, conflict := 0, 0
	for s := range results {
		if s == http.StatusOK {
			ok++
		} else if s == http.StatusConflict {
			conflict++
		} else {
			t.Fatalf("unexpected status %d", s)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Fatalf("exactly one decision must succeed, got ok=%d conflict=%d", ok, conflict)
	}
	status := e.queryIntRowStatus("tx", "px", "r1")
	if status != 1 {
		t.Fatal("reservation should hold exactly one terminal decision")
	}
}

func (e *env) queryIntRowStatus(tenant, pool, reservation string) int {
	var status string
	if err := e.pool.QueryRow(context.Background(),
		`SELECT status FROM reservations WHERE tenant_id=$1 AND pool_id=$2 AND reservation_id=$3`,
		tenant, pool, reservation).Scan(&status); err != nil {
		e.t.Fatal(err)
	}
	switch status {
	case "confirmed", "released":
		return 1
	}
	return 0
}

func TestIntegrationConcurrentSameKeyCreatesOnce(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tk", "pk", 5, now.Add(-time.Hour), now.Add(2*time.Hour))

	body := fmt.Sprintf(`{"reservationId":"r1","amount":5,"expiresAt":%q}`, now.Add(time.Hour).Format(time.RFC3339Nano))
	var wg sync.WaitGroup
	statuses := make(chan int, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses <- e.createReservationRaw("tk", "pk", "shared-key", body).Code
		}()
	}
	wg.Wait()
	close(statuses)

	var created, replays int
	for s := range statuses {
		switch s {
		case http.StatusCreated:
			created++
		case http.StatusOK:
			replays++
		default:
			t.Fatalf("unexpected status %d", s)
		}
	}
	if created != 1 || replays != 11 {
		t.Fatalf("created=%d replays=%d", created, replays)
	}
	if n := e.queryInt(`SELECT count(*) FROM reservations WHERE tenant_id='tk'`); n != 1 {
		t.Fatalf("expected exactly one reservation, got %d", n)
	}
}

func TestIntegrationGetMissingAndCrossTenant(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tg", "pg", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.createReservation("tg", "pg", "r1", "k1", 1, now.Add(time.Hour))

	for _, tenant := range []string{"tg", "other"} {
		rec := e.do(http.MethodGet, "/v1/tenants/"+tenant+"/quota-pools/pg/reservations/r1", "", "")
		if tenant == "tg" {
			if rec.Code != http.StatusOK {
				t.Fatalf("owner get status=%d", rec.Code)
			}
		} else if rec.Code != http.StatusNotFound {
			t.Fatalf("cross-tenant get status=%d", rec.Code)
		}
	}
	if rec := e.do(http.MethodGet, "/v1/tenants/tg/quota-pools/pg/reservations/ghost", "", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("missing get status=%d", rec.Code)
	}
}

func TestIntegrationBadRequestsWriteNothing(t *testing.T) {
	e := newEnv(t)
	target := "/v1/tenants/tb/quota-pools/pb/reservations"

	// Missing Idempotency-Key.
	rec := e.do(http.MethodPost, target, "", `{"reservationId":"r1","amount":1,"expiresAt":"2027-01-01T00:00:00Z"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing key status=%d", rec.Code)
	}
	// Malformed JSON with a key present.
	rec = e.do(http.MethodPost, target, "k1", `{bad json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed status=%d", rec.Code)
	}
	if n := e.queryInt(`SELECT count(*) FROM idempotency_records`); n != 0 {
		t.Fatalf("invalid requests wrote %d idempotency rows", n)
	}
	if n := e.queryInt(`SELECT count(*) FROM reservations`); n != 0 {
		t.Fatalf("invalid requests wrote %d reservations", n)
	}

	// Invalid pool payload never inserts a pool.
	bad := e.do(http.MethodPut, "/v1/tenants/tb/quota-pools/pb", "",
		`{"limit":0,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2025-01-01T00:00:00Z"}`)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("bad pool status=%d", bad.Code)
	}
	if n := e.queryInt(`SELECT count(*) FROM quota_pools`); n != 0 {
		t.Fatalf("invalid pool wrote %d rows", n)
	}
}

func TestIntegrationStorageFailureIsGeneric503(t *testing.T) {
	// Port 1 refuses connections immediately; responses must stay generic.
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/entitlements_test?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Skip(err)
	}
	defer pool.Close()
	h := New(NewStore(pool))

	r := httptest.NewRequest(http.MethodPut, "/v1/tenants/t/quota-pools/p",
		strings.NewReader(`{"limit":1,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2027-01-01T00:00:00Z"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(strings.ToLower(body), "connect") || strings.Contains(body, "127.0.0.1") {
		t.Fatalf("internal detail leaked: %s", body)
	}
}

func TestIntegrationDataSurvivesPoolRestart(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("ts", "ps", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.createReservation("ts", "ps", "r1", "k1", 3, now.Add(time.Hour))

	// Reopen the connection pool as a freshly started process would.
	url := testDatabaseURL()
	e.pool.Close()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	e.pool = pool
	e.handler = New(NewStore(pool))

	rec := e.do(http.MethodGet, "/v1/tenants/ts/quota-pools/ps/reservations/r1", "", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"amount":3`) {
		t.Fatalf("data did not survive pool restart: %d %s", rec.Code, rec.Body.String())
	}
	// A stored idempotency record still replays after restart.
	body := fmt.Sprintf(`{"reservationId":"r1","amount":3,"expiresAt":%q}`, now.Add(time.Hour).Format(time.RFC3339Nano))
	rec = e.createReservationRaw("ts", "ps", "k1", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("idempotency replay after restart status=%d body=%s", rec.Code, rec.Body.String())
	}
}
