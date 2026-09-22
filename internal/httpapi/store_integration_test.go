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
	e.exec(`DROP TABLE IF EXISTS idempotency_records, reservations, team_allocations, quota_pools CASCADE`)
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

func (e *env) createTeamReservation(tenant, pool, reservation, team, key string, amount int, expiresAt time.Time) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`{"reservationId":%q,"amount":%d,"expiresAt":%q,"teamId":%q}`,
		reservation, amount, expiresAt.Format(time.RFC3339Nano), team)
	return e.createReservationRaw(tenant, pool, key, body)
}

func (e *env) setAllocationRaw(tenant, pool, team, key, body string) *httptest.ResponseRecorder {
	return e.do(http.MethodPost, "/v1/tenants/"+tenant+"/quota-pools/"+pool+"/teams/"+team+"/allocation", key, body)
}

func (e *env) setAllocation(tenant, pool, team, key string, amount, expectedVersion int) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`{"amount":%d,"expectedVersion":%d}`, amount, expectedVersion)
	return e.setAllocationRaw(tenant, pool, team, key, body)
}

func (e *env) getAllocation(tenant, pool, team string) *httptest.ResponseRecorder {
	return e.do(http.MethodGet, "/v1/tenants/"+tenant+"/quota-pools/"+pool+"/teams/"+team+"/allocation", "", "")
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

func TestIntegrationTeamAllocationLowerBound(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tl", "pl", 100, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.setAllocation("tl", "pl", "team-1", "k-a", 50, 0)
	e.createTeamReservation("tl", "pl", "r1", "team-1", "k-r1", 30, now.Add(time.Hour))
	e.createTeamReservation("tl", "pl", "r2", "team-1", "k-r2", 10, now.Add(time.Hour))
	e.do(http.MethodPost, "/v1/tenants/tl/quota-pools/pl/reservations/r2/confirm", "k-c2", "")

	// Occupied: 30 pending + 10 confirmed = 40. Lowering below that conflicts.
	if rec := e.setAllocation("tl", "pl", "team-1", "k-low", 39, 1); rec.Code != http.StatusConflict {
		t.Fatalf("lower below occupied status=%d", rec.Code)
	}
	if rec := e.getAllocation("tl", "pl", "team-1"); !strings.Contains(rec.Body.String(), `"allocated":50`) {
		t.Fatalf("rejected lowering must not apply: %s", rec.Body.String())
	}
	// Lowering to exactly the occupied amount is allowed.
	rec := e.setAllocation("tl", "pl", "team-1", "k-eq", 40, 1)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"used":40`) ||
		!strings.Contains(rec.Body.String(), `"available":0`) {
		t.Fatalf("lower to occupied: %d %s", rec.Code, rec.Body.String())
	}
	// Releasing the pending reservation frees team quota for a deeper cut.
	e.do(http.MethodPost, "/v1/tenants/tl/quota-pools/pl/reservations/r1/release", "k-rel", "")
	rec = e.setAllocation("tl", "pl", "team-1", "k-deep", 10, 2)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"used":10`) {
		t.Fatalf("lower after release: %d %s", rec.Code, rec.Body.String())
	}
}

func TestIntegrationTeamReservationFlow(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tt", "pt", 100, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.setAllocation("tt", "pt", "team-1", "k-a", 40, 0)

	// Unknown team cannot reserve.
	if rec := e.createTeamReservation("tt", "pt", "rx", "ghost", "k-x", 1, now.Add(time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("unknown team status=%d", rec.Code)
	}

	created := e.createTeamReservation("tt", "pt", "r1", "team-1", "k-r1", 30, now.Add(time.Hour))
	if created.Code != http.StatusCreated || !strings.Contains(created.Body.String(), `"teamId":"team-1"`) {
		t.Fatalf("team reservation status=%d body=%s", created.Code, created.Body.String())
	}
	get := e.do(http.MethodGet, "/v1/tenants/tt/quota-pools/pt/reservations/r1", "", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"teamId":"team-1"`) {
		t.Fatalf("get must include teamId: %d %s", get.Code, get.Body.String())
	}
	// Team balance is exhausted even though the pool still has room.
	if rec := e.createTeamReservation("tt", "pt", "r2", "team-1", "k-r2", 11, now.Add(time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("over team balance status=%d", rec.Code)
	}
	// Teamless reservations are unaffected by the team's balance.
	if rec := e.createReservation("tt", "pt", "r3", "k-r3", 60, now.Add(time.Hour)); rec.Code != http.StatusCreated {
		t.Fatalf("teamless reservation status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := e.do(http.MethodGet, "/v1/tenants/tt/quota-pools/pt/reservations/r3", "", ""); strings.Contains(rec.Body.String(), "teamId") {
		t.Fatalf("teamless reservation must not carry teamId: %s", rec.Body.String())
	}
	// The pool level still caps the total: 30 + 60 used of 100.
	if rec := e.createReservation("tt", "pt", "r4", "k-r4", 11, now.Add(time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("over pool limit status=%d", rec.Code)
	}

	// Releasing the team reservation restores the team's balance.
	e.do(http.MethodPost, "/v1/tenants/tt/quota-pools/pt/reservations/r1/release", "k-rel", "")
	if rec := e.getAllocation("tt", "pt", "team-1"); !strings.Contains(rec.Body.String(), `"used":0`) {
		t.Fatalf("release must free team quota: %s", rec.Body.String())
	}
	if rec := e.createTeamReservation("tt", "pt", "r5", "team-1", "k-r5", 40, now.Add(time.Hour)); rec.Code != http.StatusCreated {
		t.Fatalf("rebook after release status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Confirming keeps the quota occupied at both levels.
	e.do(http.MethodPost, "/v1/tenants/tt/quota-pools/pt/reservations/r5/confirm", "k-c5", "")
	if rec := e.getAllocation("tt", "pt", "team-1"); !strings.Contains(rec.Body.String(), `"used":40`) {
		t.Fatalf("confirmed quota must stay occupied: %s", rec.Body.String())
	}
	if rec := e.createReservation("tt", "pt", "r6", "k-r6", 31, now.Add(time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("confirmed quota must stay consumed at pool level, status=%d", rec.Code)
	}
}

func TestIntegrationTeamReservationExpiryRestoresBalance(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tx", "px", 100, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.setAllocation("tx", "px", "team-1", "k-a", 10, 0)

	deadline := time.Now().Add(1100 * time.Millisecond)
	if rec := e.createTeamReservation("tx", "px", "r1", "team-1", "k-r1", 10, deadline); rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := e.createTeamReservation("tx", "px", "r2", "team-1", "k-r2", 1, now.Add(time.Hour)); rec.Code != http.StatusConflict {
		t.Fatalf("expected team exhaustion, status=%d", rec.Code)
	}

	time.Sleep(1300 * time.Millisecond)

	// Expired pending quota is bookable again at the team level.
	if rec := e.createTeamReservation("tx", "px", "r3", "team-1", "k-r3", 10, now.Add(time.Hour)); rec.Code != http.StatusCreated {
		t.Fatalf("rebook after expiry status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := e.getAllocation("tx", "px", "team-1"); !strings.Contains(rec.Body.String(), `"used":10`) {
		t.Fatalf("summary must reflect only live reservations: %s", rec.Body.String())
	}
}

func TestIntegrationConcurrentTeamReservationsNeverOversubscribe(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("ty", "py", 100, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.setAllocation("ty", "py", "team-1", "k-a", 50, 0)

	var wg sync.WaitGroup
	statuses := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := e.createTeamReservation("ty", "py", fmt.Sprintf("r%02d", i), "team-1",
				fmt.Sprintf("key-%02d", i), 10, now.Add(time.Hour))
			statuses <- rec.Code
		}(i)
	}
	wg.Wait()
	close(statuses)

	created := 0
	for s := range statuses {
		if s == http.StatusCreated {
			created++
		} else if s != http.StatusConflict {
			t.Fatalf("unexpected status %d", s)
		}
	}
	if created != 5 {
		t.Fatalf("team allocation 50 must admit exactly 5x10, got %d", created)
	}
	used := e.queryInt(`
		SELECT COALESCE(SUM(amount),0) FROM reservations
		WHERE tenant_id='ty' AND pool_id='py' AND team_id='team-1' AND status IN ('pending','confirmed')`)
	if used != 50 {
		t.Fatalf("team used=%d, team allocation must never be exceeded", used)
	}
}

func TestIntegrationConcurrentAllocationAndReservation(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tz", "pz", 100, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.setAllocation("tz", "pz", "team-1", "k-a", 50, 0)
	e.createTeamReservation("tz", "pz", "r1", "team-1", "k-r1", 40, now.Add(time.Hour))

	// Lowering to exactly the occupied amount races with a reservation of the
	// remaining 10: whichever commits first, the team can never exceed 50.
	var wg sync.WaitGroup
	results := make(chan int, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		results <- e.setAllocation("tz", "pz", "team-1", "k-lower", 40, 1).Code
	}()
	go func() {
		defer wg.Done()
		results <- e.createTeamReservation("tz", "pz", "r2", "team-1", "k-r2", 10, now.Add(time.Hour)).Code
	}()
	wg.Wait()
	close(results)
	for s := range results {
		if s != http.StatusOK && s != http.StatusCreated && s != http.StatusConflict {
			t.Fatalf("unexpected status %d", s)
		}
	}
	var allocated, used int
	e.pool.QueryRow(context.Background(),
		`SELECT allocated FROM team_allocations WHERE tenant_id='tz' AND pool_id='pz' AND team_id='team-1'`).Scan(&allocated)
	e.pool.QueryRow(context.Background(), `
		SELECT COALESCE(SUM(amount),0) FROM reservations
		WHERE tenant_id='tz' AND pool_id='pz' AND team_id='team-1' AND status IN ('pending','confirmed')`).Scan(&used)
	if used > allocated {
		t.Fatalf("team used %d exceeds allocation %d", used, allocated)
	}
}

func TestIntegrationDataSurvivesPoolRestart(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("ts", "ps", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	e.createReservation("ts", "ps", "r1", "k1", 3, now.Add(time.Hour))
	e.setAllocation("ts", "ps", "team-1", "ka1", 5, 0)
	e.createTeamReservation("ts", "ps", "r2", "team-1", "k2", 2, now.Add(time.Hour))

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
	// Team allocations and team-scoped reservations survive as well.
	rec = e.getAllocation("ts", "ps", "team-1")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"allocated":5`) ||
		!strings.Contains(rec.Body.String(), `"used":2`) {
		t.Fatalf("allocation did not survive restart: %d %s", rec.Code, rec.Body.String())
	}
	rec = e.do(http.MethodGet, "/v1/tenants/ts/quota-pools/ps/reservations/r2", "", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"teamId":"team-1"`) {
		t.Fatalf("team reservation did not survive restart: %d %s", rec.Code, rec.Body.String())
	}
	// A stored allocation idempotency record still replays after restart.
	rec = e.setAllocation("ts", "ps", "team-1", "ka1", 5, 0)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"version":1`) {
		t.Fatalf("allocation replay after restart status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestIntegrationTeamAllocationLifecycle(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("ta", "pa", 100, now.Add(-time.Hour), now.Add(2*time.Hour))
	target := "/v1/tenants/ta/quota-pools/pa/teams/team-1/allocation"

	// Missing team, missing pool, and cross-tenant reads are all 404.
	if rec := e.getAllocation("ta", "pa", "team-1"); rec.Code != http.StatusNotFound {
		t.Fatalf("missing team status=%d", rec.Code)
	}
	if rec := e.getAllocation("ta", "ghost", "team-1"); rec.Code != http.StatusNotFound {
		t.Fatalf("missing pool status=%d", rec.Code)
	}

	// First write must expect the initial version 0.
	if rec := e.setAllocation("ta", "pa", "team-2", "kbad", 10, 1); rec.Code != http.StatusConflict {
		t.Fatalf("wrong initial version status=%d", rec.Code)
	}
	rec := e.do(http.MethodPost, target, "k1", `{"amount":60,"expectedVersion":0}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("first allocation status=%d body=%s", rec.Code, rec.Body.String())
	}
	var alloc allocationResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &alloc); err != nil {
		t.Fatal(err)
	}
	if alloc.TeamID != "team-1" || alloc.Allocated != 60 || alloc.Used != 0 || alloc.Available != 60 || alloc.Version != 1 {
		t.Fatalf("unexpected allocation %+v", alloc)
	}

	// The allocation is invisible to other tenants.
	if rec := e.getAllocation("other", "pa", "team-1"); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status=%d", rec.Code)
	}

	// GET returns the same summary.
	rec = e.getAllocation("ta", "pa", "team-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status=%d", rec.Code)
	}
	var got allocationResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got != alloc {
		t.Fatalf("get %+v != post %+v", got, alloc)
	}

	// Version mismatch leaves the state unchanged.
	if rec := e.setAllocation("ta", "pa", "team-1", "k2", 10, 0); rec.Code != http.StatusConflict {
		t.Fatalf("stale version status=%d", rec.Code)
	}
	if rec := e.getAllocation("ta", "pa", "team-1"); !strings.Contains(rec.Body.String(), `"allocated":60`) {
		t.Fatalf("stale write must not apply: %s", rec.Body.String())
	}

	// Allocations across teams may not exceed the pool limit.
	if rec := e.setAllocation("ta", "pa", "team-2", "k3", 41, 0); rec.Code != http.StatusConflict {
		t.Fatalf("over-limit status=%d", rec.Code)
	}
	if rec := e.setAllocation("ta", "pa", "team-2", "k4", 40, 0); rec.Code != http.StatusOK {
		t.Fatalf("at-limit status=%d body=%s", rec.Code, rec.Body.String())
	}
	// Raising team-1 so the total exceeds the limit conflicts too.
	if rec := e.setAllocation("ta", "pa", "team-1", "k5", 61, 1); rec.Code != http.StatusConflict {
		t.Fatalf("raise over limit status=%d", rec.Code)
	}

	// Occupied quota bounds how far an allocation can be lowered.
	e.createTeamReservation("ta", "pa", "r1", "team-1", "kr1", 20, now.Add(time.Hour))
	if rec := e.setAllocation("ta", "pa", "team-1", "k6", 19, 1); rec.Code != http.StatusConflict {
		t.Fatalf("lower below occupied status=%d", rec.Code)
	}
	rec = e.setAllocation("ta", "pa", "team-1", "k7", 20, 1)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"used":20`) ||
		!strings.Contains(rec.Body.String(), `"available":0`) || !strings.Contains(rec.Body.String(), `"version":2`) {
		t.Fatalf("lower to occupied: %d %s", rec.Code, rec.Body.String())
	}

	// Allocating into a missing pool is a conflict, not a write.
	if rec := e.setAllocation("ta", "ghost", "team-1", "k8", 1, 0); rec.Code != http.StatusConflict {
		t.Fatalf("missing pool status=%d", rec.Code)
	}
	if n := e.queryInt(`SELECT count(*) FROM team_allocations WHERE pool_id='ghost'`); n != 0 {
		t.Fatal("allocation into a missing pool must not persist")
	}
}

func TestIntegrationAllocationIdempotency(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("ti", "pi", 100, now.Add(-time.Hour), now.Add(2*time.Hour))

	body := `{"amount":30,"expectedVersion":0}`
	first := e.setAllocationRaw("ti", "pi", "team-1", "idem-1", body)
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	// Identical replay: 200 with the original result, version not bumped again.
	replay := e.setAllocationRaw("ti", "pi", "team-1", "idem-1", body)
	if replay.Code != http.StatusOK || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay status=%d body=%s original=%s", replay.Code, replay.Body.String(), first.Body.String())
	}
	if rec := e.getAllocation("ti", "pi", "team-1"); !strings.Contains(rec.Body.String(), `"version":1`) {
		t.Fatalf("replay must not bump the version: %s", rec.Body.String())
	}
	// Same key, different content: 409.
	if rec := e.setAllocationRaw("ti", "pi", "team-1", "idem-1", `{"amount":31,"expectedVersion":0}`); rec.Code != http.StatusConflict {
		t.Fatalf("mismatch status=%d", rec.Code)
	}
	// The key is tenant-scoped: the same key against a different path is a
	// different request and conflicts, even with an identical body.
	if rec := e.setAllocationRaw("ti", "pi", "team-2", "idem-1", body); rec.Code != http.StatusConflict {
		t.Fatalf("cross-team key reuse status=%d", rec.Code)
	}
	// A stored business conflict replays as 200 with the original body.
	conflict := e.setAllocationRaw("ti", "pi", "team-1", "idem-2", `{"amount":10,"expectedVersion":5}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d", conflict.Code)
	}
	replay = e.setAllocationRaw("ti", "pi", "team-1", "idem-2", `{"amount":10,"expectedVersion":5}`)
	if replay.Code != http.StatusOK || replay.Body.String() != conflict.Body.String() {
		t.Fatalf("conflict replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	// Concurrent identical requests apply the allocation exactly once.
	var wg sync.WaitGroup
	statuses := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses <- e.setAllocationRaw("ti", "pi", "team-3", "idem-3", `{"amount":5,"expectedVersion":0}`).Code
		}()
	}
	wg.Wait()
	close(statuses)
	for s := range statuses {
		if s != http.StatusOK {
			t.Fatalf("concurrent status=%d", s)
		}
	}
	if rec := e.getAllocation("ti", "pi", "team-3"); !strings.Contains(rec.Body.String(), `"version":1`) {
		t.Fatalf("concurrent same-key writes must apply once: %s", rec.Body.String())
	}
}
