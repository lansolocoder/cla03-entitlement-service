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

func (e *env) setAllocationRaw(tenant, pool, team, key, body string) *httptest.ResponseRecorder {
	return e.do(http.MethodPost, "/v1/tenants/"+tenant+"/quota-pools/"+pool+"/teams/"+team+"/allocation", key, body)
}

func (e *env) setAllocation(tenant, pool, team, key string, amount, expectedVersion int64) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`{"amount":%d,"expectedVersion":%d}`, amount, expectedVersion)
	return e.setAllocationRaw(tenant, pool, team, key, body)
}

func (e *env) getAllocation(tenant, pool, team string) *httptest.ResponseRecorder {
	return e.do(http.MethodGet, "/v1/tenants/"+tenant+"/quota-pools/"+pool+"/teams/"+team+"/allocation", "", "")
}

func TestIntegrationAllocationLifecycle(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("ta", "pa", 10, now.Add(-time.Hour), now.Add(2*time.Hour))

	// Nothing set yet: 404, also cross-tenant.
	if rec := e.getAllocation("ta", "pa", "team1"); rec.Code != http.StatusNotFound {
		t.Fatalf("unset allocation status=%d", rec.Code)
	}
	if rec := e.getAllocation("other", "pa", "team1"); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status=%d", rec.Code)
	}
	// Allocation on a missing pool is a conflict.
	if rec := e.setAllocation("ta", "ghost", "team1", "ka0", 5, 0); rec.Code != http.StatusConflict {
		t.Fatalf("missing pool status=%d", rec.Code)
	}

	// Initial allocation: version 0 -> 1.
	rec := e.setAllocation("ta", "pa", "team1", "ka1", 6, 0)
	if rec.Code != http.StatusOK {
		t.Fatalf("set status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{`"teamId":"team1"`, `"allocated":6`, `"used":0`, `"available":6`, `"version":1`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("response missing %s: %s", want, rec.Body.String())
		}
	}
	get := e.getAllocation("ta", "pa", "team1")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"allocated":6`) || !strings.Contains(get.Body.String(), `"version":1`) {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}

	// Stale version is a conflict and changes nothing.
	if rec := e.setAllocation("ta", "pa", "team1", "ka2", 2, 0); rec.Code != http.StatusConflict {
		t.Fatalf("stale version status=%d", rec.Code)
	}
	if get := e.getAllocation("ta", "pa", "team1"); !strings.Contains(get.Body.String(), `"allocated":6`) {
		t.Fatalf("state changed after conflict: %s", get.Body.String())
	}

	// Matching version updates and increments.
	rec = e.setAllocation("ta", "pa", "team1", "ka3", 4, 1)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"allocated":4`) || !strings.Contains(rec.Body.String(), `"version":2`) {
		t.Fatalf("update status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Idempotent replay returns the first result; different content conflicts.
	replay := e.setAllocation("ta", "pa", "team1", "ka3", 4, 1)
	if replay.Code != http.StatusOK || replay.Body.String() != rec.Body.String() {
		t.Fatalf("replay status=%d body=%s original=%s", replay.Code, replay.Body.String(), rec.Body.String())
	}
	if rec := e.setAllocation("ta", "pa", "team1", "ka3", 5, 1); rec.Code != http.StatusConflict {
		t.Fatalf("key reuse with different content status=%d", rec.Code)
	}
}

func TestIntegrationAllocationReplayOfConflictReturns200(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tr", "pr", 10, now.Add(-time.Hour), now.Add(2*time.Hour))

	// Version mismatch is decided before business validation and stored.
	first := e.setAllocation("tr", "pr", "team1", "kf", 5, 3)
	if first.Code != http.StatusConflict {
		t.Fatalf("first status=%d", first.Code)
	}
	// Move the version forward with another key.
	if rec := e.setAllocation("tr", "pr", "team1", "kok", 5, 0); rec.Code != http.StatusOK {
		t.Fatalf("setup status=%d", rec.Code)
	}
	// Replaying the failed request returns 200 with the original outcome; the
	// stored result is not re-evaluated against the new version.
	replay := e.setAllocation("tr", "pr", "team1", "kf", 5, 3)
	if replay.Code != http.StatusOK || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay status=%d body=%s original=%s", replay.Code, replay.Body.String(), first.Body.String())
	}
	if get := e.getAllocation("tr", "pr", "team1"); !strings.Contains(get.Body.String(), `"version":1`) {
		t.Fatalf("replay must not change state: %s", get.Body.String())
	}
}

func TestIntegrationAllocationSumLimitedByPool(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tl", "pl", 10, now.Add(-time.Hour), now.Add(2*time.Hour))

	if rec := e.setAllocation("tl", "pl", "teamA", "k1", 6, 0); rec.Code != http.StatusOK {
		t.Fatalf("teamA status=%d", rec.Code)
	}
	if rec := e.setAllocation("tl", "pl", "teamB", "k2", 5, 0); rec.Code != http.StatusConflict {
		t.Fatalf("teamB over-sum status=%d", rec.Code)
	}
	if rec := e.setAllocation("tl", "pl", "teamB", "k3", 4, 0); rec.Code != http.StatusOK {
		t.Fatalf("teamB status=%d body=%s", rec.Code, rec.Body.String())
	}
	// Raising teamA past the remaining headroom conflicts; lowering is fine.
	if rec := e.setAllocation("tl", "pl", "teamA", "k4", 7, 1); rec.Code != http.StatusConflict {
		t.Fatalf("teamA raise status=%d", rec.Code)
	}
	if rec := e.setAllocation("tl", "pl", "teamA", "k5", 6, 1); rec.Code != http.StatusOK {
		t.Fatalf("teamA same-sum status=%d", rec.Code)
	}
}

func TestIntegrationAllocationNotBelowTeamUsage(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tu", "pu", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	if rec := e.setAllocation("tu", "pu", "team1", "ka", 8, 0); rec.Code != http.StatusOK {
		t.Fatalf("alloc status=%d", rec.Code)
	}

	body := fmt.Sprintf(`{"reservationId":"r1","teamId":"team1","amount":5,"expiresAt":%q}`,
		now.Add(time.Hour).Format(time.RFC3339Nano))
	if rec := e.createReservationRaw("tu", "pu", "kr1", body); rec.Code != http.StatusCreated {
		t.Fatalf("reserve status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Lowering below the team's active usage conflicts and changes nothing.
	if rec := e.setAllocation("tu", "pu", "team1", "kb", 4, 1); rec.Code != http.StatusConflict {
		t.Fatalf("lower below usage status=%d", rec.Code)
	}
	get := e.getAllocation("tu", "pu", "team1")
	for _, want := range []string{`"allocated":8`, `"used":5`, `"available":3`, `"version":1`} {
		if !strings.Contains(get.Body.String(), want) {
			t.Fatalf("get missing %s: %s", want, get.Body.String())
		}
	}

	// Lowering exactly to the usage is allowed and leaves no headroom.
	if rec := e.setAllocation("tu", "pu", "team1", "kc", 5, 1); rec.Code != http.StatusOK {
		t.Fatalf("lower to usage status=%d", rec.Code)
	}
	body2 := fmt.Sprintf(`{"reservationId":"r2","teamId":"team1","amount":1,"expiresAt":%q}`,
		now.Add(time.Hour).Format(time.RFC3339Nano))
	if rec := e.createReservationRaw("tu", "pu", "kr2", body2); rec.Code != http.StatusConflict {
		t.Fatalf("team headroom status=%d", rec.Code)
	}

	// Releasing the reservation restores the team balance.
	if rec := e.do(http.MethodPost, "/v1/tenants/tu/quota-pools/pu/reservations/r1/release", "krel", ""); rec.Code != http.StatusOK {
		t.Fatalf("release status=%d", rec.Code)
	}
	if get := e.getAllocation("tu", "pu", "team1"); !strings.Contains(get.Body.String(), `"available":5`) {
		t.Fatalf("balance not restored: %s", get.Body.String())
	}
}

func TestIntegrationTeamReservation(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tt", "pt", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	expiry := now.Add(time.Hour).Format(time.RFC3339Nano)

	// Unknown team is a conflict.
	body := fmt.Sprintf(`{"reservationId":"r0","teamId":"ghost","amount":1,"expiresAt":%q}`, expiry)
	if rec := e.createReservationRaw("tt", "pt", "k0", body); rec.Code != http.StatusConflict {
		t.Fatalf("unknown team status=%d", rec.Code)
	}

	if rec := e.setAllocation("tt", "pt", "team1", "ka", 4, 0); rec.Code != http.StatusOK {
		t.Fatalf("alloc status=%d", rec.Code)
	}

	// Team reservation carries teamId in the create response and the record.
	body = fmt.Sprintf(`{"reservationId":"r1","teamId":"team1","amount":4,"expiresAt":%q}`, expiry)
	rec := e.createReservationRaw("tt", "pt", "k1", body)
	if rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), `"teamId":"team1"`) {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	get := e.do(http.MethodGet, "/v1/tenants/tt/quota-pools/pt/reservations/r1", "", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"teamId":"team1"`) {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}

	// Team balance exhausted even though the pool has headroom.
	body = fmt.Sprintf(`{"reservationId":"r2","teamId":"team1","amount":1,"expiresAt":%q}`, expiry)
	if rec := e.createReservationRaw("tt", "pt", "k2", body); rec.Code != http.StatusConflict {
		t.Fatalf("team exhaustion status=%d", rec.Code)
	}

	// Unteammed reservations still draw on the pool and omit teamId.
	body = fmt.Sprintf(`{"reservationId":"r3","amount":6,"expiresAt":%q}`, expiry)
	rec = e.createReservationRaw("tt", "pt", "k3", body)
	if rec.Code != http.StatusCreated || strings.Contains(rec.Body.String(), "teamId") {
		t.Fatalf("unteammed create status=%d body=%s", rec.Code, rec.Body.String())
	}
	// Pool is now full (4 team + 6 plain); one more of any kind conflicts.
	body = fmt.Sprintf(`{"reservationId":"r4","amount":1,"expiresAt":%q}`, expiry)
	if rec := e.createReservationRaw("tt", "pt", "k4", body); rec.Code != http.StatusConflict {
		t.Fatalf("pool exhaustion status=%d", rec.Code)
	}

	// Confirming keeps the team balance occupied.
	if rec := e.do(http.MethodPost, "/v1/tenants/tt/quota-pools/pt/reservations/r1/confirm", "kc1", ""); rec.Code != http.StatusOK {
		t.Fatalf("confirm status=%d", rec.Code)
	}
	if rec := e.setAllocation("tt", "pt", "team1", "kb", 3, 1); rec.Code != http.StatusConflict {
		t.Fatalf("lower below confirmed usage status=%d", rec.Code)
	}
	if get := e.getAllocation("tt", "pt", "team1"); !strings.Contains(get.Body.String(), `"used":4`) {
		t.Fatalf("confirmed usage: %s", get.Body.String())
	}
}

func TestIntegrationTeamReservationExpiryRestoresBalance(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tx", "px", 10, now.Add(-time.Hour), now.Add(2*time.Hour))
	if rec := e.setAllocation("tx", "px", "team1", "ka", 5, 0); rec.Code != http.StatusOK {
		t.Fatalf("alloc status=%d", rec.Code)
	}

	deadline := time.Now().Add(1100 * time.Millisecond)
	body := fmt.Sprintf(`{"reservationId":"r1","teamId":"team1","amount":5,"expiresAt":%q}`,
		deadline.Format(time.RFC3339Nano))
	if rec := e.createReservationRaw("tx", "px", "k1", body); rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	if get := e.getAllocation("tx", "px", "team1"); !strings.Contains(get.Body.String(), `"available":0`) {
		t.Fatalf("pending must occupy team balance: %s", get.Body.String())
	}

	time.Sleep(1300 * time.Millisecond)

	// Expiry releases the team balance; lowering to zero then succeeds.
	if get := e.getAllocation("tx", "px", "team1"); !strings.Contains(get.Body.String(), `"available":5`) {
		t.Fatalf("expiry must restore team balance: %s", get.Body.String())
	}
	if rec := e.setAllocation("tx", "px", "team1", "kb", 0, 1); rec.Code != http.StatusOK {
		t.Fatalf("lower after expiry status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestIntegrationConcurrentTeamLayersNeverOversubscribe(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	e.createPool("tz", "pz", 100, now.Add(-time.Hour), now.Add(2*time.Hour))
	if rec := e.setAllocation("tz", "pz", "team1", "ka", 50, 0); rec.Code != http.StatusOK {
		t.Fatalf("alloc status=%d", rec.Code)
	}

	// Twenty concurrent team reservations of 10 against a 50 allocation.
	var wg sync.WaitGroup
	statuses := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"reservationId":"r%02d","teamId":"team1","amount":10,"expiresAt":%q}`,
				i, now.Add(time.Hour).Format(time.RFC3339Nano))
			statuses <- e.createReservationRaw("tz", "pz", fmt.Sprintf("key-%02d", i), body).Code
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
		t.Fatalf("created=%d, team allocation must cap at 5", created)
	}
	if get := e.getAllocation("tz", "pz", "team1"); !strings.Contains(get.Body.String(), `"used":50`) {
		t.Fatalf("team usage must never exceed the allocation: %s", get.Body.String())
	}

	// Concurrent allocations that together exceed the pool limit: one wins.
	// team1 already holds 50 of the 100 limit, so only one 50 fits.
	results := make(chan int, 2)
	for i, team := range []string{"teamA", "teamB"} {
		wg.Add(1)
		go func(i int, team string) {
			defer wg.Done()
			results <- e.setAllocation("tz", "pz", team, fmt.Sprintf("kc-%d", i), 50, 0).Code
		}(i, team)
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
		t.Fatalf("allocations must serialize around the pool limit, ok=%d conflict=%d", ok, conflict)
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
	if rec := e.setAllocation("ts", "ps", "team1", "ka1", 4, 0); rec.Code != http.StatusOK {
		t.Fatalf("alloc status=%d", rec.Code)
	}

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
	// Allocations and their idempotency records survive as well.
	rec = e.getAllocation("ts", "ps", "team1")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"allocated":4`) {
		t.Fatalf("allocation did not survive restart: %d %s", rec.Code, rec.Body.String())
	}
	if rec := e.setAllocation("ts", "ps", "team1", "ka1", 4, 0); rec.Code != http.StatusOK {
		t.Fatalf("allocation replay after restart status=%d body=%s", rec.Code, rec.Body.String())
	}
}
