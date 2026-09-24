package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

func testStore(t *testing.T) (context.Context, *store.Store) {
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
	return ctx, store.New(pool)
}

func futureTime(seconds int) time.Time {
	return time.Now().UTC().Add(time.Duration(seconds) * time.Second).Truncate(time.Microsecond)
}

func createActive(t *testing.T, ctx context.Context, st *store.Store, typ string, total int64, expiresAt time.Time) store.Entitlement {
	t.Helper()
	e, err := st.Create(ctx, "team-1", typ, total, expiresAt)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return e
}

func TestCreateEntitlement(t *testing.T) {
	ctx, st := testStore(t)
	expires := futureTime(3600)

	e, err := st.Create(ctx, "team-a", "seat", 5, expires)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if e.ID == "" || e.TeamID != "team-a" || e.Type != "seat" || e.Total != 5 ||
		e.Status != "active" || e.Used != 0 || e.Version != 1 {
		t.Fatalf("unexpected record: %+v", e)
	}
	if !e.ExpiresAt.Equal(expires) || e.ExpiresAt.Location() != time.UTC {
		t.Fatalf("expiresAt not normalized UTC: %v", e.ExpiresAt)
	}

	// Duplicate (team, type, expiresAt) is rejected with the existing record.
	_, err = st.Create(ctx, "team-a", "seat", 99, expires)
	var conflict *store.ConflictError
	if !errors.As(err, &conflict) || !errors.Is(conflict, store.ErrAlreadyExists) {
		t.Fatalf("expected already exists, got %v", err)
	}
	if conflict.Existing.ID != e.ID || conflict.Existing.Total != 5 {
		t.Fatalf("existing record mismatch: %+v", conflict.Existing)
	}

	// Different type under the same team is a distinct entitlement.
	if _, err := st.Create(ctx, "team-a", "quota", 5, expires); err != nil {
		t.Fatalf("distinct type should create: %v", err)
	}
}

func TestAllocate(t *testing.T) {
	ctx, st := testStore(t)
	e := createActive(t, ctx, st, "seat", 5, futureTime(3600))

	updated, err := st.Allocate(ctx, e.ID, "m1", 3, time.Now().UTC())
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if updated.Used != 3 || updated.Version != 2 {
		t.Fatalf("used=%d version=%d, want 3/2", updated.Used, updated.Version)
	}

	// Additional allocation to the same member accumulates on one row.
	updated, err = st.Allocate(ctx, e.ID, "m1", 1, time.Now().UTC())
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if updated.Used != 4 || updated.Version != 3 {
		t.Fatalf("used=%d version=%d, want 4/3", updated.Used, updated.Version)
	}

	// Over-capacity allocation is rejected and leaves the record untouched.
	before := updated
	_, err = st.Allocate(ctx, e.ID, "m2", 2, time.Now().UTC())
	if !errors.Is(err, store.ErrCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
	details, err := st.Get(ctx, e.ID, time.Now().UTC())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if details.Entitlement.Used != before.Used || details.Entitlement.Version != before.Version {
		t.Fatalf("record changed after rejected allocation: %+v", details.Entitlement)
	}
	if len(details.Members) != 1 || details.Members[0].MemberID != "m1" {
		t.Fatalf("rejected allocation must not create member rows: %+v", details.Members)
	}
}

func TestConsumeAndIdempotency(t *testing.T) {
	ctx, st := testStore(t)
	e := createActive(t, ctx, st, "quota", 10, futureTime(3600))
	now := time.Now().UTC()

	if _, err := st.Allocate(ctx, e.ID, "m1", 5, now); err != nil {
		t.Fatalf("allocate: %v", err)
	}

	first, replayed, err := st.Consume(ctx, e.ID, "m1", 3, "key-1", now)
	if err != nil || replayed {
		t.Fatalf("consume: err=%v replayed=%v", err, replayed)
	}
	if first.Used != 8 || first.Version != 3 {
		t.Fatalf("used=%d version=%d, want 8/3", first.Used, first.Version)
	}

	// Repeating the same usageKey returns the first result verbatim.
	again, replayed, err := st.Consume(ctx, e.ID, "m1", 3, "key-1", now)
	if err != nil || !replayed {
		t.Fatalf("replay: err=%v replayed=%v", err, replayed)
	}
	if again.Used != first.Used || again.Version != first.Version {
		t.Fatalf("replay changed result: first=%+v again=%+v", first, again)
	}

	// Member unused allowance is now 2; consuming 3 is rejected, used unchanged.
	_, _, err = st.Consume(ctx, e.ID, "m1", 3, "key-2", now)
	if !errors.Is(err, store.ErrInsufficient) {
		t.Fatalf("expected insufficient, got %v", err)
	}
	details, _ := st.Get(ctx, e.ID, now)
	if details.Entitlement.Used != 8 {
		t.Fatalf("used changed after rejected consume: %d", details.Entitlement.Used)
	}

	// Member without any allocation cannot consume.
	_, _, err = st.Consume(ctx, e.ID, "m2", 1, "key-3", now)
	if !errors.Is(err, store.ErrInsufficient) {
		t.Fatalf("expected insufficient for unallocated member, got %v", err)
	}

	// Same key with different parameters is a conflict, not a replay.
	_, _, err = st.Consume(ctx, e.ID, "m1", 2, "key-1", now)
	if !errors.Is(err, store.ErrUsageKeyConflict) {
		t.Fatalf("expected usage key conflict, got %v", err)
	}
}

func TestCancel(t *testing.T) {
	ctx, st := testStore(t)
	e := createActive(t, ctx, st, "seat", 5, futureTime(3600))
	now := time.Now().UTC()
	if _, err := st.Allocate(ctx, e.ID, "m1", 2, now); err != nil {
		t.Fatalf("allocate: %v", err)
	}
	accepted, _, err := st.Consume(ctx, e.ID, "m1", 1, "key-before-cancel", now)
	if err != nil {
		t.Fatalf("consume before cancel: %v", err)
	}

	cancelled, err := st.Cancel(ctx, e.ID, now)
	if err != nil || cancelled.Status != "cancelled" {
		t.Fatalf("cancel: %+v err=%v", cancelled, err)
	}

	// Used amounts are preserved.
	details, _ := st.Get(ctx, e.ID, now)
	if details.Entitlement.Status != "cancelled" || details.Entitlement.Used != 3 {
		t.Fatalf("unexpected after cancel: %+v", details.Entitlement)
	}

	// Allocation and new usage are refused after cancellation.
	if _, err := st.Allocate(ctx, e.ID, "m2", 1, now); !errors.Is(err, store.ErrNotOperable) {
		t.Fatalf("allocate after cancel: %v", err)
	}
	if _, _, err := st.Consume(ctx, e.ID, "m1", 1, "key-after-cancel", now); !errors.Is(err, store.ErrNotOperable) {
		t.Fatalf("consume after cancel: %v", err)
	}

	// Cancelling again is idempotent and still returns the record.
	again, err := st.Cancel(ctx, e.ID, now)
	if err != nil || again.Status != "cancelled" || again.Used != 3 {
		t.Fatalf("repeat cancel: %+v err=%v", again, err)
	}

	// The previously accepted usage still replays, returning its first result.
	replay, replayedFlag, err := st.Consume(ctx, e.ID, "m1", 1, "key-before-cancel", now)
	if err != nil || !replayedFlag {
		t.Fatalf("replay after cancel: err=%v replayed=%v", err, replayedFlag)
	}
	if replay.Used != accepted.Used || replay.Version != accepted.Version {
		t.Fatalf("replay result diverged: accepted=%+v replay=%+v", accepted, replay)
	}
}

func TestExpiry(t *testing.T) {
	ctx, st := testStore(t)
	// The store itself does not require a future expiry, so seed an expired one.
	e := createActive(t, ctx, st, "seat", 5, futureTime(-10))
	now := time.Now().UTC()

	details, err := st.Get(ctx, e.ID, now)
	if err != nil {
		t.Fatalf("get expired: %v", err)
	}
	if details.Entitlement.Status != "expired" {
		t.Fatalf("status=%s, want expired", details.Entitlement.Status)
	}
	if _, err := st.Allocate(ctx, e.ID, "m1", 1, now); !errors.Is(err, store.ErrNotOperable) {
		t.Fatalf("allocate after expiry: %v", err)
	}
	if _, _, err := st.Consume(ctx, e.ID, "m1", 1, "k", now); !errors.Is(err, store.ErrNotOperable) {
		t.Fatalf("consume after expiry: %v", err)
	}
	if _, err := st.Cancel(ctx, e.ID, now); !errors.Is(err, store.ErrNotOperable) {
		t.Fatalf("cancel expired: %v", err)
	}
}

func TestNotFound(t *testing.T) {
	ctx, st := testStore(t)
	missing := "00000000-0000-0000-0000-000000000000"
	if _, err := st.Allocate(ctx, missing, "m1", 1, time.Now().UTC()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("allocate: %v", err)
	}
	if _, _, err := st.Consume(ctx, missing, "m1", 1, "k", time.Now().UTC()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("consume: %v", err)
	}
	if _, err := st.Cancel(ctx, missing, time.Now().UTC()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := st.Get(ctx, missing, time.Now().UTC()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get: %v", err)
	}
}

func TestConcurrentAllocationsSerialize(t *testing.T) {
	ctx, st := testStore(t)
	e := createActive(t, ctx, st, "seat", 10, futureTime(3600))

	const goroutines = 20
	var wg sync.WaitGroup
	results := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := st.Allocate(ctx, e.ID, fmt.Sprintf("m%d", i), 1, time.Now().UTC())
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	var ok, rejected int
	for err := range results {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, store.ErrCapacity):
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 10 || rejected != 10 {
		t.Fatalf("ok=%d rejected=%d, want 10/10", ok, rejected)
	}
	details, _ := st.Get(ctx, e.ID, time.Now().UTC())
	if details.Entitlement.Used != 10 || details.Entitlement.Version != 11 {
		t.Fatalf("used=%d version=%d, want 10/11", details.Entitlement.Used, details.Entitlement.Version)
	}
}

func TestConcurrentSameUsageKeyCountsOnce(t *testing.T) {
	ctx, st := testStore(t)
	e := createActive(t, ctx, st, "quota", 100, futureTime(3600))
	if _, err := st.Allocate(ctx, e.ID, "m1", 100, time.Now().UTC()); err != nil {
		t.Fatalf("allocate: %v", err)
	}

	const goroutines = 16
	var wg sync.WaitGroup
	type outcome struct {
		used, version int64
		replayed      bool
		err           error
	}
	results := make(chan outcome, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ent, replayed, err := st.Consume(ctx, e.ID, "m1", 1, "same-key", time.Now().UTC())
			results <- outcome{ent.Used, ent.Version, replayed, err}
		}()
	}
	wg.Wait()
	close(results)

	var first outcome
	sawFirst := false
	replays := 0
	for o := range results {
		if o.err != nil {
			t.Fatalf("unexpected error: %v", o.err)
		}
		if !sawFirst {
			first, sawFirst = o, true
			continue
		}
		if o.used != first.used || o.version != first.version {
			t.Fatalf("divergent replay results: %+v vs %+v", first, o)
		}
		if o.replayed {
			replays++
		}
	}
	if replays != goroutines-1 {
		t.Fatalf("replays=%d, want %d", replays, goroutines-1)
	}
	details, _ := st.Get(ctx, e.ID, time.Now().UTC())
	// One allocation of 100 plus a single usage of 1.
	if details.Entitlement.Used != 101 {
		t.Fatalf("used=%d, want 101", details.Entitlement.Used)
	}
}

func TestGetDetailsGroupsByMember(t *testing.T) {
	ctx, st := testStore(t)
	e := createActive(t, ctx, st, "quota", 100, futureTime(3600))
	now := time.Now().UTC()
	if _, err := st.Allocate(ctx, e.ID, "m1", 10, now); err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if _, err := st.Allocate(ctx, e.ID, "m2", 5, now); err != nil {
		t.Fatalf("allocate: %v", err)
	}
	for _, key := range []string{"m1-a", "m1-b"} {
		if _, _, err := st.Consume(ctx, e.ID, "m1", 1, key, now); err != nil {
			t.Fatalf("consume %s: %v", key, err)
		}
	}

	details, err := st.Get(ctx, e.ID, now)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(details.Members) != 2 {
		t.Fatalf("members=%+v", details.Members)
	}
	// Ordered by member id: m1 then m2.
	if details.Members[0].MemberID != "m1" || details.Members[0].Used != 2 ||
		len(details.Members[0].UsageKeys) != 2 ||
		details.Members[0].UsageKeys[0] != "m1-a" || details.Members[0].UsageKeys[1] != "m1-b" {
		t.Fatalf("m1 detail wrong: %+v", details.Members[0])
	}
	if details.Members[1].MemberID != "m2" || details.Members[1].Used != 0 ||
		len(details.Members[1].UsageKeys) != 0 {
		t.Fatalf("m2 detail wrong: %+v", details.Members[1])
	}
}
