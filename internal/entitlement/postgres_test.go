package entitlement

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests require a real PostgreSQL. Run with:
//
//	TEST_DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' go test ./...
func testStore(t *testing.T) *PGStore {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	store := NewPGStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE entitlements CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store
}

func futureTime() time.Time {
	return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
}

func codeIs(err error, code string) bool {
	apiErr, ok := AsAPIError(err)
	return ok && apiErr.Code == code
}

func TestPGCreateAndGet(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	expires := futureTime()

	e, err := s.Create(ctx, CreateParams{TeamID: "team-a", Type: Seat, Total: 10, ExpiresAt: expires})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if e.Status != Active || e.Used != 0 || e.Version != 1 || e.ID == "" {
		t.Fatalf("unexpected record: %+v", e)
	}
	if e.ExpiresAt.Location() != time.UTC || !e.ExpiresAt.Equal(expires) {
		t.Fatalf("expiresAt not UTC normalized: %v", e.ExpiresAt)
	}

	got, err := s.Get(ctx, e.ID)
	if err != nil || got.ID != e.ID {
		t.Fatalf("get: %v %+v", err, got)
	}

	// Duplicate (same team+type+expiry) is rejected and returns the existing row.
	dup, err := s.Create(ctx, CreateParams{TeamID: "team-a", Type: Seat, Total: 10, ExpiresAt: expires})
	if err == nil {
		t.Fatalf("duplicate create succeeded: %+v", dup)
	}
	conflict, ok := err.(*ConflictError)
	if !ok {
		t.Fatalf("want ConflictError, got %T %v", err, err)
	}
	if conflict.Existing.ID != e.ID {
		t.Fatalf("conflict returned %s, want existing %s", conflict.Existing.ID, e.ID)
	}
	if conflict.Kind != KindConflict {
		t.Fatalf("kind=%v", conflict.Kind)
	}
}

func TestPGCreateRejectsPastExpiry(t *testing.T) {
	s := testStore(t)
	s.now = func() time.Time { return time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC) }
	_, err := s.Create(context.Background(), CreateParams{
		TeamID: "t", Type: Seat, Total: 1, ExpiresAt: s.now().Add(-time.Minute),
	})
	if apiErr, ok := AsAPIError(err); !ok || apiErr.Kind != KindInvalid {
		t.Fatalf("want invalid error, got %v", err)
	}
}

func TestPGAllocationLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Seat, Total: 5, ExpiresAt: futureTime()})

	got, err := s.Allocate(ctx, e.ID, "m1", 2)
	if err != nil || got.Used != 2 || got.Version != 2 {
		t.Fatalf("alloc1: %v %+v", err, got)
	}
	got, err = s.Allocate(ctx, e.ID, "m1", 2)
	if err != nil || got.Used != 4 || got.Version != 3 {
		t.Fatalf("alloc2 (same member accumulates): %v %+v", err, got)
	}

	// Oversell is rejected and the record is untouched.
	bad, err := s.Allocate(ctx, e.ID, "m2", 2)
	if apiErr, ok := AsAPIError(err); !ok || apiErr.Code != "allocation_exceeds_total" {
		t.Fatalf("want allocation_exceeds_total, got %v (%+v)", err, bad)
	}
	got, _ = s.Get(ctx, e.ID)
	if got.Used != 4 || got.Version != 3 {
		t.Fatalf("failed allocation changed record: %+v", got)
	}
}

func TestPGUsageIdempotency(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Quota, Total: 100, ExpiresAt: futureTime()})
	if _, err := s.Allocate(ctx, e.ID, "m1", 10); err != nil {
		t.Fatal(err)
	}

	first, rec, replayed, err := s.Use(ctx, e.ID, "m1", 4, "key-1")
	if err != nil || replayed || first.Used != 14 || first.Version != 3 {
		t.Fatalf("first use: err=%v rec=%+v e=%+v replayed=%v", err, rec, first, replayed)
	}

	// Replay: same key, same params -> identical snapshot, no accumulation.
	second, rec2, replayed2, err := s.Use(ctx, e.ID, "m1", 4, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if !replayed2 || second.Used != 14 || second.Version != 3 {
		t.Fatalf("replay changed state: replayed=%v %+v", replayed2, second)
	}
	if rec2 != rec {
		t.Fatalf("replay record=%+v want %+v", rec2, rec)
	}

	// Later operations advance the entitlement...
	later, _, _, err := s.Use(ctx, e.ID, "m1", 1, "key-2")
	if err != nil || later.Used != 15 || later.Version != 4 {
		t.Fatalf("later use: %v %+v", err, later)
	}
	// ...but the key-1 replay must still return the original snapshot.
	replay, _, replayed3, err := s.Use(ctx, e.ID, "m1", 4, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if !replayed3 || replay.Used != 14 || replay.Version != 3 {
		t.Fatalf("replay snapshot drifted: %+v replayed=%v", replay, replayed3)
	}

	// Same key with different parameters is a conflict, state unchanged.
	_, _, _, err = s.Use(ctx, e.ID, "m1", 5, "key-1")
	var keyErr *UsageKeyConflictError
	if e2, ok := err.(*UsageKeyConflictError); !ok {
		t.Fatalf("want UsageKeyConflictError, got %v", err)
	} else {
		keyErr = e2
	}
	if keyErr.Existing.MemberID != "m1" || keyErr.Existing.Quantity != 4 {
		t.Fatalf("existing usage %+v", keyErr.Existing)
	}
	got, _ := s.Get(ctx, e.ID)
	if got.Used != 15 {
		t.Fatalf("key-reuse conflict changed used: %d", got.Used)
	}

	// Over the member's remaining headroom (10 allocated, 5 used -> 5 left).
	_, _, _, err = s.Use(ctx, e.ID, "m1", 6, "key-3")
	if apiErr, ok := AsAPIError(err); !ok || apiErr.Code != "usage_exceeds_member_remaining" {
		t.Fatalf("want usage_exceeds_member_remaining, got %v", err)
	}
	got, _ = s.Get(ctx, e.ID)
	if got.Used != 15 {
		t.Fatalf("rejected usage accumulated: %d", got.Used)
	}

	// A member with no allocation cannot use anything.
	if _, err := s.Allocate(ctx, e.ID, "m2", 2); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.Use(ctx, e.ID, "m3", 1, "key-m3"); err == nil {
		t.Fatal("usage without allocation must be rejected")
	}

	// A repeated key keeps replaying even after cancellation, while a new
	// key is refused.
	if _, err := s.Cancel(ctx, e.ID); err != nil {
		t.Fatal(err)
	}
	replayAfterCancel, _, replayedAfterCancel, err := s.Use(ctx, e.ID, "m1", 4, "key-1")
	if err != nil || !replayedAfterCancel {
		t.Fatalf("replay after cancel: err=%v replayed=%v", err, replayedAfterCancel)
	}
	if replayAfterCancel.Used != 14 || replayAfterCancel.Version != 3 || replayAfterCancel.Status != Active {
		t.Fatalf("replay snapshot after cancel drifted: %+v", replayAfterCancel)
	}
	if _, _, _, err := s.Use(ctx, e.ID, "m1", 1, "key-new-after-cancel"); !codeIs(err, "entitlement_cancelled") {
		t.Fatalf("new key after cancel: %v", err)
	}
}

func TestPGUsageReplayAfterExpiry(t *testing.T) {
	s := testStore(t)
	base := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return base }
	ctx := context.Background()
	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Quota, Total: 50, ExpiresAt: base.Add(time.Hour)})
	_, _ = s.Allocate(ctx, e.ID, "m1", 10)
	first, _, _, err := s.Use(ctx, e.ID, "m1", 3, "key-1")
	if err != nil || first.Used != 13 || first.Status != Active {
		t.Fatalf("first use: %v %+v", err, first)
	}

	s.now = func() time.Time { return base.Add(2 * time.Hour) }
	// Fresh key is refused once expired...
	if _, _, _, err := s.Use(ctx, e.ID, "m1", 1, "key-fresh"); !codeIs(err, "entitlement_expired") {
		t.Fatalf("fresh key after expiry: %v", err)
	}
	// ...but the historical key replays its first, still-active snapshot.
	replay, rec, replayed, err := s.Use(ctx, e.ID, "m1", 3, "key-1")
	if err != nil || !replayed {
		t.Fatalf("replay after expiry: err=%v replayed=%v", err, replayed)
	}
	if replay.Status != Active || replay.Used != 13 || replay.Version != 3 {
		t.Fatalf("replay snapshot after expiry drifted: %+v", replay)
	}
	if rec.Quantity != 3 || rec.MemberID != "m1" {
		t.Fatalf("replay record: %+v", rec)
	}
}

func TestPGCancelAndExpire(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return base }

	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Seat, Total: 10, ExpiresAt: base.Add(time.Hour)})
	if _, err := s.Allocate(ctx, e.ID, "m1", 3); err != nil {
		t.Fatal(err)
	}
	cancelled, err := s.Cancel(ctx, e.ID)
	if err != nil || cancelled.Status != Cancelled || cancelled.Used != 3 {
		t.Fatalf("cancel: %v %+v", err, cancelled)
	}

	// History remains readable...
	got, _ := s.Get(ctx, e.ID)
	if got.Status != Cancelled || got.Used != 3 {
		t.Fatalf("read after cancel: %+v", got)
	}
	// ...but mutations are rejected.
	for name, fn := range map[string]func() error{
		"allocate": func() error { _, e := s.Allocate(ctx, e.ID, "m2", 1); return e },
		"use":      func() error { _, _, _, e := s.Use(ctx, e.ID, "m1", 1, "k"); return e },
		"cancel":   func() error { _, e := s.Cancel(ctx, e.ID); return e },
	} {
		if err := fn(); err == nil {
			t.Fatalf("%s after cancel must fail", name)
		} else if apiErr, ok := AsAPIError(err); !ok || apiErr.Kind != KindConflict {
			t.Fatalf("%s after cancel: %v", name, err)
		}
	}

	// Expiry is derived on read and blocks mutation.
	exp, _ := s.Create(ctx, CreateParams{TeamID: "t2", Type: Seat, Total: 10, ExpiresAt: base.Add(time.Hour)})
	s.now = func() time.Time { return base.Add(2 * time.Hour) }
	got, _ = s.Get(ctx, exp.ID)
	if got.Status != Expired {
		t.Fatalf("status=%v want expired", got.Status)
	}
	if _, err := s.Allocate(ctx, exp.ID, "m1", 1); !codeIs(err, "entitlement_expired") {
		t.Fatalf("allocate expired: %v", err)
	}
	if _, _, _, err := s.Use(ctx, exp.ID, "m1", 1, "k"); !codeIs(err, "entitlement_expired") {
		t.Fatalf("use expired: %v", err)
	}
	if _, err := s.Cancel(ctx, exp.ID); !codeIs(err, "entitlement_expired") {
		t.Fatalf("cancel expired: %v", err)
	}
	// Detail of an expired entitlement stays available.
	d, err := s.Detail(ctx, exp.ID)
	if err != nil || d.Status != Expired {
		t.Fatalf("detail expired: %v %+v", err, d)
	}
}

func TestPGDetail(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Quota, Total: 100, ExpiresAt: futureTime()})
	_, _ = s.Allocate(ctx, e.ID, "alice", 10)
	_, _ = s.Allocate(ctx, e.ID, "bob", 5)
	_, _, _, _ = s.Use(ctx, e.ID, "alice", 3, "a1")
	_, _, _, _ = s.Use(ctx, e.ID, "alice", 2, "a2")
	_, _, _, _ = s.Use(ctx, e.ID, "alice", 3, "a1") // replay

	d, err := s.Detail(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Members) != 2 {
		t.Fatalf("members=%+v", d.Members)
	}
	m := map[string]MemberUsage{}
	for _, mu := range d.Members {
		m[mu.MemberID] = mu
	}
	if m["alice"].Used != 5 || fmt.Sprint(m["alice"].UsageKeys) != "[a1 a2]" {
		t.Fatalf("alice: %+v", m["alice"])
	}
	if len(m["bob"].UsageKeys) != 0 || m["bob"].Used != 0 {
		t.Fatalf("bob: %+v", m["bob"])
	}

	if _, err := s.Detail(ctx, "does-not-exist"); err == nil {
		t.Fatal("missing detail must error")
	} else if apiErr, ok := AsAPIError(err); !ok || apiErr.Kind != KindNotFound {
		t.Fatalf("missing detail: %v", err)
	}
}

func TestPGConcurrentAllocation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	const total = 20
	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Seat, Total: total, ExpiresAt: futureTime()})

	const goroutines = 50
	var wg sync.WaitGroup
	var okN, failN int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := s.Allocate(ctx, e.ID, fmt.Sprintf("m-%d", i), 1)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				okN++
			} else {
				failN++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if okN != total || failN != goroutines-total {
		t.Fatalf("successful=%d failed=%d want %d and %d", okN, failN, total, goroutines-total)
	}
	got, _ := s.Get(ctx, e.ID)
	if got.Used != total || got.Version != total+1 {
		t.Fatalf("used=%d version=%d, want %d and %d", got.Used, got.Version, total, total+1)
	}
	d, err := s.Detail(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Members) != total {
		t.Fatalf("members=%d, exactly %d winning allocations must persist", len(d.Members), total)
	}
}

func TestPGConcurrentUsageSameKey(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Quota, Total: 1000, ExpiresAt: futureTime()})
	_, _ = s.Allocate(ctx, e.ID, "m1", 1000)

	const goroutines = 30
	var wg sync.WaitGroup
	var firsts, replays, failures int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, replayed, err := s.Use(ctx, e.ID, "m1", 1, "hot-key")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failures++
			case replayed:
				replays++
			default:
				firsts++
			}
		}()
	}
	close(start)
	wg.Wait()

	if firsts != 1 {
		t.Fatalf("first usages=%d, want exactly 1", firsts)
	}
	if firsts+replays != goroutines || failures != 0 {
		t.Fatalf("firsts=%d replays=%d failures=%d total=%d", firsts, replays, failures, goroutines)
	}
	got, _ := s.Get(ctx, e.ID)
	if got.Used != 1001 {
		t.Fatalf("used=%d, same key must accumulate exactly once (1000 alloc + 1)", got.Used)
	}
}

func TestPGConcurrentUsageNoOversell(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	e, _ := s.Create(ctx, CreateParams{TeamID: "t", Type: Quota, Total: 1000, ExpiresAt: futureTime()})
	_, _ = s.Allocate(ctx, e.ID, "m1", 10)

	const goroutines = 30
	var wg sync.WaitGroup
	var successes int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, _, err := s.Use(ctx, e.ID, "m1", 1, fmt.Sprintf("key-%d", i))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if successes != 10 {
		t.Fatalf("successful usages=%d, want exactly 10 (the member's headroom)", successes)
	}
	got, _ := s.Get(ctx, e.ID)
	if got.Used != 20 {
		t.Fatalf("used=%d want 20 (10 allocated + 10 consumed)", got.Used)
	}
}
