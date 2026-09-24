package grants

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to PostgreSQL when ENTITLEMENTS_TEST_DATABASE_URL is
// set; otherwise the store integration tests are skipped.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("ENTITLEMENTS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ENTITLEMENTS_TEST_DATABASE_URL not set; skipping PostgreSQL integration tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE grants"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "TRUNCATE grants")
		pool.Close()
	})
	return pool
}

func sec(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

func TestPostgresCreateIdempotencyAndConflict(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	store := NewPostgresStore(pool)

	g := Grant{
		Tenant: "acme", GrantID: "g-1", Feature: FeatureSeats, Amount: 10,
		EffectiveAt: sec(2026, 1, 1, 0, 0), ExpiresAt: sec(2026, 2, 1, 0, 0),
		State: StateActive,
	}
	stored, reused, err := store.Create(ctx, g)
	if err != nil || reused || stored.State != StateActive {
		t.Fatalf("first create: stored=%+v reused=%v err=%v", stored, reused, err)
	}

	// Identical retry (input kept at second precision UTC).
	stored2, reused, err := store.Create(ctx, g)
	if err != nil || !reused {
		t.Fatalf("retry: stored=%+v reused=%v err=%v", stored2, reused, err)
	}
	if stored2.GrantID != "g-1" || stored2.Amount != 10 {
		t.Fatalf("retry returned wrong record: %+v", stored2)
	}

	// Each changed payload field conflicts; the row count must stay at one.
	for _, mutate := range []func(Grant) Grant{
		func(x Grant) Grant { x.Feature = FeatureQuota; return x },
		func(x Grant) Grant { x.Amount = 11; return x },
		func(x Grant) Grant { x.EffectiveAt = sec(2026, 1, 2, 0, 0); return x },
		func(x Grant) Grant { x.ExpiresAt = sec(2026, 3, 1, 0, 0); return x },
	} {
		if _, _, err := store.Create(ctx, mutate(g)); !errors.Is(err, ErrConflict) {
			t.Fatalf("want ErrConflict, got %v", err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM grants").Scan(&count); err != nil || count != 1 {
		t.Fatalf("row count=%d err=%v, want 1", count, err)
	}

	// Same grantId under another tenant is independent.
	other := g
	other.Tenant = "globex"
	if _, reused, err := store.Create(ctx, other); err != nil || reused {
		t.Fatalf("other-tenant create: reused=%v err=%v", reused, err)
	}
}

func TestPostgresListActiveAtHalfOpenInterval(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	store := NewPostgresStore(pool)

	g := Grant{
		Tenant: "acme", GrantID: "g-1", Feature: FeatureQuota, Amount: 5,
		EffectiveAt: sec(2026, 1, 1, 0, 0), ExpiresAt: sec(2026, 2, 1, 0, 0),
	}
	if _, _, err := store.Create(ctx, g); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		at   time.Time
		want int
	}{
		{"before", sec(2025, 12, 31, 23, 59), 0},
		{"at effective (closed left)", sec(2026, 1, 1, 0, 0), 1},
		{"inside", sec(2026, 1, 15, 12, 0), 1},
		{"at expires (open right)", sec(2026, 2, 1, 0, 0), 0},
		{"after", sec(2026, 3, 1, 0, 0), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.ListActiveAt(ctx, "acme", tc.at)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.want {
				t.Fatalf("len=%d want %d", len(got), tc.want)
			}
		})
	}
}

func TestPostgresCancel(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	store := NewPostgresStore(pool)

	g := Grant{
		Tenant: "acme", GrantID: "g-1", Feature: FeatureSeats, Amount: 3,
		EffectiveAt: sec(2026, 1, 1, 0, 0), ExpiresAt: sec(2026, 2, 1, 0, 0),
	}
	if _, _, err := store.Create(ctx, g); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Cancel(ctx, "acme", "missing", sec(2026, 1, 10, 0, 0)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	cancelled, err := store.Cancel(ctx, "acme", "g-1", sec(2026, 1, 10, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != StateCancelled || cancelled.CancelledAt == nil {
		t.Fatalf("unexpected cancelled grant: %+v", cancelled)
	}

	// Invisible for any asOf now.
	got, err := store.ListActiveAt(ctx, "acme", sec(2026, 1, 15, 0, 0))
	if err != nil || len(got) != 0 {
		t.Fatalf("after cancel list=%v err=%v", got, err)
	}

	// Second cancel is idempotent and keeps the first cancellation time.
	again, err := store.Cancel(ctx, "acme", "g-1", sec(2026, 1, 20, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !again.CancelledAt.Equal(sec(2026, 1, 10, 0, 0)) {
		t.Fatalf("cancelledAt changed on re-cancel: %v", again.CancelledAt)
	}
}
