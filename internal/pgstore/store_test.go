package pgstore

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

// testSchema isolates this service's tables from anything else sharing the
// test database (the unqualified table name is resolved via search_path).
const testSchema = "ent_service_test"

// testPool connects to ENTITLEMENT_TEST_DATABASE_URL in an isolated schema and
// applies the service schema there. The integration test is skipped when the
// variable is unset.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("ENTITLEMENT_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set ENTITLEMENT_TEST_DATABASE_URL to run the PostgreSQL integration test")
	}
	if !strings.Contains(url, "options=") {
		sep := "?"
		if strings.Contains(url, "?") {
			sep = "&"
		}
		url += sep + "options=-csearch_path%3D" + testSchema
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+testSchema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS entitlements`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	store := New(pool)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}
	return pool
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestStoreLifecycle(t *testing.T) {
	pool := testPool(t)
	store := New(pool)
	ctx := context.Background()
	g := entitlement.Grant{
		Tenant: "acme", GrantID: "g1", Feature: "seats", Amount: 5,
		EffectiveAt: mustTime("2026-01-01T00:00:00Z"),
		ExpiresAt:   mustTime("2026-12-31T23:59:59Z"),
		State:       entitlement.StateActive,
	}

	created, isCreated, err := store.Put(ctx, g)
	if err != nil || !isCreated {
		t.Fatalf("first Put: grant=%+v created=%v err=%v", created, isCreated, err)
	}

	// Idempotent retry with the same instant expressed in another offset.
	retry := g
	retry.EffectiveAt = mustTime("2026-01-01T02:00:00+02:00")
	retry.ExpiresAt = mustTime("2027-01-01T01:59:59+02:00")
	got, isCreated, err := store.Put(ctx, retry)
	if err != nil || isCreated {
		t.Fatalf("idempotent Put: created=%v err=%v", isCreated, err)
	}
	if got.Amount != 5 {
		t.Fatalf("retry returned different record: %+v", got)
	}

	// Conflicting content.
	conflict := g
	conflict.Amount = 99
	if _, _, err := store.Put(ctx, conflict); !errors.Is(err, entitlement.ErrConflict) {
		t.Fatalf("conflict Put err=%v, want ErrConflict", err)
	}

	// Same grantId under another tenant is independent.
	other := g
	other.Tenant = "globex"
	if _, isCreated, err := store.Put(ctx, other); err != nil || !isCreated {
		t.Fatalf("other-tenant Put: created=%v err=%v", isCreated, err)
	}

	// Concurrent identical creates insert exactly one row; losers see the
	// original with created=false.
	const racers = 8
	var wg sync.WaitGroup
	results := make([]bool, racers)
	errs := make([]error, racers)
	racer := g
	racer.Tenant = "racetenant"
	racer.GrantID = "racer"
	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i], errs[i] = store.Put(context.Background(), racer)
		}(i)
	}
	wg.Wait()
	createdN := 0
	for i := range racers {
		if errs[i] != nil {
			t.Fatalf("racer %d: %v", i, errs[i])
		}
		if results[i] {
			createdN++
		}
	}
	if createdN != 1 {
		t.Fatalf("concurrent identical creates: %d rows inserted, want 1", createdN)
	}

	// Active interval: left-closed, right-open.
	cases := []struct {
		at     string
		wantID string
		ok     bool
	}{
		{"2026-01-01T00:00:00Z", "g1", true},
		{"2026-06-01T00:00:00Z", "g1", true},
		{"2026-12-31T23:59:59Z", "", false}, // == expiresAt, excluded
		{"2025-12-31T23:59:59Z", "", false}, // before effectiveAt
	}
	for _, tc := range cases {
		grants, err := store.ActiveAt(ctx, "acme", mustTime(tc.at))
		if err != nil {
			t.Fatalf("ActiveAt %s: %v", tc.at, err)
		}
		found := len(grants) == 1
		if found != tc.ok || (found && grants[0].GrantID != tc.wantID) {
			t.Fatalf("ActiveAt %s = %+v, ok=%v", tc.at, grants, tc.ok)
		}
	}

	// Cancel: 404 then success, then the grant never shows up at any asOf.
	if _, err := store.Cancel(ctx, "acme", "missing", mustTime("2026-06-01T00:00:00Z")); !errors.Is(err, entitlement.ErrNotFound) {
		t.Fatalf("cancel missing err=%v, want ErrNotFound", err)
	}
	cancelled, err := store.Cancel(ctx, "acme", "g1", mustTime("2026-06-01T00:00:00Z"))
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.State != entitlement.StateCancelled {
		t.Fatalf("state=%q", cancelled.State)
	}
	for _, at := range []string{"2026-01-01T00:00:00Z", "2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z"} {
		grants, err := store.ActiveAt(ctx, "acme", mustTime(at))
		if err != nil {
			t.Fatalf("ActiveAt after cancel: %v", err)
		}
		if len(grants) != 0 {
			t.Fatalf("cancelled grant returned at %s: %+v", at, grants)
		}
	}
}
