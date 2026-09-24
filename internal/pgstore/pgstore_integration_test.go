package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
	"github.com/lansolocoder/cla03-entitlement-service/internal/pgstore"
)

func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS entitlements")
		pool.Close()
	})
	return pool
}

func TestStoreIntegration(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)
	store := pgstore.New(pool)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	expiry := time.Now().UTC().Add(365 * 24 * time.Hour).Truncate(time.Second)
	input := entitlement.CreateInput{Key: "seats", Quantity: 10, ExpiresAt: expiry}

	created, isNew, err := store.Create(ctx, "acme", input)
	if err != nil || !isNew {
		t.Fatalf("first create: new=%v err=%v", isNew, err)
	}
	if created.Status != "active" || created.Consumed != 0 || created.Reserved != 0 {
		t.Fatalf("defaults wrong: %+v", created)
	}
	if !created.CreatedAt.Equal(created.CreatedAt.UTC()) || created.CreatedAt.Year() < 2026 {
		t.Fatalf("createdAt not sane UTC: %v", created.CreatedAt)
	}

	// Idempotent repeat: same record, not new.
	again, isNew, err := store.Create(ctx, "acme", input)
	if err != nil || isNew {
		t.Fatalf("identical repeat: new=%v err=%v", isNew, err)
	}
	if again.ID != created.ID || again.CreatedAt != created.CreatedAt {
		t.Fatalf("idempotent repeat returned a different record")
	}

	// Conflicting quantity and expiry.
	for _, mutate := range []func(entitlement.CreateInput) entitlement.CreateInput{
		func(in entitlement.CreateInput) entitlement.CreateInput { in.Quantity = 11; return in },
		func(in entitlement.CreateInput) entitlement.CreateInput {
			in.ExpiresAt = in.ExpiresAt.Add(time.Hour)
			return in
		},
	} {
		_, _, err := store.Create(ctx, "acme", mutate(input))
		var conflict *entitlement.ConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("expected ConflictError, got %v", err)
		}
		if conflict.Existing.ID != created.ID {
			t.Fatalf("conflict must reference the untouched existing row")
		}
	}

	// Original row is untouched.
	got, err := store.Get(ctx, "acme", "seats")
	if err != nil {
		t.Fatal(err)
	}
	if got.Quantity != 10 || !got.ExpiresAt.Equal(expiry) || got.ID != created.ID {
		t.Fatalf("existing row was modified: %+v", got)
	}

	// Unknown key.
	if _, err := store.Get(ctx, "acme", "missing"); !errors.Is(err, entitlement.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// List ordered by key across a couple of rows and tenants.
	if _, _, err := store.Create(ctx, "acme", entitlement.CreateInput{Key: "aaa", Quantity: 1, ExpiresAt: expiry}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(ctx, "acme", entitlement.CreateInput{Key: "zzz", Quantity: 1, ExpiresAt: expiry}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(ctx, "globex", input); err != nil {
		t.Fatal(err)
	}
	items, err := store.List(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].Key != "aaa" || items[1].Key != "seats" || items[2].Key != "zzz" {
		var keys []string
		for _, e := range items {
			keys = append(keys, e.Key)
		}
		t.Fatalf("list not sorted/filtered: %v", keys)
	}

	// Row count for acme/seats stays exactly one.
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM entitlements WHERE tenant='acme' AND key='seats'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected exactly one row, found %d", n)
	}
}

func TestStoreConcurrentIdenticalCreates(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)
	store := pgstore.New(pool)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	expiry := time.Now().UTC().Add(365 * 24 * time.Hour).Truncate(time.Second)
	input := entitlement.CreateInput{Key: "seats", Quantity: 10, ExpiresAt: expiry}
	const submitters = 32

	var wg sync.WaitGroup
	results := make(chan string, submitters)
	errs := make(chan error, submitters)
	start := make(chan struct{})
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, isNew, err := store.Create(ctx, "race-tenant", input)
			if err != nil {
				errs <- err
				return
			}
			if isNew {
				results <- "created"
			} else {
				results <- "idempotent"
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent create failed: %v", err)
	}
	created, idempotent := 0, 0
	for r := range results {
		switch r {
		case "created":
			created++
		case "idempotent":
			idempotent++
		}
	}
	if created != 1 {
		t.Fatalf("exactly one request must insert, got created=%d", created)
	}
	if created+idempotent != submitters {
		t.Fatalf("accounting: created=%d idempotent=%d total=%d", created, idempotent, submitters)
	}

	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM entitlements WHERE tenant='race-tenant'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected a single surviving row, got %d", n)
	}
}

func TestStoreConcurrentConflictingCreates(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)
	store := pgstore.New(pool)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	const submitters = 24
	expiry := time.Now().UTC().Add(365 * 24 * time.Hour).Truncate(time.Second)

	var wg sync.WaitGroup
	creates, idempotent, conflicts := 0, 0, 0
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Half submit quantity 10, half quantity 20: exactly one
			// request wins; losers get either idempotent 200 (same params
			// as the winner) or 409 (different params), never a 500.
			qty := int64(10)
			if i%2 == 1 {
				qty = 20
			}
			_, isNew, err := store.Create(ctx, "race-conflict",
				entitlement.CreateInput{Key: "seats", Quantity: qty, ExpiresAt: expiry})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && isNew:
				creates++
			case err == nil:
				idempotent++
			default:
				var conflict *entitlement.ConflictError
				if !errors.As(err, &conflict) {
					t.Errorf("unexpected error: %v", err)
					return
				}
				conflicts++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if creates != 1 {
		t.Fatalf("expected exactly 1 created, got %d", creates)
	}
	if creates+idempotent+conflicts != submitters {
		t.Fatalf("all submitters must resolve: created=%d idempotent=%d conflicts=%d", creates, idempotent, conflicts)
	}
	if idempotent+conflicts != submitters-1 {
		t.Fatalf("expected %d non-created responses, got %d", submitters-1, idempotent+conflicts)
	}
	var n int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf("SELECT count(*) FROM entitlements WHERE tenant='race-conflict'")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected a single surviving row, got %d", n)
	}
}
