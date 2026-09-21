package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lansolocoder/cla03-entitlement-service/internal/httpapi"
	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	defer pool.Close()

	st := store.New(pool)
	if err := st.EnsureSchema(ctx); err != nil {
		log.Fatalf("failed to initialize database schema: %v", err)
	}

	// Expire stale pending reservations periodically so held quota is
	// released even without traffic touching the pool.
	housekeepingCtx, cancelHousekeeping := context.WithCancel(ctx)
	defer cancelHousekeeping()
	go runHousekeeping(housekeepingCtx, st, time.Minute)

	address := os.Getenv("LISTEN_ADDR")
	if address == "" {
		address = "127.0.0.1:8080"
	}
	server := &http.Server{
		Addr: address, Handler: httpapi.New(pool.Ping, st),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
	}
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- server.ListenAndServe() }()
	select {
	case err := <-errorsCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("HTTP listener failed: ", err)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
	}
}

// runHousekeeping expires pending reservations past their deadline on a tick.
func runHousekeeping(ctx context.Context, st *store.Store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := st.ExpirePending(opCtx); err != nil {
				log.Printf("reservation housekeeping failed: %v", err)
			}
			cancel()
		}
	}
}
