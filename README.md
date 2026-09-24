# Entitlement service

Go HTTP service for team service entitlements. Requires Go 1.27.1 and PostgreSQL 18.6. The service provisions its `grants` table automatically at startup.

```sh
go mod download
go test ./...
ENTITLEMENTS_TEST_DATABASE_URL='postgres://127.0.0.1:15432/entitlements_test?sslmode=disable' go test ./...
mkdir -p .local
initdb -D .local/pgdata --auth=trust --encoding=UTF8 --locale=C
pg_ctl -D .local/pgdata -l .local/postgres.log -o '-h 127.0.0.1 -p 15432' -w start
createdb -h 127.0.0.1 -p 15432 entitlements
DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/server
```

## Endpoints

`GET /healthz` returns HTTP 200 with `{"status":"ok"}`. `GET /readyz` checks PostgreSQL and returns HTTP 200 with `{"status":"ready"}`, or HTTP 503 with `{"status":"unavailable"}`.

Grant entitlements use a half-open validity interval `[effectiveAt, expiresAt)`:

- `POST /tenants/{tenant}/entitlements` opens a grant. The JSON body carries `grantId` (idempotency key), `feature` (`seats` or `quota`), `amount` (positive integer), `effectiveAt` and `expiresAt` (RFC 3339, `expiresAt` strictly later). Returns **201** with `tenant`, `grantId`, `feature`, `amount`, `effectiveAt`, `expiresAt`, `state` on first success. Resubmitting the same `grantId` with identical parameters is an idempotent retry and returns **200** with the stored record; any differing parameter returns **409** without writing data.
- `GET /tenants/{tenant}/entitlements?at=<RFC3339>` returns `{"tenant","asOf","grants":[…]}` with every non-cancelled grant whose interval contains `asOf`. Elements carry the same six grant fields plus `state` (`active`). Grants outside the interval (including at `expiresAt`) are omitted.
- `POST /tenants/{tenant}/entitlements/{grantId}/cancel` with `{"cancelledAt":RFC3339}` sets `state` to `cancelled`; the grant is then absent from every query. An unknown `grantId` returns **404** without changing state.

Timestamps are parsed as RFC 3339 and echoed normalized to whole-second UTC with a trailing `Z`. Validation failures (missing fields, non-positive/non-integer `amount`, malformed timestamps or an invalid interval) return **400** with `{"error":...}`. Unmatched routes return **404** and wrong methods **405**, matching the built-in mux behavior. Database write failures return **500** with no partial data.

`DATABASE_URL` is required; `LISTEN_ADDR` defaults to `127.0.0.1:8080`. Stop the server with Ctrl+C and the local database with `pg_ctl -D .local/pgdata -m fast -w stop`.

The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies.
