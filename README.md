# Entitlement service

Go HTTP service for team service entitlements. Requires Go 1.27.1 and PostgreSQL 18.6. It provides health probes and the first entitlement API (creating and listing tenant entitlements).

```sh
go mod download
go test ./...
mkdir -p .local
initdb -D .local/pgdata --auth=trust --encoding=UTF8 --locale=C
pg_ctl -D .local/pgdata -l .local/postgres.log -o '-h 127.0.0.1 -p 15432' -w start
createdb -h 127.0.0.1 -p 15432 entitlements
DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/server
```

`GET /healthz` returns HTTP 200 with `{"status":"ok"}`. `GET /readyz` checks PostgreSQL and returns HTTP 200 with `{"status":"ready"}`, or HTTP 503 with `{"status":"unavailable"}`. The `entitlements` table (with a unique `(tenant, key)` constraint) is created automatically at startup. `DATABASE_URL` is required; `LISTEN_ADDR` defaults to `127.0.0.1:8080`. Stop the server with Ctrl+C and the local database with `pg_ctl -D .local/pgdata -m fast -w stop`.

## Entitlement API

### Create entitlement

```
POST /v1/tenants/{tenant}/entitlements
{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}
```

- `key` must match `^[a-z][a-z0-9_-]{1,31}$`.
- `quantity` must be an integer from 1 to 1000000.
- `expiresAt` must be an RFC3339 UTC timestamp later than the server's current time.
- `{tenant}` must be present and non-empty (otherwise 404).

Responses:

- `201` with the created record; `createdAt` is the server time rendered in UTC.
- `200` with the existing record when the same `(tenant, key)` already exists with identical quantity and expiry — no new row is written.
- `409` when a record with the same key exists but its quantity or expiry differs. The existing record is never modified.
- `400` for any validation failure; `404` for an unknown or empty tenant.

Record body:

```json
{"id":"<uuid>","tenant":"<tenant>","key":"seats","quantity":10,"consumed":0,"reserved":0,"status":"active","createdAt":"<RFC3339 UTC>","expiresAt":"2030-01-01T00:00:00Z"}
```

The unique `(tenant, key)` database constraint makes creation atomic. Concurrent identical submissions persist exactly one row; every other request receives either the idempotent `200` confirmation or a `409`, never a `500`. Once created, `quantity` and `expiresAt` cannot be changed through any public endpoint.

### List and fetch

- `GET /v1/tenants/{tenant}/entitlements` returns `{"items":[...]}` ordered by key ascending. It accepts no query parameters (a query string yields `400`). An unknown tenant returns `{"items":[]}`.
- `GET /v1/tenants/{tenant}/entitlements/{key}` returns one record, or `404` for an unknown key.

Every response is a single JSON object terminated by a newline. Unknown paths return `404`; unsupported methods return `405` with an `Allow` header.

## Testing

`go test ./...` runs the unit tests with an in-memory store. The PostgreSQL-backed tests in `internal/pgstore` additionally run when `TEST_DATABASE_URL` points at a scratch database, e.g.:

```sh
TEST_DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' go test -race ./...
```

The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies.
