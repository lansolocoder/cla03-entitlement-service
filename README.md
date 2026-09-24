# Entitlement service

Go HTTP service foundation for team service entitlements. Requires Go 1.27.1 and PostgreSQL 18.6. The service provides health endpoints and the tenant entitlement API.

```sh
go mod download
go test ./...
mkdir -p .local
initdb -D .local/pgdata --auth=trust --encoding=UTF8 --locale=C
pg_ctl -D .local/pgdata -l .local/postgres.log -o '-h 127.0.0.1 -p 15432' -w start
createdb -h 127.0.0.1 -p 15432 entitlements
DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/server
```

`GET /healthz` returns HTTP 200 with `{"status":"ok"}`. `GET /readyz` checks PostgreSQL and returns HTTP 200 with `{"status":"ready"}`, or HTTP 503 with `{"status":"unavailable"}`. Other paths return 404. `DATABASE_URL` is required; `LISTEN_ADDR` defaults to `127.0.0.1:8080`. Stop the server with Ctrl+C and the local database with `pg_ctl -D .local/pgdata -m fast -w stop`.

The server creates the `entitlements` table on startup; give the service its own database.

## Entitlements API

`POST /v1/tenants/{tenant}/entitlements` creates an entitlement. The request body is `{"key":"seats","quantity":10,"expiresAt":"2030-01-01T00:00:00Z"}`: `key` must match `^[a-z][a-z0-9_-]{1,31}$`, `quantity` must be an integer between 1 and 1000000, and `expiresAt` must be an RFC3339 UTC timestamp in the future. Invalid bodies return 400.

The key is unique per tenant. Re-creating with an identical quantity and expiry returns 200 with the existing record; different parameters return 409. Neither case modifies the stored record, and concurrent duplicate creates leave exactly one record. A successful create returns 201:

```json
{"id":"<uuid>","tenant":"acme","key":"seats","quantity":10,"consumed":0,"reserved":0,"status":"active","createdAt":"<RFC3339 UTC>","expiresAt":"<RFC3339 UTC>"}
```

`GET /v1/tenants/{tenant}/entitlements` returns `{"items":[...]}` sorted by key ascending; query parameters are rejected with 400. `GET /v1/tenants/{tenant}/entitlements/{key}` returns one record or 404. Every response is a single JSON object terminated by a newline. An entitlement's `quantity` and `expiresAt` cannot be changed after creation.

The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies.
