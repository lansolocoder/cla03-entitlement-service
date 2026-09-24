# Entitlement service

Go HTTP service for team service entitlements. Requires Go 1.27.1 and PostgreSQL 18.6.

```sh
go mod download
go test ./...
mkdir -p .local
initdb -D .local/pgdata --auth=trust --encoding=UTF8 --locale=C
pg_ctl -D .local/pgdata -l .local/postgres.log -o '-h 127.0.0.1 -p 15432' -w start
createdb -h 127.0.0.1 -p 15432 entitlements
DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/server
```

The schema (`entitlements` table) is created automatically at startup. The PostgreSQL-backed store integration test runs when `ENTITLEMENT_TEST_DATABASE_URL` is set (it isolates its tables in the `ent_service_test` schema via `search_path`); otherwise it is skipped:

```sh
ENTITLEMENT_TEST_DATABASE_URL='postgres://127.0.0.1:15432/entitlements_test?sslmode=disable' go test ./...
```

## Endpoints

`GET /healthz` returns HTTP 200 with `{"status":"ok"}` without touching the database. `GET /readyz` checks PostgreSQL and returns HTTP 200 with `{"status":"ready"}`, or HTTP 503 with `{"status":"unavailable"}`.

### Create grant

`POST /tenants/{tenant}/entitlements`

```json
{"grantId":"g1","feature":"seats","amount":5,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-12-31T23:59:59Z"}
```

- `grantId` is the idempotency key, unique per tenant. `feature` is `seats` or `quota`; `amount` must be a positive JSON integer; `effectiveAt`/`expiresAt` are RFC 3339 timestamps and the validity interval is half-open `[effectiveAt, expiresAt)`, so `expiresAt` must be strictly later than `effectiveAt`.
- First successful creation returns **201**.
- Resubmitting the same `grantId` with the identical four content fields (`feature`, `amount`, `effectiveAt`, `expiresAt` — compared as instants, so the same timestamp in another offset matches) is an idempotent retry: returns **200** with the original record and does not insert again.
- Same `grantId` with any different field returns **409** and changes nothing.
- Response body: `tenant`, `grantId`, `feature`, `amount`, `effectiveAt`, `expiresAt`, `state`. Timestamps are echoed normalized to RFC 3339 UTC with second precision and a trailing `Z`; `state` is `active`.

### List active grants

`GET /tenants/{tenant}/entitlements?at=<RFC3339>` returns:

```json
{"tenant":"acme","asOf":"2026-06-01T00:00:00Z","grants":[…]}
```

`asOf` is normalized like request timestamps. Each grant element carries the six fields above and is `active`. Only active grants whose interval contains `asOf` (`effectiveAt <= at < expiresAt`) appear; cancelled grants never appear, for any `asOf`. An empty result returns `"grants":[]`.

### Cancel grant

`POST /tenants/{tenant}/entitlements/{grantId}/cancel` with `{"cancelledAt":"<RFC3339>"}` sets `state` to `cancelled`, records the cancellation time, and returns the updated grant (**200**). The grant then disappears from every list response. An unknown `grantId` returns **404** without changing state.

### Errors and routing

Validation failures (missing fields, non-positive or non-integer `amount`, bad timestamp format, inverted interval) return **400** with `{"error":"…"}`. Unmatched routes return **404**; a known path with another method returns **405**. Database write failures return **500** with no partial data and without leaking internal error text. Responses are `application/json` with `Cache-Control: no-store`.

`DATABASE_URL` is required; `LISTEN_ADDR` defaults to `127.0.0.1:8080`. Stop the server with Ctrl+C and the local database with `pg_ctl -D .local/pgdata -m fast -w stop`.

The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies.
