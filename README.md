# Entitlement service

Go HTTP service for team service entitlements. It manages the full lifecycle of seat and quota grants — creation, member allocation, idempotent consumption, cancellation and expiry — on top of PostgreSQL. Requires Go 1.27.1 and PostgreSQL 18.6.

```sh
go mod download
go test ./...
mkdir -p .local
initdb -D .local/pgdata --auth=trust --encoding=UTF8 --locale=C
pg_ctl -D .local/pgdata -l .local/postgres.log -o '-h 127.0.0.1 -p 15432' -w start
createdb -h 127.0.0.1 -p 15432 entitlements
DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/server
```

The schema (`entitlements`, `allocations`, `usages`) is created automatically on startup. Stop the server with Ctrl+C and the local database with `pg_ctl -D .local/pgdata -m fast -w stop`.

## Health

- `GET /healthz` → 200 `{"status":"ok"}`
- `GET /readyz` → 200 `{"status":"ready"}` when PostgreSQL is reachable, otherwise 503 `{"status":"unavailable"}`. Database errors never leak into responses.
- Unknown paths return 404.

## Entitlement API

All request and response bodies are JSON. Timestamps are RFC3339 UTC strings (e.g. `2027-01-01T00:00:00Z`); non-UTC offsets are rejected.

### Create an entitlement

`POST /entitlements`

```json
{"teamId":"team-1","type":"seat","total":5,"expiresAt":"2027-01-01T00:00:00Z"}
```

- `type` is `seat` or `quota`; `total` is a positive integer; `expiresAt` must be in the future.
- 201 returns the record: `id`, `teamId`, `type`, `total`, `status` (`active`), `expiresAt`, `used` (`0`), `version` (`1`).
- Recreating the same `teamId` + `type` + `expiresAt` is a 409 and the response includes the already-existing record under `entitlement`.

### Allocate seats or quota to a member

`POST /entitlements/{id}/allocations`

```json
{"memberId":"member-1","quantity":2}
```

- 200 returns the updated record: `used` increases by `quantity`, `version` increments. Repeated allocations for the same member accumulate.
- 409 when `used + quantity` would exceed `total`, or when the entitlement is cancelled/expired. The record is unchanged on failure.

### Use an entitlement

`POST /entitlements/{id}/usages`

```json
{"memberId":"member-1","quantity":1,"usageKey":"order-123"}
```

- The member must have enough *unused* allocation (`allocated - previously consumed`); otherwise 409 with `used` unchanged.
- `usageKey` is unique within an entitlement. Repeating a request with the same key and the same parameters replays the **first** response (including its `used`/`version` snapshot, flagged with `"replayed":true`) and does not accumulate again. The same key with different `memberId`/`quantity` is a 409 carrying the first usage under `existingUsage`.
- First-time success increments `used` and `version`.

### Cancel an entitlement

`POST /entitlements/{id}/cancel`

- 200 returns the record with `status=cancelled`. Already-used quantities are preserved for history. Cancelling again, or allocating/using after cancellation, is a 409.

### Read history and detail

`GET /entitlements/{id}` returns the entitlement plus a `members` array grouped by `memberId`, each with the consumed `used` total and the ordered `usageKeys` list. Expired and cancelled entitlements remain readable.

### Expiry

Expiry is derived from `expiresAt` on every read using UTC comparison — no background job is needed. Once expired, `status` is reported as `expired` and allocations and usages are rejected with 409; reads continue to work.

### Status and error codes

| HTTP | When |
| --- | --- |
| 200 | Allocation, usage, cancel, detail |
| 201 | Entitlement created |
| 400 | Missing/malformed fields, non-UTC or past `expiresAt` |
| 404 | Unknown entitlement id or unknown path |
| 409 | Duplicate create, over-total allocation, over-remaining usage, reused key, cancelled/expired state |

Errors have the shape `{"error":{"code":"...","message":"..."}}`. Every failed request is rolled back: no entitlement, allocation or usage record is changed by a request that returns an error.

### Concurrency

Each mutation runs in a transaction that takes the entitlement row lock first, so concurrent allocations/usages against the same entitlement are serialized: total capacity is never oversold, and concurrent requests sharing one `usageKey` result in exactly one first usage and the rest replays.

## Configuration

`DATABASE_URL` is required; `LISTEN_ADDR` defaults to `127.0.0.1:8080`. The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies.

## Tests

```sh
go test ./...                                                       # unit tests (no database needed)
TEST_DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' go test ./...  # + PostgreSQL integration tests
```

The integration suite covers the full lifecycle, exact idempotent replay snapshots, and high-concurrency allocation/usage races.
