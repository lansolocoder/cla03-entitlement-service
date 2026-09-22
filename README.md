# Entitlement service

Go HTTP service for team service entitlements: per-tenant quota pools with
reservations, plus health endpoints. Requires Go 1.27.1 and PostgreSQL 18.6.
Schema tables are created automatically on startup.

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

## Multi-tenant quota pools and reservations

Tenants and pools are path parameters; a pool is only visible within its
tenant. All POST endpoints require an `Idempotency-Key` header.

### Create or conflict a quota pool

`PUT /v1/tenants/{tenantId}/quota-pools/{poolId}`

```json
{"limit": 100, "validFrom": "2026-01-01T00:00:00Z", "validUntil": "2027-01-01T00:00:00Z"}
```

`limit` must be a positive integer; both timestamps must be RFC3339 and
`validFrom` must be earlier than `validUntil`. Returns `201` with the pool on
creation and `409` if the pool already exists (a pool is never overwritten).

### Create a reservation

`POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations` (requires
`Idempotency-Key`)

```json
{"reservationId": "r-1", "amount": 10, "expiresAt": "2026-06-01T00:00:00Z"}
```

`amount` must be a positive integer. The pool must be currently valid
(`validFrom <= now < validUntil`), `expiresAt` must be in the future and no
later than the pool's `validUntil`, and enough quota must be available.
Available quota is the pool limit minus the amounts of unexpired `pending`
reservations and all `confirmed` reservations. On success a `pending`
reservation is created atomically and `201` returned. Any failed condition
returns `409`. Past-deadline `pending` reservations lazily become `expired`
and release their quota on the next reservation, decision, or fetch.

An optional `teamId` attributes the reservation to a team:

```json
{"reservationId": "r-1", "teamId": "team-1", "amount": 10, "expiresAt": "2026-06-01T00:00:00Z"}
```

The team must already have an allocation in the pool, and both the pool and
the team balance (allocated minus the team's unexpired `pending` and all
`confirmed` reservations) must cover the amount; otherwise `409`. The create
response and the fetched record include `teamId`; reservations without one
are unchanged and omit the field. Releasing or expiring a team reservation
restores the team balance; confirming keeps it occupied.

### Set a team allocation

`POST /v1/tenants/{tenantId}/quota-pools/{poolId}/teams/{teamId}/allocation`
(requires `Idempotency-Key`)

```json
{"amount": 20, "expectedVersion": 0}
```

`amount` must be a non-negative integer and `expectedVersion` the version the
caller last observed (a team starts at allocated `0`, version `0`). On a
version match the allocation is set to `amount`, the version increments, and
`200` is returned with `teamId`, `allocated`, `used`, `available` and
`version`. The sum of all team allocations in a pool may not exceed the pool
limit, and an allocation may not be lowered below the team's unexpired
`pending` plus `confirmed` reservations; violations and version mismatches
return `409` and change nothing.

`GET /v1/tenants/{tenantId}/quota-pools/{poolId}/teams/{teamId}/allocation`
returns the same summary with `200`. A missing team, missing pool, or a team
of another tenant all return `404`.

### Confirm or release a reservation

`POST .../reservations/{reservationId}/confirm` and
`POST .../reservations/{reservationId}/release` (both require
`Idempotency-Key`; the `.../reservations/{reservationId}:confirm` and
`:release` forms are also accepted). Only an unexpired `pending` reservation
can be confirmed or released; any other state returns `409`. Concurrent
confirm and release are serialized so at most one takes effect. Both return
the updated record with `200`.

### Fetch a reservation

`GET /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{reservationId}`
returns the full record with `200` (including `teamId` when the reservation
was attributed to a team). A missing reservation or one owned by another
tenant both return `404`.

### Idempotency, errors and durability

For POSTs, idempotency is scoped to `(tenant, Idempotency-Key)` and is checked
before any business validation. Replaying the same key with the identical
request returns `200` and the original result (including an originally `409`
business outcome, which replays as `200` with the original body); reusing the
same key with different request content returns `409`. Concurrent identical
requests create exactly one reservation. Quota checks and writes run in a
single row-locked transaction so quota can never be oversubscribed; team
allocations and team-attributed reservations serialize on the same pool lock,
so neither the pool nor the team layer can be exceeded even when allocations,
reservations, decisions and expiry cleanup run concurrently.

Malformed or invalid requests return `400` and write nothing. Storage failures
return a generic `503` (`{"error":"service temporarily unavailable"}`) and
never leak internal details, which are logged server-side instead. All data is
stored in PostgreSQL and survives process restarts.

The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies. Integration tests use `TEST_DATABASE_URL` (default `postgres://127.0.0.1:15432/entitlements_test?sslmode=disable`) and skip automatically when no database is reachable:

```sh
createdb -h 127.0.0.1 -p 15432 entitlements_test
go test ./...
```
