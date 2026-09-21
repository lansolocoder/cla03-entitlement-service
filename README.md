# Entitlement service

Go HTTP service for multi-tenant quota pools and reservations. Requires Go 1.27.1 and PostgreSQL 18.6.

```sh
go mod download
go test ./...
mkdir -p .local
initdb -D .local/pgdata --auth=trust --encoding=UTF8 --locale=C
pg_ctl -D .local/pgdata -l .local/postgres.log -o '-h 127.0.0.1 -p 15432' -w start
createdb -h 127.0.0.1 -p 15432 entitlements
DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/server
```

Tables (`quota_pools`, `reservations`, `idempotency_records`) are created automatically at startup and data persists across restarts. `DATABASE_URL` is required; `LISTEN_ADDR` defaults to `127.0.0.1:8080`. Stop the server with Ctrl+C and the local database with `pg_ctl -D .local/pgdata -m fast -w stop`.

## Health

`GET /healthz` returns `200 {"status":"ok"}` without touching the database. `GET /readyz` checks PostgreSQL and returns `200 {"status":"ready"}`, or `503 {"status":"unavailable"}`. Other paths return 404.

## Quota pools

`PUT /v1/tenants/{tenantId}/quota-pools/{poolId}` creates a pool:

```json
{ "limit": 100, "validFrom": "2026-01-01T00:00:00Z", "validUntil": "2026-12-31T00:00:00Z" }
```

- `limit` must be a positive integer; both timestamps are RFC3339 and `validFrom` must be earlier than `validUntil`. Invalid requests return `400` and write nothing.
- Creation returns `201` with the stored pool; creating an existing `(tenantId, poolId)` again returns `409` and never modifies it.

## Reservations

`POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations`:

```json
{ "reservationId": "r-1", "amount": 4, "expiresAt": "2026-06-30T00:00:00Z" }
```

- Every POST requires an `Idempotency-Key` header (missing or oversized → `400`, before any business logic).
- `amount` must be a positive integer; the pool must be currently active; `expiresAt` must be after now and no later than the pool end.
- Available quota is the pool `limit` minus the summed `amount` of every unexpired `pending` and `confirmed` reservation. On success a `pending` reservation is created atomically and `201` returned. Any failed precondition (inactive/missing pool, bad expiry, insufficient quota, duplicate `reservationId`) returns `409`.
- Pending reservations past `expiresAt` are flipped to `expired`, releasing their held quota. This happens on the next reservation, confirm/release, GET, or via the per-minute background sweep.

`POST .../reservations/{reservationId}/confirm` marks an unexpired pending reservation `confirmed` (its quota stays held). `POST .../reservations/{reservationId}/release` marks it `released` (quota returns to the pool). Both return `200` with the updated record. Only an unexpired `pending` record is actionable; any other state returns `409`. Concurrent confirm and release are mutually exclusive — at most one takes effect. Both endpoints require `Idempotency-Key` and take an empty body (or `{}`).

`GET .../reservations/{reservationId}` returns the full record, expiring a stale pending row first. Unknown ids and cross-tenant access both return `404`.

## Idempotency

All POST endpoints are idempotent per `(tenantId, Idempotency-Key)`. The idempotency claim is taken before business validation:

- An identical replay (same method, path and canonical JSON body) returns `200` with the original result and status semantics — including the original body for a first-time `201` or `409`.
- The same key replayed with different content returns `409`.
- Concurrent same-key requests serialize on the claim: exactly one executes, the rest receive its result; concurrency can never oversubscribe a pool or double-apply confirm/release.

## Errors

Malformed or illegal requests return `400` and write nothing. Business conflicts return `409`; missing or cross-tenant reservation lookups return `404`; storage failures return `503` with a generic body — internal/database details are logged but never sent to clients.

The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies.
