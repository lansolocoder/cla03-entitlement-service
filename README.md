# Entitlement service

Go HTTP service for team service entitlements: seats and quotas with allocation, usage, cancellation and expiry. Requires Go 1.27.1 and PostgreSQL 18.6. The schema is created automatically on startup from `internal/store/schema.sql` (idempotent).

```sh
go mod download
go test ./...
mkdir -p .local
initdb -D .local/pgdata --auth=trust --encoding=UTF8 --locale=C
pg_ctl -D .local/pgdata -l .local/postgres.log -o '-h 127.0.0.1 -p 15432' -w start
createdb -h 127.0.0.1 -p 15432 entitlements
DATABASE_URL='postgres://127.0.0.1:15432/entitlements?sslmode=disable' LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/server
```

## Health

`GET /healthz` returns HTTP 200 with `{"status":"ok"}`. `GET /readyz` checks PostgreSQL and returns HTTP 200 with `{"status":"ready"}`, or HTTP 503 with `{"status":"unavailable"}`. Other paths return 404.

## Entitlement API

All request and response bodies are JSON. Timestamps are RFC3339 UTC. Successful writes return HTTP 201 (create) or 200; malformed/missing parameters return 400; business conflicts, capacity violations and operations on cancelled/expired entitlements return 409; unknown resources and paths return 404. Every error response is atomic — no entitlement, allocation or usage record changes.

The entitlement record has the shape:

```json
{"id":"…","teamId":"…","type":"seat|quota","total":10,"status":"active|cancelled|expired",
 "expiresAt":"2030-01-01T00:00:00Z","used":0,"version":1}
```

`status` is derived on every read and mutation: the stored status is `active` or `cancelled`, and a record whose `expiresAt` has passed is reported as `expired` automatically. Expired and cancelled records stay readable but can no longer be allocated or used.

| Operation | Method and path | Body |
| --- | --- | --- |
| Create | `POST /entitlements` | `{"teamId","type":"seat\|quota","total",10,"expiresAt":"…"}` |
| Allocate | `POST /entitlements/{id}/allocations` | `{"memberId","amount":4}` |
| Use | `POST /entitlements/{id}/usages` | `{"memberId","amount":3,"usageKey":"…"}` |
| Cancel | `POST /entitlements/{id}/cancel` | — |
| Details | `GET /entitlements/{id}` | — |

- **Create** returns 201 with `used=0`, `version=1`, `status=active`. A repeat with the same `teamId`, `type` and `expiresAt` returns 409 together with the already-existing record.
- **Allocate** increases `used` and bumps `version`. It is rejected (409, record unchanged) when the amount would exceed `total`, or when the entitlement is cancelled/expired. Repeated allocations for the same member accumulate.
- **Use** consumes a member's previously allocated allowance. `usageKey` is unique within an entitlement: repeating a request with the same key returns the first result verbatim (with the `Idempotency-Replayed: true` header) and never accumulates twice. Reusing a key with a different member or amount returns 409. Consuming more than the member's unused allowance returns 409 and leaves `used` unchanged. A key accepted before cancellation/expiry still replays afterwards.
- **Cancel** sets `status=cancelled` and preserves the used amount. Further allocations and new usages return 409; the record remains readable. Cancelling twice is idempotent.
- **Details** returns the entitlement record plus `members`, an array grouped by `memberId` with each member's `used` amount and `usageKeys` list.

Concurrency is serialized with a row lock (`SELECT … FOR UPDATE`) on the entitlement inside each mutation transaction, so parallel allocations cannot oversubscribe and parallel usages with the same key are recorded exactly once.

`DATABASE_URL` is required; `LISTEN_ADDR` defaults to `127.0.0.1:8080`. Integration tests connect to `TEST_DATABASE_URL` (falling back to `DATABASE_URL`) and skip when neither is set. Stop the server with Ctrl+C and the local database with `pg_ctl -D .local/pgdata -m fast -w stop`.

The local setup uses a trusted loopback connection and C collation without ICU. Use separate database directories, database ports and HTTP ports when running multiple copies.
