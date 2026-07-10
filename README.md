# Karlo FMS

Fleet Management System for ~3,000 vehicles across two telemetry sources:

- **Teltonika** (~2,000 units) — Codec 8/8E binary over a persistent **TCP** socket.
- **Safetrucks** (~1,000 units) — pulled/pushed via **REST** from their server.

Both sources normalize into one canonical `TelemetryEvent`
([internal/model/telemetry.go](internal/model/telemetry.go)); everything
downstream is source-agnostic.

## Scale

~3,000 vehicles × 1 ping / 90s ≈ **33 msg/s**, ~2.9M rows/day. A single-node
problem — no Kinesis, no IoT Core, no Kubernetes (those are documented upgrade
paths, not day-1).

## Stack

| Layer            | Choice                                             |
|------------------|----------------------------------------------------|
| Ingest + API/WS  | Go                                                 |
| Database         | PostgreSQL 16 + TimescaleDB + PostGIS              |
| Live state / bus | Redis 7                                            |
| Frontend         | React + TypeScript + Vite, MapLibre GL             |
| Local dev        | Docker Compose                                     |

## Data model

- `vehicles` — the asset (truck).
- `devices` — the sensor bolted to a truck (Teltonika IMEI / Safetrucks id).
- `device_assignments` — **temporal** device↔vehicle link (a box moves between
  trucks; telemetry is attributed to the vehicle it was on *at ping time*).
- `telemetry` — Timescale hypertable, write path; `vehicle_id` denormalized.
- `geofences` — PostGIS polygons.
- `kecamatan` — Indonesia district boundary polygons (reverse-geocode reference
  data; shared, not tenant-scoped). See [scripts/geocode/](scripts/geocode/).
- Tenant isolation via `tenant_id` + Row-Level Security on every tenant table.

## Quickstart

```bash
make up        # start Timescale + Redis
make migrate   # apply db/migrations/*.sql
make psql      # open a psql shell
make run-api   # start the HTTP API on :8080
make swagger   # regenerate the OpenAPI spec into docs/
make load-geocode  # load Indonesia kecamatan boundaries (reverse-geocode data)
```

## API docs (Swagger)

The HTTP API is documented with OpenAPI, generated from
[swaggo](https://github.com/swaggo/swag) annotations on the handlers.

- Swagger UI: <http://localhost:8080/swagger/index.html>
- Spec: `docs/swagger.json` / `docs/swagger.yaml` (regenerate with `make swagger`)

Add annotations to new handlers and rerun `make swagger` to keep the spec in
sync with the code.

## Services

Each service is a separate binary under `cmd/` (own port, own Swagger),
deployable as its own container via the shared `Dockerfile`
(`--build-arg SERVICE=<name>`).

| Service   | Port | Run (dev)         | Purpose                                        |
|-----------|------|-------------------|------------------------------------------------|
| `api`     | 8080 | `make run-api`    | REST API (vehicles, drivers, trips, …)         |
| `geocode` | 8081 | `make run-geocode`| Reverse geocode lat/lon → kecamatan/kota/prov  |

`make up` starts only infra (db + redis) for local `go run`. `make up-app`
builds + runs the app services in containers too (compose profile `app`).

### geocode

Stateless reverse-geocoder: geohash-keyed **Redis** cache in front of a PostGIS
`ST_Contains` lookup against the `kecamatan` table.

```bash
curl "http://localhost:8081/reverse?lat=-6.1751&lon=106.8272"
# {"lat":-6.1751,"lon":106.8272,"found":true,"cached":false,
#  "kecamatan":"Gambir","kabupaten":"Kota Jakarta Pusat",
#  "provinsi":"Dki Jakarta","kecamatan_code":"ID3173080"}
```

Env: `DATABASE_URL`, `REDIS_ADDR`, `GEOCODE_ADDR`, `GEOHASH_PRECISION` (7),
`CACHE_TTL_HOURS` (720). Swagger at <http://localhost:8081/swagger/index.html>.

## Build phases

- [x] **Phase 0** — infra, core schema, RLS, canonical event.
- [x] **Geocode** — kecamatan master data + reverse-geocode service.
- [ ] **Phase 1** — ingest core: writer (Timescale history + Redis live state).
- [ ] **Phase 2** — Teltonika TCP gateway (Codec 8) + device simulator.
- [ ] **Phase 3** — Safetrucks REST adapter.
- [ ] **Phase 4** — REST API + WebSocket (vehicles, live positions).
- [ ] **Phase 5** — React/MapLibre live map + vehicle list.
- [ ] **Migration** — one-time ETL of 200 GB historical data.
