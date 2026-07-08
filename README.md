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
- Tenant isolation via `tenant_id` + Row-Level Security on every table.

## Quickstart

```bash
make up        # start Timescale + Redis
make migrate   # apply db/migrations/*.sql
make psql      # open a psql shell
make run-api   # start the HTTP API on :8080
make swagger   # regenerate the OpenAPI spec into docs/
```

## API docs (Swagger)

The HTTP API is documented with OpenAPI, generated from
[swaggo](https://github.com/swaggo/swag) annotations on the handlers.

- Swagger UI: <http://localhost:8080/swagger/index.html>
- Spec: `docs/swagger.json` / `docs/swagger.yaml` (regenerate with `make swagger`)

Add annotations to new handlers and rerun `make swagger` to keep the spec in
sync with the code.

## Build phases

- [x] **Phase 0** — infra, core schema, RLS, canonical event.
- [ ] **Phase 1** — ingest core: writer (Timescale history + Redis live state).
- [ ] **Phase 2** — Teltonika TCP gateway (Codec 8) + device simulator.
- [ ] **Phase 3** — Safetrucks REST adapter.
- [ ] **Phase 4** — REST API + WebSocket (vehicles, live positions).
- [ ] **Phase 5** — React/MapLibre live map + vehicle list.
- [ ] **Migration** — one-time ETL of 200 GB historical data.
