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

Tracking-first (the vehicle/asset registry was removed for now — see `0007`):

- `telemetry` — Timescale hypertable, keyed by `imei`. Required core columns:
  `time` (dateGps — device fix time), `received_at` (dateCreated), `geom`
  (lat/long), `bearing`, `speed`, `ignition`; plus `kecamatan/kabupaten/provinsi`
  (reverse-geocoded at ingest) and `metadata` jsonb (altitude, satellites, full
  vendor `io`, source, raw payload).
- `kecamatan` — Indonesia district boundary polygons (reverse-geocode reference
  data). See [scripts/geocode/](scripts/geocode/).
- `geofences` — PostGIS polygons.
- `tenants` — retained for future multi-tenancy (currently unused by telemetry).

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

### Dashcam clips (ingest, `/v1/dashcams/...`)

The ingest service fronts the Gtrack VSS camera platform and copies clips to S3.

| Env | Default | Purpose |
|---|---|---|
| `VSS_BASE_URL`, `VSS_USERNAME`, `VSS_PASSWORD` | empty (endpoints answer 503) | Camera platform login |
| `VIDEO_S3_BUCKET` | `$S3_BUCKET`, else `karlo-fms-cold-storage` | Default bucket for clips |
| `VIDEO_S3_BUCKETS` | empty | Comma-separated extra buckets a download may name (a customer's own bucket, e.g. `sibasuryastorage`). Any other bucket gets 400. |

A download with `alarm_id` copies exactly that event's uploaded clip for the
channel, found in `dashcam_alarms` (or one camera-platform search if the stored
link fails). It never pulls from the SD card.

**Local loop without the real camera platform** (its logins are rate-limited and
shared with production):

```bash
make up                 # db + redis + LocalStack S3 (bucket karlo-fms-local-video)
make fake-vss           # fake camera platform on :8090 (FAKE-CAM-1, FAKE-CAM-2)
DATABASE_URL=postgres://postgres:postgres@localhost:5436/fms INGEST_API_KEY=local-dev-key \
VSS_BASE_URL=http://localhost:8090 VSS_USERNAME=x VSS_PASSWORD=x \
VIDEO_S3_BUCKET=karlo-fms-local-video VIDEO_S3_BUCKETS=karlo-fms-local-video \
AWS_ENDPOINT_URL_S3=http://s3.localhost.localstack.cloud:4566 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=ap-southeast-1 \
make run-ingest
```

fms-backend calls the dashcam endpoints with that key (`TELEMETRY_API_KEY`).
`dashcam_alarms` fills within ~90 s. The fake's SD-card endpoint always answers
"no data" and logs `SD-CARD PULL`, so an accidental SD pull shows up.

## Build phases

- [x] **Phase 0** — infra, core schema, RLS, canonical event.
- [x] **Geocode** — kecamatan master data + reverse-geocode service.
- [ ] **Phase 1** — ingest core: writer (Timescale history + Redis live state).
- [ ] **Phase 2** — Teltonika TCP gateway (Codec 8) + device simulator.
- [ ] **Phase 3** — Safetrucks REST adapter.
- [ ] **Phase 4** — REST API + WebSocket (vehicles, live positions).
- [ ] **Phase 5** — React/MapLibre live map + vehicle list.
- [ ] **Migration** — one-time ETL of 200 GB historical data.
