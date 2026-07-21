-- 0022_mobile_telemetry.sql — telemetry from phones (K-Trip app), keyed by driver.
--
-- Why a separate pair of tables rather than reusing telemetry/latest:
-- everything on the GPS side is keyed by IMEI (latest PK, /v1/history/{imei},
-- geofence check, the coverage and vehicle-day rollups). A phone has no IMEI —
-- its identity is the driver ObjectId. Mixing driver ids into the imei column
-- would silently pollute that identity space and skew the signal map (phones
-- have no device_generation row and a different radio). Separate tables keep
-- the tracker analytics honest for free.
--
-- Shape mirrors telemetry/latest (0008/0011 + kota 0012 + kelurahan/jalan 0014)
-- minus `ignition` (phones have none) and with driver in place of imei, so the
-- ingest → hot → cold Parquet flow is identical to the GPS side.

CREATE TABLE mobile_telemetry (
  time        timestamptz NOT NULL,          -- device fix time (partition key)
  received_at timestamptz,                   -- stamped by us on ingest
  driver      text        NOT NULL,          -- driver ObjectId (identity; no IMEI)
  geom        geography(Point, 4326),
  bearing     real,
  speed       real,
  kecamatan   text,
  kelurahan   text,
  kabupaten   text,
  kota        text,
  provinsi    text,
  jalan       text,
  metadata    jsonb                          -- altitude, battery, accuracy, status, io, source
);

SELECT create_hypertable('mobile_telemetry', by_range('time', INTERVAL '1 day'));

CREATE INDEX idx_mob_driver ON mobile_telemetry (driver, time DESC);
CREATE INDEX idx_mob_geom   ON mobile_telemetry USING gist (geom, time);

-- Same storage policy as telemetry (0015): recent day uncompressed for fast
-- writes, everything older ~90% smaller but still queryable.
ALTER TABLE mobile_telemetry SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'driver',
  timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('mobile_telemetry', INTERVAL '1 day');

-- NOTE: no retention policy here on purpose. A drop policy without a cold
-- archive silently destroys history — the trap we avoided on telemetry. It is
-- added in 0023 only after the archiver has written mobile Parquet to S3.

-- Current state per driver: one row, advanced only when a NEWER fix arrives so
-- buffered/late pings cannot overwrite a fresher position (mirrors `latest`).
CREATE TABLE mobile_latest (
  driver      text PRIMARY KEY,
  time        timestamptz NOT NULL,
  received_at timestamptz,
  geom        geography(Point, 4326),
  bearing     real,
  speed       real,
  kecamatan   text,
  kelurahan   text,
  kabupaten   text,
  kota        text,
  provinsi    text,
  jalan       text,
  metadata    jsonb
);

CREATE INDEX idx_mobile_latest_geom ON mobile_latest USING gist (geom);
