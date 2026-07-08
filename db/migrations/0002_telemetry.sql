-- 0002_telemetry.sql — the write-heavy path: a Timescale hypertable.
-- vehicle_id is DENORMALIZED here (resolved from device_assignments at write
-- time) so history/playback queries never need a temporal join.

CREATE TABLE telemetry (
  time        timestamptz NOT NULL,
  tenant_id   bigint      NOT NULL,
  device_id   bigint      NOT NULL,
  vehicle_id  bigint,                    -- NULL if the device is unassigned at ping time
  source      text        NOT NULL,
  geom        geography(Point, 4326),
  speed_kmh   real,
  heading     real,
  ignition    boolean,
  altitude_m  real,
  satellites  smallint,
  attrs       jsonb                       -- vendor IO elements (fuel, temp, custom IDs)
);

-- Partition into 1-day chunks.
SELECT create_hypertable('telemetry', by_range('time', INTERVAL '1 day'));

-- Query patterns: "one vehicle's track over a window", "by device", "spatial".
CREATE INDEX idx_tel_vehicle ON telemetry (tenant_id, vehicle_id, time DESC);
CREATE INDEX idx_tel_device  ON telemetry (device_id, time DESC);
CREATE INDEX idx_tel_geom    ON telemetry USING gist (geom, time);

-- Compress chunks older than 7 days (typically 90%+ on telemetry).
ALTER TABLE telemetry SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'tenant_id, vehicle_id',
  timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('telemetry', INTERVAL '7 days');

-- Keep 90 days of raw pings in Postgres; older data lives in the S3/Parquet
-- archive (added later). Comment out until the archive sink exists.
-- SELECT add_retention_policy('telemetry', INTERVAL '90 days');
