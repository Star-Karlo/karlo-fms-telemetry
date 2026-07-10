-- Reshape telemetry to the tracking schema: a small required core + metadata.
-- Self-contained (drops any prior asset tables + telemetry and rebuilds), so it
-- brings any earlier state to the final one. telemetry is empty → no data lost.
-- Keeps tenants + geofences.
DROP TABLE IF EXISTS device_assignments;
DROP TABLE IF EXISTS devices;
DROP TABLE IF EXISTS vehicles;
DROP TABLE IF EXISTS telemetry;

CREATE TABLE telemetry (
  time        timestamptz NOT NULL,          -- dateGps: device GPS fix time (partition key)
  received_at timestamptz,                   -- dateCreated: when our backend received it
  imei        text        NOT NULL,
  geom        geography(Point, 4326),        -- lat/long
  bearing     real,
  speed       real,
  ignition    boolean,
  kecamatan   text,
  kabupaten   text,
  provinsi    text,
  metadata    jsonb                          -- altitude, satellites, hdop, full io, source, _raw
);

SELECT create_hypertable('telemetry', by_range('time', INTERVAL '1 day'));

CREATE INDEX idx_tel_imei ON telemetry (imei, time DESC);
CREATE INDEX idx_tel_geom ON telemetry USING gist (geom, time);

-- Compress chunks older than 7 days, segmented by device.
ALTER TABLE telemetry SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'imei',
  timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('telemetry', INTERVAL '7 days');
-- Retention deferred until the S3 archive exists.
