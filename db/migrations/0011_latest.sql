-- 0011_latest.sql — current-state table: one row per device (imei), upserted on
-- ingest. Same shape as telemetry but PK on imei, a plain table (not a
-- hypertable). Lets consumers read "where is the whole fleet now" cheaply
-- (SELECT * FROM latest) instead of scanning the historical hypertable.
--
-- The ingest upsert only advances a row when the incoming GPS fix is newer than
-- the stored one, so a device dumping buffered (old) readings can't clobber the
-- true current position.

CREATE TABLE latest (
  imei        text PRIMARY KEY,
  time        timestamptz NOT NULL,          -- dateGps of the newest fix
  received_at timestamptz,
  geom        geography(Point, 4326),
  bearing     real,
  speed       real,
  ignition    boolean,
  kecamatan   text,
  kabupaten   text,
  provinsi    text,
  metadata    jsonb
);

CREATE INDEX idx_latest_geom ON latest USING gist (geom);
