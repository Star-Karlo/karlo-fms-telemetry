-- db/rds/schema.sql — the telemetry database on RDS Postgres, WITHOUT Timescale.
--
-- Generated from the live Tiger Cloud catalog on 2026-09-15 and then edited by
-- hand where noted. Tiger ran PostgreSQL 18 + TimescaleDB 2.29; this targets
-- RDS PostgreSQL 16 + PostGIS. The only Timescale features FMS used were
-- daily-chunked hypertables, a compression policy and a retention policy:
--   * hypertables  -> native RANGE partitioning by day (below)
--   * compression  -> none; the hot tier is 7 days, ~15 GB uncompressed, and
--                     gp3 storage costs less than the SaaS did
--   * retention    -> partition_maintenance() (bottom), run nightly by the
--                     archiver AFTER it has written the day to cold storage,
--                     which is the same "never drop an unarchived day" order
--                     the two Timescale policies had by their 8d-vs-7d offset.
--
-- Everything else — PostGIS geography, the geocoding reference tables, the
-- daily rollup, coverage grids, driving events — is plain Postgres and
-- carries over unchanged.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS coverage_grid (
  cell_lat double precision NOT NULL,
  cell_lon double precision NOT NULL,
  pings bigint NOT NULL DEFAULT 0,
  sig_sum double precision NOT NULL DEFAULT 0,
  sig_cnt bigint NOT NULL DEFAULT 0,
  tsel_sum double precision NOT NULL DEFAULT 0,
  tsel_cnt bigint NOT NULL DEFAULT 0,
  isat_sum double precision NOT NULL DEFAULT 0,
  isat_cnt bigint NOT NULL DEFAULT 0,
  xl_sum double precision NOT NULL DEFAULT 0,
  xl_cnt bigint NOT NULL DEFAULT 0,
  weak bigint NOT NULL DEFAULT 0,
  rug_num double precision NOT NULL DEFAULT 0,
  rug_den double precision NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  alt_sum double precision NOT NULL DEFAULT 0,
  alt_cnt bigint NOT NULL DEFAULT 0,
  lte_seen bigint NOT NULL DEFAULT 0,
  lte_on bigint NOT NULL DEFAULT 0,
  CONSTRAINT coverage_grid_pkey PRIMARY KEY (cell_lat, cell_lon)
);

CREATE TABLE IF NOT EXISTS coverage_rollup_log (
  day date NOT NULL,
  pings bigint NOT NULL,
  cells bigint NOT NULL,
  done_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT coverage_rollup_log_pkey PRIMARY KEY (day)
);

CREATE TABLE IF NOT EXISTS coverage_signal (
  cell_lat double precision NOT NULL,
  cell_lon double precision NOT NULL,
  gen text NOT NULL,
  provider text NOT NULL,
  sig_sum double precision NOT NULL DEFAULT 0,
  sig_cnt bigint NOT NULL DEFAULT 0,
  weak bigint NOT NULL DEFAULT 0,
  pings bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT coverage_signal_gen_check CHECK ((gen = ANY (ARRAY['2g'::text, '4g'::text]))),
  CONSTRAINT coverage_signal_pkey PRIMARY KEY (cell_lat, cell_lon, gen, provider),
  CONSTRAINT coverage_signal_provider_check CHECK ((provider = ANY (ARRAY['tsel'::text, 'isat'::text, 'xl'::text, 'unk'::text])))
);

CREATE TABLE IF NOT EXISTS dashcam_alarms (
  id bigint NOT NULL,
  device_id text NOT NULL,
  alarm_id text,
  code integer NOT NULL,
  occurred_at timestamptz NOT NULL,
  lat double precision,
  lon double precision,
  speed double precision,
  raw jsonb,
  received_at timestamptz NOT NULL DEFAULT now(),
  gforce double precision,
  CONSTRAINT dashcam_alarms_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS device_generation (
  imei text NOT NULL,
  generation text NOT NULL,
  CONSTRAINT device_generation_generation_check CHECK ((generation = ANY (ARRAY['2g'::text, '4g'::text]))),
  CONSTRAINT device_generation_pkey PRIMARY KEY (imei)
);

CREATE TABLE IF NOT EXISTS geofences (
  id bigint NOT NULL,
  tenant_id bigint NOT NULL,
  name text NOT NULL,
  geom geography(Polygon,4326) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT geofences_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS green_driving_events (
  id bigint NOT NULL,
  imei text NOT NULL,
  event_type integer NOT NULL,
  value double precision,
  duration_ms integer,
  occurred_at timestamptz NOT NULL,
  lat double precision,
  lon double precision,
  speed double precision,
  raw jsonb,
  received_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT green_driving_events_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS ingest_key (
  id bigint NOT NULL,
  name text NOT NULL,
  key_prefix text NOT NULL,
  key_hash bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz,
  revoked_at timestamptz,
  CONSTRAINT ingest_key_key_hash_key UNIQUE (key_hash),
  CONSTRAINT ingest_key_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS kelurahan (
  id bigint NOT NULL,
  kel_code text,
  kel_name text NOT NULL,
  kec_code text,
  kec_name text,
  kab_code text,
  kab_name text,
  prov_code text,
  prov_name text,
  geom geometry(Multipolygon,4326) NOT NULL,
  CONSTRAINT kelurahan_kel_code_key UNIQUE (kel_code),
  CONSTRAINT kelurahan_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS latest (
  imei text NOT NULL,
  time timestamptz NOT NULL,
  received_at timestamptz,
  geom geography(Point,4326),
  bearing real,
  speed real,
  ignition boolean,
  kecamatan text,
  kabupaten text,
  provinsi text,
  metadata jsonb,
  kota text,
  kelurahan text,
  jalan text,
  CONSTRAINT latest_pkey PRIMARY KEY (imei)
);

CREATE TABLE IF NOT EXISTS mobile_latest (
  driver text NOT NULL,
  time timestamptz NOT NULL,
  received_at timestamptz,
  geom geography(Point,4326),
  bearing real,
  speed real,
  kecamatan text,
  kelurahan text,
  kabupaten text,
  kota text,
  provinsi text,
  jalan text,
  metadata jsonb,
  CONSTRAINT mobile_latest_pkey PRIMARY KEY (driver)
);

CREATE TABLE IF NOT EXISTS pg_buffercache (
  bufferid integer,
  relfilenode oid,
  reltablespace oid,
  reldatabase oid,
  relforknumber smallint,
  relblocknumber bigint,
  isdirty boolean,
  usagecount smallint,
  pinning_backends integer
);

CREATE TABLE IF NOT EXISTS pg_buffercache_numa (
  bufferid integer,
  os_page_num bigint,
  numa_node integer
);

CREATE TABLE IF NOT EXISTS pg_stat_statements (
  userid oid,
  dbid oid,
  toplevel boolean,
  queryid bigint,
  query text,
  plans bigint,
  total_plan_time double precision,
  min_plan_time double precision,
  max_plan_time double precision,
  mean_plan_time double precision,
  stddev_plan_time double precision,
  calls bigint,
  total_exec_time double precision,
  min_exec_time double precision,
  max_exec_time double precision,
  mean_exec_time double precision,
  stddev_exec_time double precision,
  rows bigint,
  shared_blks_hit bigint,
  shared_blks_read bigint,
  shared_blks_dirtied bigint,
  shared_blks_written bigint,
  local_blks_hit bigint,
  local_blks_read bigint,
  local_blks_dirtied bigint,
  local_blks_written bigint,
  temp_blks_read bigint,
  temp_blks_written bigint,
  shared_blk_read_time double precision,
  shared_blk_write_time double precision,
  local_blk_read_time double precision,
  local_blk_write_time double precision,
  temp_blk_read_time double precision,
  temp_blk_write_time double precision,
  wal_records bigint,
  wal_fpi bigint,
  wal_bytes numeric,
  wal_buffers_full bigint,
  jit_functions bigint,
  jit_generation_time double precision,
  jit_inlining_count bigint,
  jit_inlining_time double precision,
  jit_optimization_count bigint,
  jit_optimization_time double precision,
  jit_emission_count bigint,
  jit_emission_time double precision,
  jit_deform_count bigint,
  jit_deform_time double precision,
  parallel_workers_to_launch bigint,
  parallel_workers_launched bigint,
  stats_since timestamptz,
  minmax_stats_since timestamptz
);

CREATE TABLE IF NOT EXISTS pg_stat_statements_info (
  dealloc bigint,
  stats_reset timestamptz
);

CREATE TABLE IF NOT EXISTS road (
  id bigint NOT NULL,
  osm_id text,
  name text NOT NULL,
  highway text,
  geom geometry(Linestring,4326) NOT NULL,
  CONSTRAINT road_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tenants (
  id bigint NOT NULL,
  name text NOT NULL,
  slug text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tenants_pkey PRIMARY KEY (id),
  CONSTRAINT tenants_slug_key UNIQUE (slug)
);

CREATE TABLE IF NOT EXISTS vehicle_day (
  imei text NOT NULL,
  day date NOT NULL,
  moving_s bigint NOT NULL DEFAULT 0,
  idle_s bigint NOT NULL DEFAULT 0,
  parking_s bigint NOT NULL DEFAULT 0,
  distance_km numeric NOT NULL DEFAULT 0,
  max_speed numeric,
  samples bigint NOT NULL DEFAULT 0,
  first_seen timestamptz,
  last_seen timestamptz,
  final boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT vehicle_day_pkey PRIMARY KEY (imei, day)
);

-- Was a hypertable (daily chunks). Partitions are created ahead by partition_maintenance().
CREATE TABLE IF NOT EXISTS mobile_telemetry (
  time timestamptz NOT NULL,
  received_at timestamptz,
  driver text NOT NULL,
  geom geography(Point,4326),
  bearing real,
  speed real,
  kecamatan text,
  kelurahan text,
  kabupaten text,
  kota text,
  provinsi text,
  jalan text,
  metadata jsonb
) PARTITION BY RANGE ("time");

-- Was a hypertable (daily chunks). Partitions are created ahead by partition_maintenance().
CREATE TABLE IF NOT EXISTS telemetry (
  time timestamptz NOT NULL,
  received_at timestamptz,
  imei text NOT NULL,
  geom geography(Point,4326),
  bearing real,
  speed real,
  ignition boolean,
  kecamatan text,
  kabupaten text,
  provinsi text,
  metadata jsonb,
  kota text,
  kelurahan text,
  jalan text
) PARTITION BY RANGE ("time");

-- Foreign keys, after every table exists.
ALTER TABLE geofences ADD CONSTRAINT geofences_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tenants(id);

-- Indexes, verbatim. On a partitioned parent Postgres propagates them to every partition.
CREATE UNIQUE INDEX IF NOT EXISTS dashcam_alarms_alarm_id_uk ON public.dashcam_alarms USING btree (alarm_id) WHERE (alarm_id IS NOT NULL);
CREATE INDEX IF NOT EXISTS dashcam_alarms_device_idx ON public.dashcam_alarms USING btree (device_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS dashcam_alarms_id_idx ON public.dashcam_alarms USING btree (id);
CREATE INDEX IF NOT EXISTS idx_geofences_geom ON public.geofences USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_geofences_tenant ON public.geofences USING btree (tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS green_driving_events_dedup ON public.green_driving_events USING btree (imei, occurred_at, event_type);
CREATE INDEX IF NOT EXISTS green_driving_events_id_idx ON public.green_driving_events USING btree (id);
CREATE INDEX IF NOT EXISTS idx_ingest_key_active ON public.ingest_key USING btree (key_hash) WHERE (revoked_at IS NULL);
CREATE INDEX IF NOT EXISTS idx_kelurahan_geom ON public.kelurahan USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_kelurahan_prov ON public.kelurahan USING btree (prov_code);
CREATE INDEX IF NOT EXISTS idx_latest_geom ON public.latest USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_mobile_latest_geom ON public.mobile_latest USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_road_geom ON public.road USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_road_name ON public.road USING btree (name);
CREATE INDEX IF NOT EXISTS idx_vehicle_day_day ON public.vehicle_day USING btree (day DESC, imei);
CREATE INDEX IF NOT EXISTS idx_mob_driver ON public.mobile_telemetry USING btree (driver, "time" DESC);
CREATE INDEX IF NOT EXISTS idx_mob_geom ON public.mobile_telemetry USING gist (geom, "time");
CREATE INDEX IF NOT EXISTS mobile_telemetry_time_idx ON public.mobile_telemetry USING btree ("time" DESC);
CREATE INDEX IF NOT EXISTS idx_tel_geom ON public.telemetry USING gist (geom, "time");
CREATE INDEX IF NOT EXISTS idx_tel_imei ON public.telemetry USING btree (imei, "time" DESC);
CREATE INDEX IF NOT EXISTS telemetry_time_idx ON public.telemetry USING btree ("time" DESC);

-- ---------------------------------------------------------------------------
-- Partition maintenance — the retention policy, made explicit.
--
-- Creates daily partitions for the next `ahead` days so an insert never
-- lands on a missing partition, and drops partitions whose day is older than
-- `keep` days. Idempotent. The archiver calls it nightly after archiving.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION partition_maintenance(tbl text, keep_days int, ahead_days int DEFAULT 3)
RETURNS TABLE(action text, partition_name text) LANGUAGE plpgsql AS $$
DECLARE d date; p text; oldest date := current_date - keep_days; r record;
BEGIN
  FOR d IN SELECT generate_series(current_date - 1, current_date + ahead_days, '1 day')::date LOOP
    p := format('%s_%s', tbl, to_char(d, 'YYYYMMDD'));
    IF to_regclass(p) IS NULL THEN
      EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)', p, tbl, d, d + 1);
      action := 'created'; partition_name := p; RETURN NEXT;
    END IF;
  END LOOP;
  FOR r IN
    SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
     WHERE i.inhparent = tbl::regclass AND c.relname ~ (tbl || '_\d{8}$')
       AND to_date(substring(c.relname from '\d{8}$'), 'YYYYMMDD') < oldest
  LOOP
    EXECUTE format('DROP TABLE %I', r.relname);
    action := 'dropped'; partition_name := r.relname; RETURN NEXT;
  END LOOP;
END $$;

-- First partitions, so the schema is usable the moment it is applied.
SELECT * FROM partition_maintenance('telemetry', 8, 3);
SELECT * FROM partition_maintenance('mobile_telemetry', 8, 3);
