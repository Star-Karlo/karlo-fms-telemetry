-- 0026_green_driving_events.sql — harsh-driving events from Teltonika trackers'
-- Green Driving feature, the raw-layer record FMS scores drivers on.
--
-- Same role as dashcam_alarms, but sourced from the GPS tracker instead of the
-- camera: when a unit reports a Green Driving event (AVL 253 type + 254 value),
-- the ingest handler writes a row here. FMS polls it, maps the IMEI to a vehicle
-- it owns, and materialises a tenant-scoped driving_events row (source
-- 'teltonika'). This is what lets a truck with only a tracker — no camera — feed
-- the harsh braking / acceleration / cornering score pillars.
--
-- Tenant-agnostic, keyed by IMEI, exactly like the telemetry hypertable.
-- Idempotent.

CREATE TABLE IF NOT EXISTS green_driving_events (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  imei        text NOT NULL,
  -- Teltonika Green Driving type: 1 harsh acceleration, 2 harsh braking,
  -- 3 harsh cornering (AVL 253).
  event_type  int NOT NULL,
  -- Raw Teltonika Green Driving Value (AVL 254), device units. For the
  -- accelerometer scenario this is g×100 (e.g. 45 = 0.45 g); FMS converts it to
  -- g. Kept raw here; the full payload is stored so the scale can be confirmed.
  value       double precision,
  duration_ms int,               -- AVL 243, event duration
  occurred_at timestamptz NOT NULL,
  lat         double precision,
  lon         double precision,
  speed       double precision,
  raw         jsonb,
  received_at timestamptz NOT NULL DEFAULT now()
);

-- One row per event. A tracker reports a given event once at a given fix time;
-- keying on (imei, occurred_at, type) drops a re-posted duplicate.
CREATE UNIQUE INDEX IF NOT EXISTS green_driving_events_dedup
  ON green_driving_events (imei, occurred_at, event_type);
-- FMS polls "everything after id N".
CREATE INDEX IF NOT EXISTS green_driving_events_id_idx ON green_driving_events (id);

DO $$ BEGIN RAISE NOTICE 'green_driving_events ready'; END $$;
