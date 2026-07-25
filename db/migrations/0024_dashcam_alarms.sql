-- 0024_dashcam_alarms.sql — dashcam (MDVR) events received over the VSS
-- WebSocket, the raw layer's record of them.
--
-- Tenant-agnostic like the rest of this service: keyed by the VSS deviceno.
-- FMS polls this table, maps each device to a vehicle it owns, and raises its
-- own tenant-scoped alert. Storing them here (rather than only relaying) means
-- an alarm survives an FMS restart or a brief poll gap — the durable raw record
-- lives on this side, exactly like GPS.
--
-- Idempotent.

CREATE TABLE IF NOT EXISTS dashcam_alarms (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  device_id   text NOT NULL,
  -- The VSS event uuid. Deduplicates: the vendor can re-push the same event
  -- (start/ongoing/end), and we keep one row per event.
  alarm_id    text,
  code        int NOT NULL,
  occurred_at timestamptz NOT NULL,
  lat         double precision,
  lon         double precision,
  speed       double precision,
  raw         jsonb,
  received_at timestamptz NOT NULL DEFAULT now()
);

-- One row per VSS event. Partial, because a rare malformed push may carry no id
-- and should still be stored rather than rejected.
CREATE UNIQUE INDEX IF NOT EXISTS dashcam_alarms_alarm_id_uk
  ON dashcam_alarms (alarm_id) WHERE alarm_id IS NOT NULL;

-- FMS polls "give me everything after id N", newest work first.
CREATE INDEX IF NOT EXISTS dashcam_alarms_id_idx ON dashcam_alarms (id);
CREATE INDEX IF NOT EXISTS dashcam_alarms_device_idx ON dashcam_alarms (device_id, occurred_at DESC);

DO $$ BEGIN RAISE NOTICE 'dashcam_alarms ready'; END $$;
