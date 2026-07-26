-- 0025_dashcam_alarms_gforce.sql — record the acceleration magnitude a
-- driving-behaviour event reports, when it carries one.
--
-- Harsh brake/accel/turn events from the MDVR's G-sensor may include a peak
-- G value in the event detail. We store it here alongside the raw payload so
-- FMS can classify severity (yellow/red/black) by G threshold and score driver
-- behaviour. Null for events that carry no acceleration figure — the column is
-- additive and safe to back-fill from `raw` once the exact vendor field is
-- confirmed against a real event.
--
-- Idempotent.

ALTER TABLE dashcam_alarms ADD COLUMN IF NOT EXISTS gforce double precision;

DO $$ BEGIN RAISE NOTICE 'dashcam_alarms.gforce ready'; END $$;
