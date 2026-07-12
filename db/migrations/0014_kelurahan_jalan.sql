-- Persist the deeper geocode levels (village + street) on telemetry so live
-- and historical reads expose them without re-geocoding. Populated at ingest
-- from the reverse-geocode result (kelurahan = ADM4, jalan = nearest OSM road).

ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS kelurahan text;
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS jalan     text;

ALTER TABLE latest ADD COLUMN IF NOT EXISTS kelurahan text;
ALTER TABLE latest ADD COLUMN IF NOT EXISTS jalan     text;
