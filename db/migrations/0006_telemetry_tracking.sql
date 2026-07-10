-- Tracking-first telemetry: key readings by IMEI and store the reverse-geocoded
-- location (kecamatan/kabupaten/provinsi). Tenant + vehicle linkage is deferred,
-- so relax those columns to nullable for now (re-introduced later).
ALTER TABLE telemetry ALTER COLUMN tenant_id DROP NOT NULL;
ALTER TABLE telemetry ALTER COLUMN device_id DROP NOT NULL;

ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS imei      text;
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS kecamatan text;
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS kabupaten text;
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS provinsi  text;

-- Primary query pattern is now "one device's track over time".
CREATE INDEX IF NOT EXISTS idx_tel_imei ON telemetry (imei, time DESC);
