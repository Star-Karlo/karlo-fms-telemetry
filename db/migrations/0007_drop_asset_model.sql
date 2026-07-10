-- Tracking-only for now: drop the vehicle/asset registry and telemetry's FK
-- columns. Keeps tenants + geofences. (Re-introduce vehicle linkage later.)

-- Asset registry tables (dropping cascades their RLS policies + FKs).
DROP TABLE IF EXISTS device_assignments;
DROP TABLE IF EXISTS devices;
DROP TABLE IF EXISTS vehicles;

-- Telemetry compression segments by tenant_id/vehicle_id — re-point it to imei
-- BEFORE dropping those columns, or the drop fails.
ALTER TABLE telemetry SET (timescaledb.compress_segmentby = 'imei');

ALTER TABLE telemetry DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE telemetry DROP COLUMN IF EXISTS device_id;
ALTER TABLE telemetry DROP COLUMN IF EXISTS vehicle_id;
