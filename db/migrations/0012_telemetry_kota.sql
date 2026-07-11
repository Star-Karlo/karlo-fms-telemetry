-- Split the ADM2 admin level into kabupaten (regency) and kota (city) on stored
-- telemetry, matching the geocode response. Kecamatan-level stays; kelurahan and
-- street are intentionally NOT stored on hot telemetry (geocode lookup only).
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS kota text;
ALTER TABLE latest    ADD COLUMN IF NOT EXISTS kota text;
