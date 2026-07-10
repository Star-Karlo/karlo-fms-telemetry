-- Multi-vendor ingest: `devices.source` identifies the vendor/adapter and is
-- now open-ended (valid sources are enforced in the app via the ingest
-- Adapters map), so drop the hard-coded CHECK from 0001_core.sql.
ALTER TABLE devices DROP CONSTRAINT IF EXISTS devices_source_check;
