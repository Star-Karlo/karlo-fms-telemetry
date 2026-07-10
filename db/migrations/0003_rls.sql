-- 0003_rls.sql — tenant isolation via Row-Level Security.
-- The app connects as a NON-owner role and runs `SET app.current_tenant = '<id>'`
-- (per transaction). Every query is then automatically scoped to that tenant.
--
-- IMPORTANT: `telemetry` is intentionally EXCLUDED from RLS. TimescaleDB does
-- not allow row-level security on a hypertable with columnstore/compression
-- enabled ("columnstore cannot be used on table with row security"), and at
-- ~2.9M rows/day the 90%+ compression is worth more than RLS on that one table.
-- Tenant scoping for telemetry is enforced in the QUERY LAYER: every telemetry
-- read/write must include `WHERE tenant_id = <current tenant>`. RLS still guards
-- all the low-volume operational tables below.

ALTER TABLE vehicles           ENABLE ROW LEVEL SECURITY;
ALTER TABLE devices            ENABLE ROW LEVEL SECURITY;
ALTER TABLE device_assignments ENABLE ROW LEVEL SECURITY;
ALTER TABLE geofences          ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'vehicles','devices','device_assignments','geofences'
  ] LOOP
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      || 'USING (tenant_id = current_setting(''app.current_tenant'', true)::bigint) '
      || 'WITH CHECK (tenant_id = current_setting(''app.current_tenant'', true)::bigint)',
      t);
  END LOOP;
END $$;

-- A dedicated app role that is subject to RLS (not the DB owner/superuser).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fms_app') THEN
    CREATE ROLE fms_app LOGIN PASSWORD 'fms_app';
  END IF;
END $$;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fms_app;
GRANT USAGE ON SCHEMA public TO fms_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fms_app;
