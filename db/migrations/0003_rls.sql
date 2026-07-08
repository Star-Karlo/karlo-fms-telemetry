-- 0003_rls.sql — tenant isolation via Row-Level Security.
-- The app connects as a NON-owner role and runs `SET app.current_tenant = '<id>'`
-- (per transaction). Every query is then automatically scoped to that tenant.
--
-- NOTE: table owners bypass RLS unless FORCE is set. We FORCE it so even the
-- migration/owner role is subject to the policy — the app must set the GUC.

ALTER TABLE vehicles           ENABLE ROW LEVEL SECURITY;
ALTER TABLE devices            ENABLE ROW LEVEL SECURITY;
ALTER TABLE device_assignments ENABLE ROW LEVEL SECURITY;
ALTER TABLE geofences          ENABLE ROW LEVEL SECURITY;
ALTER TABLE telemetry          ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'vehicles','devices','device_assignments','geofences','telemetry'
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
