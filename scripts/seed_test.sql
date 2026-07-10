-- Idempotent test fixtures for the ingest service:
-- a tenant, a vehicle, a registered device (IMEI), and an open assignment.
WITH t AS (
  INSERT INTO tenants (name, slug) VALUES ('Test Tenant', 'test')
  ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
  RETURNING id
), v AS (
  INSERT INTO vehicles (tenant_id, plate_no, make, model, type)
  SELECT id, 'B1234TEST', 'Hino', '500', 'truck' FROM t
  ON CONFLICT (tenant_id, plate_no) DO UPDATE SET make = EXCLUDED.make
  RETURNING id, tenant_id
), d AS (
  INSERT INTO devices (tenant_id, source, identifier, sim_iccid, status)
  SELECT tenant_id, 'generic', '860000000000001', '8962000000000000001', 'active' FROM v
  ON CONFLICT (tenant_id, source, identifier) DO UPDATE SET status = 'active'
  RETURNING id, tenant_id
)
INSERT INTO device_assignments (tenant_id, device_id, vehicle_id, valid_from)
SELECT d.tenant_id, d.id, v.id, now()
FROM d, v
WHERE NOT EXISTS (
  SELECT 1 FROM device_assignments da
  WHERE da.device_id = d.id AND da.valid_to IS NULL
);

SELECT d.identifier AS imei, d.id AS device_id, v.plate_no, v.id AS vehicle_id, ten.slug AS tenant
FROM devices d
JOIN device_assignments da ON da.device_id = d.id AND da.valid_to IS NULL
JOIN vehicles v ON v.id = da.vehicle_id
JOIN tenants ten ON ten.id = d.tenant_id
WHERE d.identifier = '860000000000001';
