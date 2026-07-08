-- 0001_core.sql — operational (CRUD) schema: tenants, vehicles, devices,
-- the TEMPORAL device<->vehicle assignment, and geofences.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;  -- needed for the assignment exclusion constraint

-- ---------------------------------------------------------------------------
-- Tenants (SaaS multi-tenancy root; every other table carries tenant_id)
-- ---------------------------------------------------------------------------
CREATE TABLE tenants (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name        text NOT NULL,
  slug        text NOT NULL UNIQUE,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Vehicles = the asset (the truck itself)
-- ---------------------------------------------------------------------------
CREATE TABLE vehicles (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id   bigint NOT NULL REFERENCES tenants(id),
  plate_no    text   NOT NULL,
  vin         text,
  make        text,
  model       text,
  year        int,
  type        text,                               -- truck | van | trailer ...
  status      text   NOT NULL DEFAULT 'active',
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, plate_no)
);
CREATE INDEX idx_vehicles_tenant ON vehicles (tenant_id);

-- ---------------------------------------------------------------------------
-- Devices = the sensor/box bolted to a vehicle (Teltonika unit or Safetrucks id)
-- Telemetry arrives keyed by `identifier`; ingest resolves it to a device.
-- ---------------------------------------------------------------------------
CREATE TABLE devices (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id   bigint NOT NULL REFERENCES tenants(id),
  source      text   NOT NULL CHECK (source IN ('teltonika','safetrucks')),
  identifier  text   NOT NULL,                    -- Teltonika IMEI | Safetrucks external id
  model       text,
  sim_iccid   text,
  status      text   NOT NULL DEFAULT 'active',
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, source, identifier)
);
CREATE INDEX idx_devices_tenant ON devices (tenant_id);
-- hot path: look up a device by its wire identifier on every incoming ping
CREATE INDEX idx_devices_lookup ON devices (source, identifier);

-- ---------------------------------------------------------------------------
-- device_assignments = TEMPORAL link between a device and a vehicle.
-- A box gets moved between trucks; this table records *when* it was on *which*.
-- The exclusion constraint guarantees a device is on <= 1 vehicle at any instant.
-- valid_to IS NULL  => currently attached.
-- ---------------------------------------------------------------------------
CREATE TABLE device_assignments (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id   bigint NOT NULL REFERENCES tenants(id),
  device_id   bigint NOT NULL REFERENCES devices(id),
  vehicle_id  bigint NOT NULL REFERENCES vehicles(id),
  valid_from  timestamptz NOT NULL DEFAULT now(),
  valid_to    timestamptz,
  EXCLUDE USING gist (
    device_id WITH =,
    tstzrange(valid_from, valid_to, '[)') WITH &&
  )
);
CREATE INDEX idx_assign_device ON device_assignments (device_id, valid_from DESC);
CREATE INDEX idx_assign_vehicle ON device_assignments (vehicle_id);

-- ---------------------------------------------------------------------------
-- Geofences (PostGIS). Polygon zones for enter/exit alerting.
-- ---------------------------------------------------------------------------
CREATE TABLE geofences (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id   bigint NOT NULL REFERENCES tenants(id),
  name        text   NOT NULL,
  geom        geography(Polygon, 4326) NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_geofences_geom   ON geofences USING gist (geom);
CREATE INDEX idx_geofences_tenant ON geofences (tenant_id);
