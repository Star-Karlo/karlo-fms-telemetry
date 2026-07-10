-- 0004_geocode.sql — Indonesia kecamatan (district) boundary master data.
--
-- Used for reverse geocoding telemetry pings "one level deeper than city":
-- a point-in-polygon lookup returns the kecamatan plus its parent kabupaten/kota
-- and provinsi. This is SHARED reference data — no tenant_id, no RLS
-- (contrast with the tenant-scoped tables in 0003_rls.sql).
--
-- Source: HDX COD-AB Indonesia (BPS-derived). ADM3 = kecamatan; parent
-- ADM2 (kabupaten/kota) and ADM1 (provinsi) names + P-codes are denormalized
-- onto each row so one ST_Contains returns the whole chain.

CREATE TABLE kecamatan (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kec_code   text UNIQUE,                        -- ADM3 P-code (BPS)
  kec_name   text NOT NULL,                      -- kecamatan / distrik
  kab_code   text,                               -- ADM2 P-code
  kab_name   text,                               -- kabupaten / kota
  prov_code  text,                               -- ADM1 P-code
  prov_name  text,                               -- provinsi
  geom       geometry(MultiPolygon, 4326) NOT NULL
);

-- GIST index makes ST_Contains(geom, point) an index scan, not a seq scan.
CREATE INDEX idx_kecamatan_geom ON kecamatan USING gist (geom);
CREATE INDEX idx_kecamatan_prov ON kecamatan (prov_code);
