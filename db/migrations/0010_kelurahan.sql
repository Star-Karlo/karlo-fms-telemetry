-- 0010_kelurahan.sql — Indonesia kelurahan/desa (village, ADM4) boundary master.
--
-- Deepest administrative polygon: point-in-polygon on this layer returns the
-- full chain kelurahan → kecamatan → kabupaten/kota → provinsi in one lookup
-- (all parent names + P-codes denormalized onto each row). Supersedes the ADM3
-- `kecamatan` table (0004) as the geocode polygon source; kecamatan lives here
-- as a parent column now.
--
-- Source: HDX COD-AB Indonesia (BPS-derived), ADM4 shapefile (idn_admin4.shp).
-- SHARED reference data — no tenant_id, no RLS.

-- Supersede the ADM3 kecamatan table (0004): its hierarchy now lives here.
DROP TABLE IF EXISTS kecamatan;

CREATE TABLE kelurahan (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kel_code   text UNIQUE,                        -- ADM4 P-code (BPS)
  kel_name   text NOT NULL,                      -- kelurahan / desa
  kec_code   text,                               -- ADM3 P-code
  kec_name   text,                               -- kecamatan / distrik
  kab_code   text,                               -- ADM2 P-code
  kab_name   text,                               -- kabupaten / kota
  prov_code  text,                               -- ADM1 P-code
  prov_name  text,                               -- provinsi
  geom       geometry(MultiPolygon, 4326) NOT NULL
);

-- GIST index makes ST_Contains(geom, point) an index scan, not a seq scan.
CREATE INDEX idx_kelurahan_geom ON kelurahan USING gist (geom);
CREATE INDEX idx_kelurahan_prov ON kelurahan (prov_code);
