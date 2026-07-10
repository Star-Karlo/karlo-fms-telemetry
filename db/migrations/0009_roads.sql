-- 0009_roads.sql — OpenStreetMap road centerlines for street-name geocoding.
--
-- The kecamatan table (0004) answers "which district contains this point?" via
-- point-in-polygon. Roads answer "what street is this point on?" via nearest-line
-- KNN: a coordinate rarely falls *inside* a road, so we take the closest named
-- road centerline within a small threshold.
--
-- Source: OpenStreetMap (Geofabrik regional extracts — currently Java + Sumatra).
-- SHARED reference data — no tenant_id, no RLS (like kecamatan).

CREATE TABLE road (
  id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  osm_id   text,                              -- OSM way id
  name     text NOT NULL,                     -- street name (e.g. "Jalan Jenderal Sudirman")
  highway  text,                              -- OSM class: motorway|primary|residential|service...
  geom     geometry(LineString, 4326) NOT NULL
);

-- GiST index powers the KNN (`geom <-> point`) nearest-road ordering.
CREATE INDEX idx_road_geom ON road USING gist (geom);
CREATE INDEX idx_road_name ON road (name);
