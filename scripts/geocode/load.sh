#!/usr/bin/env bash
#
# Load Indonesia kecamatan (ADM3) boundaries from HDX COD-AB into Postgres.
#
#   Usage (from backend/):  make load-geocode
#   Direct:                 ./scripts/geocode/load.sh
#
# Requires Docker (uses ghcr.io/osgeo/gdal for ogr2ogr — no host GDAL install)
# and a running compose `db` service (`make up`). Idempotent: re-running
# reloads the kecamatan table from scratch.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_ROOT="$(cd "$HERE/../.." && pwd)"
DATA_DIR="$HERE/data"
mkdir -p "$DATA_DIR"

PGHOST="${PGHOST:-host.docker.internal}"   # from inside the GDAL container
PGPORT="${PGPORT:-5432}"
PGDB="${PGDB:-fms}"
PGUSER="${PGUSER:-postgres}"
PGPASS="${PGPASS:-postgres}"
GDAL_IMAGE="${GDAL_IMAGE:-ghcr.io/osgeo/gdal:alpine-small-latest}"
COMPOSE="docker compose -f $BACKEND_ROOT/docker-compose.yml"

# Optional: a direct shapefile-zip URL. HDX often blocks scripted downloads,
# so the manual-drop path below is the reliable route.
HDX_URL="${HDX_URL:-}"

# --- 1. Ensure an ADM3 shapefile exists locally (manual drop wins) -----------
find_shp() { find "$DATA_DIR" -iname '*adm*3*.shp' -o -iname '*ADM3*.shp' 2>/dev/null | head -n1; }
shp="$(find_shp || true)"

if [ -z "$shp" ] && [ -n "$HDX_URL" ]; then
  echo ">> Downloading COD-AB from: $HDX_URL"
  if curl -fSL -A "Mozilla/5.0" "$HDX_URL" -o "$DATA_DIR/idn_codab.zip"; then
    unzip -o "$DATA_DIR/idn_codab.zip" -d "$DATA_DIR" >/dev/null
    shp="$(find_shp || true)"
  fi
fi

if [ -z "$shp" ]; then
  cat <<EOF
!! No kecamatan (ADM3) shapefile found in:
     $DATA_DIR

   HDX blocks scripted downloads, so grab it manually:
     1. Open  https://data.humdata.org/dataset/cod-ab-idn
     2. Download the administrative-boundary SHAPEFILE zip (contains ADM0-4).
     3. Unzip its contents into: $DATA_DIR
     4. Re-run:  make load-geocode
EOF
  exit 1
fi
echo ">> Using shapefile: $shp"

# --- 2. ogr2ogr shapefile -> Postgres staging (WGS84, MultiPolygon) ----------
docker run --rm -v "$DATA_DIR:/data" "$GDAL_IMAGE" \
  ogr2ogr -f PostgreSQL \
    "PG:host=$PGHOST port=$PGPORT dbname=$PGDB user=$PGUSER password=$PGPASS" \
    "/data/$(basename "$shp")" \
    -nln kecamatan_staging -nlt MULTIPOLYGON -t_srs EPSG:4326 \
    -lco GEOMETRY_NAME=geom -lco LAUNDER=YES -overwrite

echo ">> Staging columns:"
$COMPOSE exec -T db psql -U "$PGUSER" -d "$PGDB" -c "\d kecamatan_staging" || true

# --- 3. Normalize staging -> kecamatan --------------------------------------
# COD-AB Indonesia (HDX) columns: adm{3,2,1}_name + adm{3,2,1}_pcode
# (verified via ogrinfo on idn_admin3.shp). Adjust here if your vintage differs.
$COMPOSE exec -T db psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE kecamatan RESTART IDENTITY;
INSERT INTO kecamatan (kec_code, kec_name, kab_code, kab_name, prov_code, prov_name, geom)
SELECT adm3_pcode, adm3_name, adm2_pcode, adm2_name, adm1_pcode, adm1_name,
       ST_Multi(geom)::geometry(MultiPolygon, 4326)
FROM kecamatan_staging;
DROP TABLE kecamatan_staging;
SELECT count(*) AS kecamatan_loaded FROM kecamatan;
SQL

echo ">> Done."
