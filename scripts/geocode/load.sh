#!/usr/bin/env bash
#
# Load Indonesia kecamatan (ADM3) boundaries into the kecamatan table.
#   Local:   ./scripts/geocode/load.sh
#   Remote:  DATABASE_URL="postgres://tsdbadmin:...@...tsdb.cloud.timescale.com:5432/tsdb?sslmode=require" \
#            ./scripts/geocode/load.sh
#
# Requires Docker (ghcr.io/osgeo/gdal for ogr2ogr — no host GDAL install) and
# host `psql`. Idempotent: reloads the kecamatan table from scratch. The ADM3
# shapefile must already be in scripts/geocode/data/ (see README.md).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$HERE/data"
GDAL_IMAGE="${GDAL_IMAGE:-ghcr.io/osgeo/gdal:alpine-small-latest}"
DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/fms}"

# Parse DATABASE_URL into libpq parts (shell-quoted; safe for special chars).
eval "$(python3 - "$DATABASE_URL" <<'PY'
import sys, urllib.parse as u, shlex
r = u.urlparse(sys.argv[1]); q = dict(u.parse_qsl(r.query))
def e(k, v): print(f'{k}={shlex.quote(v)}')
e('PGH', r.hostname or 'localhost')
e('PGP', str(r.port or 5432))
e('PGU', u.unquote(r.username or ''))
e('PGPW', u.unquote(r.password or ''))
e('PGDB', (r.path or '/').lstrip('/') or 'postgres')
e('PGSSL', q.get('sslmode', 'prefer'))
PY
)"

# From inside the GDAL container, a host-local DB is reached via host.docker.internal.
OGR_HOST="$PGH"
case "$PGH" in localhost|127.0.0.1) OGR_HOST="host.docker.internal";; esac

# 1. Locate the ADM3 shapefile.
shp="$(find "$DATA_DIR" -iname '*adm*3*.shp' 2>/dev/null | head -n1 || true)"
if [ -z "$shp" ]; then
  echo "!! No ADM3 shapefile in $DATA_DIR — see scripts/geocode/README.md for the download." >&2
  exit 1
fi
echo ">> shapefile: $shp"
echo ">> target:    $PGH:$PGP/$PGDB (ogr host: $OGR_HOST)"

# 2. ogr2ogr shapefile -> Postgres staging (WGS84, MultiPolygon).
docker run --rm -v "$DATA_DIR:/data" "$GDAL_IMAGE" \
  ogr2ogr -f PostgreSQL \
    "PG:host=$OGR_HOST port=$PGP dbname=$PGDB user=$PGU password=$PGPW sslmode=$PGSSL" \
    "/data/$(basename "$shp")" \
    -nln kecamatan_staging -nlt MULTIPOLYGON -t_srs EPSG:4326 \
    -lco GEOMETRY_NAME=geom -lco LAUNDER=YES -overwrite

# 3. Normalize staging -> kecamatan (host psql).
# COD-AB Indonesia columns: adm{3,2,1}_name + adm{3,2,1}_pcode.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE kecamatan RESTART IDENTITY;
INSERT INTO kecamatan (kec_code, kec_name, kab_code, kab_name, prov_code, prov_name, geom)
SELECT adm3_pcode, adm3_name, adm2_pcode, adm2_name, adm1_pcode, adm1_name,
       ST_Multi(geom)::geometry(MultiPolygon, 4326)
FROM kecamatan_staging;
DROP TABLE kecamatan_staging;
SELECT count(*) AS kecamatan_loaded FROM kecamatan;
SQL

echo ">> Done."
