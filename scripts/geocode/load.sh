#!/usr/bin/env bash
#
# Load the COD-AB ADM4 boundaries into the `kelurahan` table — the full admin
# hierarchy (provinsi/kabupaten/kecamatan/kelurahan + BPS P-codes) denormalized
# onto each kelurahan polygon, so one point-in-polygon returns the whole chain.
#
#   Local:   ./scripts/geocode/load.sh
#   Remote:  DATABASE_URL="postgres://...:5432/tsdb?sslmode=require" \
#            ./scripts/geocode/load.sh
#
# Requires Docker (ghcr.io/osgeo/gdal for ogr2ogr — no host GDAL) and host
# `psql`. Idempotent. Needs idn_admin4.shp (COD-AB) in scripts/geocode/data/.
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

OGR_HOST="$PGH"
case "$PGH" in localhost|127.0.0.1) OGR_HOST="host.docker.internal";; esac

shp="$(find "$DATA_DIR" -iname 'idn_admin4.shp' 2>/dev/null | head -n1 || true)"
if [ -z "$shp" ]; then
  echo "!! idn_admin4.shp (COD-AB ADM4) not found in $DATA_DIR — see README.md." >&2
  exit 1
fi
echo ">> shapefile: $shp"
echo ">> target:    $PGH:$PGP/$PGDB (ogr host: $OGR_HOST)"

# 1. ogr2ogr ADM4 shapefile -> staging (WGS84, MultiPolygon).
docker run --rm -v "$DATA_DIR:/data" "$GDAL_IMAGE" \
  ogr2ogr -f PostgreSQL \
    "PG:host=$OGR_HOST port=$PGP dbname=$PGDB user=$PGU password=$PGPW sslmode=$PGSSL" \
    "/data/$(basename "$shp")" \
    -nln kelurahan_staging -nlt MULTIPOLYGON -t_srs EPSG:4326 \
    -lco GEOMETRY_NAME=geom -lco LAUNDER=YES -overwrite

# 2. Normalize staging -> kelurahan. COD-AB columns: adm{4,3,2,1}_name/_pcode.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE kelurahan RESTART IDENTITY;
INSERT INTO kelurahan
  (kel_code, kel_name, kec_code, kec_name, kab_code, kab_name, prov_code, prov_name, geom)
SELECT adm4_pcode, adm4_name, adm3_pcode, adm3_name, adm2_pcode, adm2_name,
       adm1_pcode, adm1_name, ST_Multi(geom)::geometry(MultiPolygon, 4326)
FROM kelurahan_staging;
DROP TABLE kelurahan_staging;
SELECT count(*) AS kelurahan_loaded FROM kelurahan;
SQL

echo ">> Done."
