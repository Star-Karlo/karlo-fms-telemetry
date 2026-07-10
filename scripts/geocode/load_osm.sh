#!/usr/bin/env bash
#
# Load OpenStreetMap road centerlines into the `road` table for street-name
# reverse geocoding. Reads every *.osm / *.osm.pbf file in scripts/geocode/data/
# (e.g. java-latest.osm.pbf, sumatra-latest.osm.pbf) and imports the named
# highways from each into one `road` table.
#
#   Local:   ./scripts/geocode/load_osm.sh
#   Remote:  DATABASE_URL="postgres://...:5432/tsdb?sslmode=require" \
#            ./scripts/geocode/load_osm.sh
#
# Requires Docker (ghcr.io/osgeo/gdal for ogr2ogr — same image as load.sh) and
# host `psql`. Idempotent: rebuilds the road table from scratch each run.
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

# 1. Locate OSM input files (.osm or .osm.pbf).
files=()
while IFS= read -r line; do files+=("$line"); done < <(find "$DATA_DIR" -maxdepth 1 \( -iname '*.osm' -o -iname '*.osm.pbf' -o -iname '*.pbf' \) 2>/dev/null | sort)
if [ "${#files[@]}" -eq 0 ]; then
  echo "!! No .osm/.osm.pbf files in $DATA_DIR — download Geofabrik extracts first." >&2
  echo "   Java:    https://download.geofabrik.de/asia/indonesia/java-latest.osm.pbf" >&2
  echo "   Sumatra: https://download.geofabrik.de/asia/indonesia/sumatra-latest.osm.pbf" >&2
  exit 1
fi
echo ">> target: $PGH:$PGP/$PGDB (ogr host: $OGR_HOST)"
echo ">> inputs: ${files[*]}"

# 2. ogr2ogr: OSM `lines` layer -> Postgres staging. Keep only named highways.
#    First file overwrites the staging table; the rest append.
mode="-overwrite"
for f in "${files[@]}"; do
  base="$(basename "$f")"
  echo ">> importing $base ($mode) ..."
  docker run --rm -v "$DATA_DIR:/data" "$GDAL_IMAGE" \
    ogr2ogr -f PostgreSQL \
      "PG:host=$OGR_HOST port=$PGP dbname=$PGDB user=$PGU password=$PGPW sslmode=$PGSSL" \
      "/data/$base" lines \
      -where "highway IS NOT NULL AND name IS NOT NULL" \
      -nln road_staging -nlt LINESTRING -t_srs EPSG:4326 \
      -lco GEOMETRY_NAME=geom -lco LAUNDER=YES $mode -update \
      --config OSM_MAX_TMPFILE_SIZE 2000
  mode="-append"
done

# 3. Normalize staging -> road (host psql). ST_Dump splits any multipart geoms
#    so every row satisfies the LineString typmod.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE road RESTART IDENTITY;
INSERT INTO road (osm_id, name, highway, geom)
SELECT osm_id, name, highway, (ST_Dump(geom)).geom
FROM road_staging
WHERE name IS NOT NULL AND highway IS NOT NULL;
DROP TABLE road_staging;
SELECT count(*) AS roads_loaded, count(DISTINCT name) AS distinct_names FROM road;
SQL

echo ">> Done."
