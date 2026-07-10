# Geocode master data — Indonesia kecamatan

Loads kecamatan (district, ADM3) boundary polygons into the `kecamatan` table so
telemetry pings can be reverse-geocoded to `kecamatan → kabupaten/kota → provinsi`
with a PostGIS point-in-polygon query.

## Source

**HDX COD-AB Indonesia** (BPS-derived): <https://data.humdata.org/dataset/cod-ab-idn>
— ADM0–4 shapefiles with the full hierarchy and BPS P-codes in the attributes.

## Run

```bash
# Local (docker compose DB):
make up            # ensure Timescale/PostGIS + Redis are running
make migrate       # ensure 0004_geocode.sql is applied
make load-geocode  # runs scripts/geocode/load.sh

# Remote (e.g. Tiger Cloud) — target any DATABASE_URL:
DATABASE_URL="postgres://tsdbadmin:...@...tsdb.cloud.timescale.com:5432/tsdb?sslmode=require" \
  ./scripts/geocode/load.sh
```

The loader derives its target from `DATABASE_URL` (default: local compose DB),
uses a **Docker GDAL image** (`ghcr.io/osgeo/gdal`) for `ogr2ogr` (no host GDAL
install), and `psql` for the normalize step.

## Manual download (HDX blocks scripted pulls)

If the script reports no shapefile, download it by hand:

1. Open <https://data.humdata.org/dataset/cod-ab-idn>
2. Download the **shapefile** zip (contains ADM0–4).
3. Unzip its contents into `scripts/geocode/data/` (the loader finds `*adm*3*.shp`).
4. Re-run the loader.

## Column mapping

The normalizer maps the COD-AB Indonesia columns (lower-cased by ogr2ogr):
`adm3_name/adm3_pcode`, `adm2_name/adm2_pcode`, `adm1_name/adm1_pcode`. If a
different dataset vintage uses other names, adjust the `INSERT ... SELECT` in
`load.sh`.

## Data directory

`scripts/geocode/data/` is git-ignored (boundary files are large). Keep the
source zip there for reproducibility.
