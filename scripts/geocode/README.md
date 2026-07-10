# Geocode master data — Indonesia kecamatan

Loads kecamatan (district, ADM3) boundary polygons into the `kecamatan` table so
telemetry pings can be reverse-geocoded to `kecamatan → kabupaten/kota → provinsi`
with a PostGIS point-in-polygon query.

## Source

**HDX COD-AB Indonesia** (BPS-derived): <https://data.humdata.org/dataset/cod-ab-idn>
— ADM0–4 shapefiles with the full hierarchy and BPS P-codes in the attributes.

## Run

```bash
make up            # ensure Timescale/PostGIS + Redis are running
make migrate       # ensure 0004_geocode.sql is applied
make load-geocode  # runs scripts/geocode/load.sh
```

The loader uses a **Docker GDAL image** (`ghcr.io/osgeo/gdal`) for `ogr2ogr`, so
no host GDAL install is needed.

## Manual download (HDX blocks scripted pulls)

If the script reports no shapefile, download it by hand:

1. Open <https://data.humdata.org/dataset/cod-ab-idn>
2. Download the **shapefile** zip (contains ADM0–4).
3. Unzip its contents into `scripts/geocode/data/`.
4. Re-run `make load-geocode`.

Alternatively set `HDX_URL=<direct-zip-url> make load-geocode`.

## Column mapping

The normalizer expects the COD-AB standard columns (lower-cased by ogr2ogr):
`adm3_en/adm3_pcode`, `adm2_en/adm2_pcode`, `adm1_en/adm1_pcode`. If your dataset
vintage uses different names, the loader prints the staging table's columns —
adjust the `INSERT ... SELECT` in `load.sh` accordingly.

## Data directory

`scripts/geocode/data/` is git-ignored (boundary files are large). Keep the
source zip there for reproducibility.
