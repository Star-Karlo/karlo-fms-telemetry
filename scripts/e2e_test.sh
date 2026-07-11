#!/usr/bin/env bash
#
# End-to-end test of the telemetry pipeline:
#   1 input telemetry   2 see it saved   3 get live   4 burst
#   5 get historical    6 archive -> Parquet -> S3 (Glacier IR)
#
# Prereqs:
#   - ingest service running:  make run-ingest        (on :8082)
#   - local DB up + migrated:  make up && make migrate
#   - for step 6: AWS creds (AWS_PROFILE) + S3_BUCKET
#
# Usage:  ./scripts/e2e_test.sh
set -euo pipefail

BASE="${BASE:-http://localhost:8082}"
DB="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/fms}"
DAY="${DAY:-2026-06-10}"   # an "old" day so step 6 can archive it
IMEI=E2E01

hr(){ printf '\n=============== %s ===============\n' "$1"; }
post(){ curl -s -XPOST "$BASE/v1/ingest" -H 'Content-Type: application/json' -d "$1" >/dev/null; }

hr "0. reset test rows"
psql "$DB" -qc "DELETE FROM telemetry WHERE imei LIKE 'E2E%'; DELETE FROM latest WHERE imei LIKE 'E2E%';"

hr "1. INPUT telemetry — 3 points for $IMEI (Brangsong, Kendal)"
for t in 08:00 08:05 08:10; do
  post "{\"imei\":\"$IMEI\",\"latitude\":-6.948723,\"longitude\":110.23872,\"speed\":40,\"gpsCreatedAt\":\"${DAY}T${t}:00Z\",\"createdAt\":\"${DAY}T${t}:05Z\"}"
  echo "  posted $t"
done

hr "2. SEE data saved (history rows + latest state)"
psql "$DB" -c "SELECT time, imei, speed, provinsi, kabupaten, kota, kecamatan FROM telemetry WHERE imei='$IMEI' ORDER BY time;"
psql "$DB" -c "SELECT imei, time AS latest_gps, speed FROM latest WHERE imei='$IMEI';"

hr "3. GET LIVE data — /v1/live/$IMEI"
curl -s "$BASE/v1/live/$IMEI" | python3 -m json.tool

hr "4. BURST — 20 rapid readings for E2E02"
for i in $(seq 1 20); do
  post "{\"imei\":\"E2E02\",\"latitude\":-6.95,\"longitude\":110.24,\"speed\":$i,\"gpsCreatedAt\":\"${DAY}T09:00:$(printf '%02d' "$i")Z\"}" &
done
wait
echo "  E2E02 stored rows: $(psql "$DB" -tAc "SELECT count(*) FROM telemetry WHERE imei='E2E02';")"

hr "5. GET HISTORICAL data — /v1/history/$IMEI (full day)"
curl -s "$BASE/v1/history/$IMEI?from=${DAY}T00:00:00Z&to=${DAY}T23:59:59Z" \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print(' points:',len(d));[print('  ',p['time'],'speed',p['speed']) for p in d]"

hr "steps 1-5 complete. Run step 6 with:"
echo "  ARCHIVE_DATE=$DAY S3_BUCKET=karlo-fms-tracking-cold-storage AWS_PROFILE=Karlo \\"
echo "    go run ./cmd/archiver"
