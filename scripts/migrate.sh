#!/usr/bin/env bash
#
# Apply db/migrations/*.sql in order against $DATABASE_URL using host `psql`.
# Works for any Postgres target — the local compose DB or a remote managed DB
# (e.g. Tiger Cloud). Defaults to the local compose DB.
#
#   Local:   ./scripts/migrate.sh
#   Remote:  DATABASE_URL="postgres://tsdbadmin:...@...tsdb.cloud.timescale.com:5432/tsdb?sslmode=require" \
#            ./scripts/migrate.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_ROOT="$(cd "$HERE/.." && pwd)"
DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/fms}"

# Mask credentials when echoing the target.
masked="$(printf '%s' "$DATABASE_URL" | sed -E 's#://[^@]*@#://***@#')"
echo ">> target: $masked"

for f in "$BACKEND_ROOT"/db/migrations/*.sql; do
  echo ">> applying $(basename "$f")"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -f "$f"
done

echo ">> migrations applied."
