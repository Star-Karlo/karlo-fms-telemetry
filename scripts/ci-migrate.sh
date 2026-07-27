#!/usr/bin/env bash
# Apply pending db/migrations/*.sql, tracked in a schema_migrations table so only
# files that have never run are applied. Safe to run on every deploy — already
# applied migrations are skipped, so the non-idempotent early schema is never
# re-run.
#
# Requires in the environment:
#   DB_URL     — a psql keyword DSN, e.g. "host=... user=... dbname=fms_app sslmode=require"
#   PGPASSWORD — the DB password
#
# Connects as the master/owner role (migrations do CREATE TABLE / GRANT / RLS).
set -euo pipefail
: "${DB_URL:?DB_URL required}"
cd "$(dirname "$0")/.."

psql "$DB_URL" -v ON_ERROR_STOP=1 -q -c \
  "CREATE TABLE IF NOT EXISTS schema_migrations (version text primary key, applied_at timestamptz not null default now());"

count=0
for f in db/migrations/*.sql; do
  v="$(basename "$f")"
  # Already recorded? Skip. One small query per file — portable and clear.
  if [ "$(psql "$DB_URL" -tAc "SELECT 1 FROM schema_migrations WHERE version = '$v' LIMIT 1;")" = "1" ]; then
    continue
  fi
  echo ">> applying $v"
  # The file runs as one transaction; recording the version follows. Files are
  # idempotent (IF NOT EXISTS), so a failure between the two is safe to retry.
  psql "$DB_URL" -v ON_ERROR_STOP=1 -q -1 -f "$f"
  psql "$DB_URL" -v ON_ERROR_STOP=1 -q -c "INSERT INTO schema_migrations(version) VALUES ('$v');"
  count=$((count + 1))
done
echo ">> $count new migration(s) applied; schema up to date"
