-- Enable hot-tier retention on the telemetry hypertable.
--
-- Hot window: ~2 weeks in Timescale. Older data lives in S3 Parquet (cold),
-- read transparently via the /v1/history dual-read router (Athena).
--
-- IMPORTANT ordering: the daily archiver (EventBridge -> ECS) exports the day
-- that just crossed 14 days (now-14). Retention here drops chunks older than
-- **15 days** — one day *behind* the archiver — so every day is guaranteed to
-- be written to cold storage a full day before it is dropped from hot. Do NOT
-- lower this to 14 days or a chunk could be dropped the same day it is archived.
--
-- MIGRATION CAVEAT: before bulk-loading historical (>15d old) data, either
-- disable this policy during the load and backfill-archive those days first
-- (ARCHIVE_DATE=YYYY-MM-DD per day), or the next retention run will drop the
-- freshly-loaded old data. Re-enable afterwards.

SELECT add_retention_policy('telemetry', INTERVAL '15 days', if_not_exists => true);
