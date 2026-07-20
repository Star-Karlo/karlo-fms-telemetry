-- 0015_retention_7day.sql — shrink the hot window from ~14d to 7d.
--
-- New layout:
--   * hot (uncompressed): today only (chunk interval = 1 day)
--   * compressed but still in Timescale: chunks older than 1 day
--   * retention drop: chunks older than 8 days
--   * cold (S3 Parquet via Athena): everything older, read transparently
--
-- Coordination (must match infra var.retention_days = 7):
--   * ingest read-boundary (RETENTION_DAYS=7): data older than 7 days → cold
--   * archiver target day (RETENTION_DAYS=7): each nightly run archives now-7
--   * retention drop at 8 days → one day BEHIND the archiver, so every day is
--     written to cold a full day before it leaves hot (the same 1-day grace the
--     14/15 setup had). Do NOT lower the drop to 7 or a chunk could be dropped
--     the same day it is archived.
--
-- Safe to run now: cold storage already holds data older than 7 days, so the
-- chunks this newly drops (8–14 days old) are already archived.
--
-- Idempotent: remove-then-add so the end state is deterministic regardless of
-- which prior policy (0008 compression @7d, 0013 retention @15d) is present.

-- Retention: 15 days → 8 days.
SELECT remove_retention_policy('telemetry', if_exists => true);
SELECT add_retention_policy('telemetry', INTERVAL '8 days', if_not_exists => true);

-- Compression: 7 days → 1 day (this is where most of the disk savings come
-- from — recent day stays uncompressed for fast writes, everything else is
-- ~90% smaller but still fully queryable in Timescale).
SELECT remove_compression_policy('telemetry', if_exists => true);
SELECT add_compression_policy('telemetry', INTERVAL '1 day', if_not_exists => true);
