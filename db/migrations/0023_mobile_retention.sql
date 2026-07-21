-- 0023_mobile_retention.sql — same hot/cold window for mobile as for telemetry.
--
-- Mirrors 0015 exactly, so both datasets age out identically:
--   * hot (uncompressed): today only (chunk interval = 1 day)
--   * compressed but still in Timescale: chunks older than 1 day (set in 0022)
--   * retention drop: chunks older than 8 days
--   * cold (S3 Parquet via Athena fms.mobile_cold): everything older
--
-- Coordination (identical to the GPS side, infra var.retention_days = 7):
--   * ingest read-boundary (RETENTION_DAYS=7): older than 7 days → cold
--   * archiver (RETENTION_DAYS=7): each nightly run archives day now-7 for BOTH
--     telemetry/ and mobile/ prefixes
--   * drop at 8 days → one day BEHIND the archiver, so every day is written to
--     cold a full day before it leaves hot. Do NOT lower the drop to 7.
--
-- Safe to apply immediately: mobile_telemetry was created in 0022, so no chunk
-- is anywhere near 8 days old yet — the archiver will have covered every day
-- long before the first drop is eligible.

SELECT remove_retention_policy('mobile_telemetry', if_exists => true);
SELECT add_retention_policy('mobile_telemetry', INTERVAL '8 days', if_not_exists => true);
