-- 0021_vehicle_day.sql — per-device daily rollup of how time was actually spent.
--
-- WHY A TABLE AND NOT A QUERY: the aggregate over the raw hypertable costs
-- ~15s for one day and ~70s for seven (measured against production: 13.7M rows,
-- 1,959 devices). That is fine for a background job and hopeless for a
-- dashboard request. Rolled up once, a week of fleet history reads in
-- milliseconds from ~2k rows/day.
--
-- WHY NOT A CONTINUOUS AGGREGATE: durations need the gap to the NEXT sample,
-- i.e. a window function, which continuous aggregates do not allow. The
-- toolkit's state_agg() would sidestep that, but it rejects two different
-- states at one timestamp and this stream genuinely contains duplicate
-- timestamps per device. Both formulations were run against production and
-- agreed to the hour, so the window-function version is used for being the one
-- that tolerates the real data.
--
-- Idempotent: scripts/migrate.sh re-applies every file on every deploy.

CREATE TABLE IF NOT EXISTS vehicle_day (
  imei        text        NOT NULL,
  day         date        NOT NULL,      -- local calendar day, see the note below
  moving_s    bigint      NOT NULL DEFAULT 0,
  idle_s      bigint      NOT NULL DEFAULT 0,   -- engine on, not moving: the waste
  parking_s   bigint      NOT NULL DEFAULT 0,
  distance_km numeric     NOT NULL DEFAULT 0,
  max_speed   numeric,
  samples     bigint      NOT NULL DEFAULT 0,   -- data health, not a fleet metric
  first_seen  timestamptz,
  last_seen   timestamptz,
  final       boolean     NOT NULL DEFAULT false, -- day complete, stop recomputing
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (imei, day)
);

-- The dashboard always reads a date range across all devices, so day leads.
CREATE INDEX IF NOT EXISTS idx_vehicle_day_day ON vehicle_day (day DESC, imei);

COMMENT ON TABLE vehicle_day IS
  'Per-device daily time-in-state and distance. Days are Asia/Jakarta calendar '
  'days, NOT UTC: the fleet runs in WIB (+07), so a UTC bucket would split '
  'every night shift across two days and misreport anything within 7 hours of '
  'midnight.';

COMMENT ON COLUMN vehicle_day.idle_s IS
  'Seconds with ignition on and speed <= 3 km/h. Time-weighted from the gap to '
  'the next sample, capped at 900s so a silent device cannot invent hours. '
  'Counting samples instead would bias this badly — devices report far more '
  'often while moving (median gap 90s, but as low as 1s in bursts).';

COMMENT ON COLUMN vehicle_day.final IS
  'Set once the day is over and has been rolled up, so the hourly job only '
  'recomputes today and yesterday rather than rescanning the whole window.';
