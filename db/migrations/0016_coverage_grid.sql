-- 0016_coverage_grid.sql — spatial rollup of GSM signal + terrain ruggedness.
--
-- Every telemetry ping is bucketed into a ~1.2 km cell (lat/lon floored to a
-- 0.011° grid). We store SUMS + COUNTS (not averages) so a nightly per-day
-- rollup can MERGE incrementally (add sums, add counts); the read endpoint
-- computes averages. Physical-world data (coverage/terrain), not tenant-scoped.

CREATE TABLE IF NOT EXISTS coverage_grid (
  cell_lat   double precision NOT NULL,  -- SW corner of the cell (0.011° grid)
  cell_lon   double precision NOT NULL,
  pings      bigint  NOT NULL DEFAULT 0,
  -- GSM signal (0-5): overall + per provider (sum/cnt -> avg on read)
  sig_sum    double precision NOT NULL DEFAULT 0,
  sig_cnt    bigint  NOT NULL DEFAULT 0,
  tsel_sum   double precision NOT NULL DEFAULT 0,
  tsel_cnt   bigint  NOT NULL DEFAULT 0,
  isat_sum   double precision NOT NULL DEFAULT 0,
  isat_cnt   bigint  NOT NULL DEFAULT 0,
  xl_sum     double precision NOT NULL DEFAULT 0,
  xl_cnt     bigint  NOT NULL DEFAULT 0,
  weak       bigint  NOT NULL DEFAULT 0,   -- pings with signal <=1 or no operator
  -- terrain ruggedness: |Δaltitude| accumulated over distance driven
  -- (ruggedness m/km = 1000 * rug_num / rug_den).
  rug_num    double precision NOT NULL DEFAULT 0,  -- sum |Δaltitude| (m)
  rug_den    double precision NOT NULL DEFAULT 0,  -- sum distance (m)
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (cell_lat, cell_lon)
);

-- Which days have been folded in — prevents double-counting on re-run and lets
-- the catch-up loop know what's left to do.
CREATE TABLE IF NOT EXISTS coverage_rollup_log (
  day      date PRIMARY KEY,
  pings    bigint NOT NULL,
  cells    bigint NOT NULL,
  done_at  timestamptz NOT NULL DEFAULT now()
);
