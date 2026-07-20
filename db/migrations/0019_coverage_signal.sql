-- 0019_coverage_signal.sql — signal grid split by device generation AND carrier,
-- so the map can answer "signal as measured by 4G units on Telkomsel".
--
-- Long format rather than crossed columns on coverage_grid: 2 generations x 4
-- carrier buckets would need ~24 more columns (sum/cnt/weak per pair), and any
-- future split would need another migration. At most 8 rows per cell (~130k
-- rows total) the endpoint just filters and re-aggregates.
--
-- provider 'unk' holds readings where the device reported no operator (~15% of
-- pings) plus '0' = no service. Selecting every named carrier is therefore NOT
-- the same as no filter at all, which is why the API treats "all ticked" as
-- "omit the filter" instead of listing the three.

CREATE TABLE IF NOT EXISTS coverage_signal (
  cell_lat   double precision NOT NULL,
  cell_lon   double precision NOT NULL,
  gen        text             NOT NULL CHECK (gen IN ('2g','4g')),
  provider   text             NOT NULL CHECK (provider IN ('tsel','isat','xl','unk')),
  sig_sum    double precision NOT NULL DEFAULT 0,
  sig_cnt    bigint           NOT NULL DEFAULT 0,
  weak       bigint           NOT NULL DEFAULT 0,
  pings      bigint           NOT NULL DEFAULT 0,
  updated_at timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (cell_lat, cell_lon, gen, provider)
);
