-- 0017_coverage_altitude.sql — store real elevation per cell alongside the
-- ruggedness ratio. Sustained climbing burns fuel even where the up/down
-- ruggedness looks moderate, so the map needs actual altitude as its own layer.
-- Sums+counts (like the rest of the grid) so nightly rollups merge incrementally.

ALTER TABLE coverage_grid
  ADD COLUMN IF NOT EXISTS alt_sum double precision NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS alt_cnt bigint           NOT NULL DEFAULT 0;
