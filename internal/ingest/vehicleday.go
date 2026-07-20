package ingest

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fleetTZ is the timezone that defines a "day".
//
// The fleet runs in WIB (+07). Bucketing on UTC would split every night shift
// across two calendar days and misattribute anything within 7 hours of
// midnight — the same defect that exists today in the app DB's alerts-by-day
// query, which uses a bare date_trunc in whatever the session timezone happens
// to be (nothing sets one, so: UTC).
const fleetTZ = "Asia/Jakarta"

// movingKph mirrors liveview's threshold so "moving" means one thing across
// the product. Below it with the engine on is idling; below it with the engine
// off is parked.
const movingKph = 3.0

// maxGapSec caps how much time a single sample may account for.
//
// Durations come from the gap to the next sample, so without a cap a device
// that goes silent for six hours would report six hours of whatever state it
// was last in. 15 minutes is comfortably above the observed p99 gap (~6 min)
// while making a real outage cost minutes, not hours. Time inside a longer gap
// is simply not counted — "we don't know" beats a confident fabrication.
const maxGapSec = 900

// maxJumpM discards the distance of a single implausible hop. GPS occasionally
// teleports; without this one bad fix adds hundreds of km to a truck's day.
const maxJumpM = 3000

// VehicleDay is one device's day.
type VehicleDay struct {
	IMEI       string     `json:"imei"`
	Day        string     `json:"day"` // YYYY-MM-DD, Asia/Jakarta
	MovingS    int64      `json:"moving_s"`
	IdleS      int64      `json:"idle_s"`
	ParkingS   int64      `json:"parking_s"`
	DistanceKm float64    `json:"distance_km"`
	MaxSpeed   *float64   `json:"max_speed"`
	Samples    int64      `json:"samples"`
	FirstSeen  *time.Time `json:"first_seen"`
	LastSeen   *time.Time `json:"last_seen"`
}

// VehicleDayService rolls telemetry up into per-device days and serves them.
type VehicleDayService struct{ pool *pgxpool.Pool }

// NewVehicleDay builds the service.
func NewVehicleDay(pool *pgxpool.Pool) *VehicleDayService { return &VehicleDayService{pool: pool} }

// rollupSQL folds one local day into vehicle_day.
//
// The whole computation stays in SQL deliberately: the alternative is shipping
// ~1.6M rows/day to Go, which is what the per-vehicle report path already does
// over HTTP and why a fleet-wide number was previously unaffordable.
const rollupSQL = `
WITH bounds AS (
  SELECT ($1::date::timestamp AT TIME ZONE $2) AS lo,
         (($1::date + 1)::timestamp AT TIME ZONE $2) AS hi
),
steps AS (
  SELECT
    imei,
    time,
    speed,
    CASE WHEN speed > $3 THEN 'moving'
         WHEN ignition   THEN 'idle'
         ELSE 'parking' END AS state,
    -- Gap to the NEXT sample is this sample's duration, capped.
    --
    -- NOT LEAST(): in PostgreSQL LEAST skips NULLs rather than propagating
    -- them, so LEAST(NULL, 900) is 900. Each device's last sample of the day
    -- has no next sample and so a NULL gap — with LEAST it silently claimed a
    -- full 15-minute cap instead of nothing, inflating every device's day by
    -- exactly 0.25h and attributing it to whatever state the day ended in.
    -- Caught only because the totals came to 24.2 hours in a 24-hour day.
    CASE
      WHEN lead(time) OVER w IS NULL THEN NULL
      WHEN EXTRACT(epoch FROM lead(time) OVER w - time) > $4 THEN $4
      ELSE EXTRACT(epoch FROM lead(time) OVER w - time)
    END AS dt,
    ST_Distance(geom, lag(geom) OVER w) AS d_m
  FROM telemetry, bounds
  WHERE time >= bounds.lo AND time < bounds.hi
  WINDOW w AS (PARTITION BY imei ORDER BY time)
)
INSERT INTO vehicle_day AS vd
  (imei, day, moving_s, idle_s, parking_s, distance_km, max_speed, samples, first_seen, last_seen, final, updated_at)
SELECT
  imei,
  $1::date,
  COALESCE(sum(dt) FILTER (WHERE state = 'moving'  AND dt >= 0), 0)::bigint,
  COALESCE(sum(dt) FILTER (WHERE state = 'idle'    AND dt >= 0), 0)::bigint,
  COALESCE(sum(dt) FILTER (WHERE state = 'parking' AND dt >= 0), 0)::bigint,
  COALESCE(sum(d_m) FILTER (WHERE d_m < $5), 0) / 1000.0,
  max(speed),
  count(*)::bigint,
  min(time),
  max(time),
  $6,
  now()
FROM steps
GROUP BY imei
ON CONFLICT (imei, day) DO UPDATE SET
  moving_s    = EXCLUDED.moving_s,
  idle_s      = EXCLUDED.idle_s,
  parking_s   = EXCLUDED.parking_s,
  distance_km = EXCLUDED.distance_km,
  max_speed   = EXCLUDED.max_speed,
  samples     = EXCLUDED.samples,
  first_seen  = EXCLUDED.first_seen,
  last_seen   = EXCLUDED.last_seen,
  final       = EXCLUDED.final,
  updated_at  = now()`

// rollupDay recomputes one local day. Safe to re-run: the upsert REPLACES a
// day's row rather than adding to it, which is what makes recomputing today
// every hour correct rather than cumulative.
func (s *VehicleDayService) rollupDay(ctx context.Context, day time.Time, final bool) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	d := day.Format("2006-01-02")
	start := time.Now()
	tag, err := s.pool.Exec(ctx, rollupSQL, d, fleetTZ, movingKph, maxGapSec, maxJumpM, final)
	if err != nil {
		return fmt.Errorf("rollup %s: %w", d, err)
	}
	log.Printf("vehicle_day: %s -> %d devices in %s (final=%v)", d, tag.RowsAffected(), time.Since(start).Round(time.Second), final)
	return nil
}

// RunRollup seeds the hot window, then keeps today fresh.
//
// Past days are computed once and marked final; only today and yesterday are
// recomputed on each tick. Yesterday is included because a tick just after
// local midnight would otherwise freeze the last day while late-arriving
// samples were still being written.
func (s *VehicleDayService) RunRollup(ctx context.Context, hotDays int) {
	loc, err := time.LoadLocation(fleetTZ)
	if err != nil {
		log.Printf("vehicle_day: unknown timezone %q, rollup disabled: %v", fleetTZ, err)
		return
	}

	backfill := func() {
		today := time.Now().In(loc)
		for i := hotDays; i >= 1; i-- {
			day := today.AddDate(0, 0, -i)
			var done bool
			// A finalised day never changes — the chunk it came from is
			// compressed and, past the retention window, gone entirely.
			err := s.pool.QueryRow(ctx,
				`SELECT true FROM vehicle_day WHERE day = $1 AND final LIMIT 1`,
				day.Format("2006-01-02")).Scan(&done)
			if err == nil && done {
				continue
			}
			if err := s.rollupDay(ctx, day, true); err != nil {
				log.Printf("vehicle_day: %v", err)
			}
		}
	}
	refreshRecent := func() {
		now := time.Now().In(loc)
		if err := s.rollupDay(ctx, now, false); err != nil {
			log.Printf("vehicle_day: %v", err)
		}
		if err := s.rollupDay(ctx, now.AddDate(0, 0, -1), true); err != nil {
			log.Printf("vehicle_day: %v", err)
		}
	}

	backfill()
	refreshRecent()

	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			backfill() // picks up a day that rolled over
			refreshRecent()
		}
	}
}

// Route registers the read endpoint.
func (s *VehicleDayService) Route(r chiRouter) { r.Get("/v1/stats/daily", s.handleDaily) }

// handleDaily serves per-device daily rows for a date range.
//
// @Summary     Daily per-device time-in-state and distance
// @Description Pre-aggregated daily rollup: seconds moving/idle/parked, distance, max speed. Days are Asia/Jakarta.
// @Tags        stats
// @Produce     json
// @Param       from  query string true  "start day, YYYY-MM-DD (inclusive)"
// @Param       to    query string true  "end day, YYYY-MM-DD (inclusive)"
// @Param       imeis query string false "comma-separated IMEIs; omit for all"
// @Success     200 {array} VehicleDay
// @Failure     400 {object} map[string]string
// @Router      /v1/stats/daily [get]
func (s *VehicleDayService) handleDaily(w http.ResponseWriter, r *http.Request) {
	from, to := r.URL.Query().Get("from"), r.URL.Query().Get("to")
	if from == "" || to == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from and to are required (YYYY-MM-DD)"})
		return
	}
	for _, d := range []string{from, to} {
		if _, err := time.Parse("2006-01-02", d); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("bad date %q, want YYYY-MM-DD", d)})
			return
		}
	}

	var imeis []string
	if raw := r.URL.Query().Get("imeis"); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				imeis = append(imeis, p)
			}
		}
	}

	// $3 IS NULL means "all devices" — one query shape rather than two, so the
	// filtered and unfiltered paths cannot drift apart.
	rows, err := s.pool.Query(r.Context(), `
SELECT imei, day::text, moving_s, idle_s, parking_s, distance_km, max_speed, samples, first_seen, last_seen
FROM vehicle_day
WHERE day >= $1::date AND day <= $2::date
  AND ($3::text[] IS NULL OR imei = ANY($3))
ORDER BY day, imei`, from, to, imeis)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		log.Printf("vehicle_day: query: %v", err)
		return
	}
	defer rows.Close()

	out := make([]VehicleDay, 0, 4096)
	for rows.Next() {
		var v VehicleDay
		if err := rows.Scan(&v.IMEI, &v.Day, &v.MovingS, &v.IdleS, &v.ParkingS,
			&v.DistanceKm, &v.MaxSpeed, &v.Samples, &v.FirstSeen, &v.LastSeen); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "scan failed"})
			log.Printf("vehicle_day: scan: %v", err)
			return
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "read failed"})
		return
	}

	writeJSON(w, http.StatusOK, out)
}
