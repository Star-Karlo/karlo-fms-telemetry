package ingest

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// gridSize is the cell edge in degrees (~1.2 km near the equator).
const gridSize = 0.011

// Coverage builds and serves the signal + terrain-ruggedness spatial grid.
type Coverage struct{ pool *pgxpool.Pool }

// NewCoverage builds the coverage service.
func NewCoverage(pool *pgxpool.Pool) *Coverage { return &Coverage{pool: pool} }

// rollupDay folds one UTC day of telemetry into coverage_grid. Idempotent: a day
// already present in coverage_rollup_log is skipped (re-running would double
// count, since we keep only merged sums, not per-day contributions).
func (c *Coverage) rollupDay(ctx context.Context, day time.Time) error {
	d := day.UTC().Format("2006-01-02")
	var exists bool
	if err := c.pool.QueryRow(ctx, `SELECT true FROM coverage_rollup_log WHERE day = $1`, d).Scan(&exists); err == nil && exists {
		return nil // already rolled up
	}

	// Aggregate the day into grid cells, then upsert-merge into coverage_grid.
	// A single statement keeps it atomic and avoids shipping rows to Go.
	tag, err := c.pool.Exec(ctx, `
WITH pts AS (
  SELECT imei, time, geom,
    ST_Y(geom::geometry) AS lat, ST_X(geom::geometry) AS lon,
    NULLIF(metadata->>'GSM Signal','')::numeric AS sig,
    metadata->>'Active GSM Operator' AS op,
    NULLIF(metadata->>'altitude','')::numeric AS alt,
    -- Network Type: 1 = 2G/3G, 2 = LTE, 99 = no service. Only 4G-capable
    -- radios emit it at all, so a non-null reading self-selects hardware that
    -- could have seen LTE here — which is exactly the population we may count.
    NULLIF(metadata->>'Network Type','')::int AS nt
  FROM telemetry
  WHERE time >= $1::date AND time < ($1::date + INTERVAL '1 day') AND geom IS NOT NULL
),
seq AS (
  SELECT *, alt - lag(alt) OVER w AS dalt, ST_Distance(geom, lag(geom) OVER w) AS dist
  FROM pts WINDOW w AS (PARTITION BY imei ORDER BY time)
),
agg AS (
  SELECT
    floor(lat/$2)*$2 AS cell_lat,
    floor(lon/$2)*$2 AS cell_lon,
    count(*)::bigint AS pings,
    coalesce(sum(sig),0)::float8 AS sig_sum,
    count(sig)::bigint AS sig_cnt,
    coalesce(sum(sig) FILTER (WHERE op='Telkomsel'),0)::float8 AS tsel_sum,
    count(sig) FILTER (WHERE op='Telkomsel')::bigint AS tsel_cnt,
    coalesce(sum(sig) FILTER (WHERE op='Indosat'),0)::float8 AS isat_sum,
    count(sig) FILTER (WHERE op='Indosat')::bigint AS isat_cnt,
    coalesce(sum(sig) FILTER (WHERE op='XL'),0)::float8 AS xl_sum,
    count(sig) FILTER (WHERE op='XL')::bigint AS xl_cnt,
    count(*) FILTER (WHERE sig <= 1 OR op = '0')::bigint AS weak,
    coalesce(sum(abs(dalt)) FILTER (WHERE dist BETWEEN 5 AND 1000 AND abs(dalt) < 200),0)::float8 AS rug_num,
    coalesce(sum(dist)      FILTER (WHERE dist BETWEEN 5 AND 1000 AND abs(dalt) < 200),0)::float8 AS rug_den,
    -- real elevation (guard obvious bad fixes: Indonesia is ~ -100..4000 m)
    coalesce(sum(alt) FILTER (WHERE alt BETWEEN -100 AND 4000),0)::float8 AS alt_sum,
    count(alt)        FILTER (WHERE alt BETWEEN -100 AND 4000)::bigint    AS alt_cnt,
    -- 4G availability, measured only by radios that can actually see 4G.
    count(*) FILTER (WHERE nt IN (1,2,99))::bigint AS lte_seen,
    count(*) FILTER (WHERE nt = 2)::bigint         AS lte_on
  FROM seq
  GROUP BY 1,2
)
INSERT INTO coverage_grid AS g
  (cell_lat,cell_lon,pings,sig_sum,sig_cnt,tsel_sum,tsel_cnt,isat_sum,isat_cnt,xl_sum,xl_cnt,weak,rug_num,rug_den,alt_sum,alt_cnt,lte_seen,lte_on,updated_at)
SELECT cell_lat,cell_lon,pings,sig_sum,sig_cnt,tsel_sum,tsel_cnt,isat_sum,isat_cnt,xl_sum,xl_cnt,weak,rug_num,rug_den,alt_sum,alt_cnt,lte_seen,lte_on, now()
FROM agg
ON CONFLICT (cell_lat,cell_lon) DO UPDATE SET
  alt_sum  = g.alt_sum  + EXCLUDED.alt_sum,
  alt_cnt  = g.alt_cnt  + EXCLUDED.alt_cnt,
  lte_seen = g.lte_seen + EXCLUDED.lte_seen,
  lte_on   = g.lte_on   + EXCLUDED.lte_on,
  pings    = g.pings    + EXCLUDED.pings,
  sig_sum  = g.sig_sum  + EXCLUDED.sig_sum,
  sig_cnt  = g.sig_cnt  + EXCLUDED.sig_cnt,
  tsel_sum = g.tsel_sum + EXCLUDED.tsel_sum,
  tsel_cnt = g.tsel_cnt + EXCLUDED.tsel_cnt,
  isat_sum = g.isat_sum + EXCLUDED.isat_sum,
  isat_cnt = g.isat_cnt + EXCLUDED.isat_cnt,
  xl_sum   = g.xl_sum   + EXCLUDED.xl_sum,
  xl_cnt   = g.xl_cnt   + EXCLUDED.xl_cnt,
  weak     = g.weak     + EXCLUDED.weak,
  rug_num  = g.rug_num  + EXCLUDED.rug_num,
  rug_den  = g.rug_den  + EXCLUDED.rug_den,
  updated_at = now()`, d, gridSize)
	if err != nil {
		return fmt.Errorf("aggregate %s: %w", d, err)
	}
	cells := tag.RowsAffected()

	// Second pass: the per-(generation, carrier) signal breakdown. Kept separate
	// from the wide grid above because it fans each cell out into up to 8 rows.
	if _, err := c.pool.Exec(ctx, `
WITH pts AS (
  SELECT ST_Y(t.geom::geometry) AS lat, ST_X(t.geom::geometry) AS lon,
    NULLIF(t.metadata->>'GSM Signal','')::numeric AS sig,
    CASE t.metadata->>'Active GSM Operator'
      WHEN 'Telkomsel' THEN 'tsel' WHEN 'Indosat' THEN 'isat' WHEN 'XL' THEN 'xl'
      ELSE 'unk' END AS prov,
    -- Absent from device_generation = 4G-capable, per the fleet owner's list.
    CASE WHEN dg.generation = '2g' THEN '2g' ELSE '4g' END AS gen
  FROM telemetry t
  LEFT JOIN device_generation dg ON dg.imei = t.imei
  WHERE t.time >= $1::date AND t.time < ($1::date + INTERVAL '1 day') AND t.geom IS NOT NULL
)
INSERT INTO coverage_signal AS s (cell_lat,cell_lon,gen,provider,sig_sum,sig_cnt,weak,pings,updated_at)
SELECT floor(lat/$2)*$2, floor(lon/$2)*$2, gen, prov,
  coalesce(sum(sig),0)::float8, count(sig)::bigint,
  count(*) FILTER (WHERE sig <= 1)::bigint, count(*)::bigint, now()
FROM pts GROUP BY 1,2,3,4
ON CONFLICT (cell_lat,cell_lon,gen,provider) DO UPDATE SET
  sig_sum = s.sig_sum + EXCLUDED.sig_sum,
  sig_cnt = s.sig_cnt + EXCLUDED.sig_cnt,
  weak    = s.weak    + EXCLUDED.weak,
  pings   = s.pings   + EXCLUDED.pings,
  updated_at = now()`, d, gridSize); err != nil {
		return fmt.Errorf("signal breakdown %s: %w", d, err)
	}

	if _, err := c.pool.Exec(ctx,
		`INSERT INTO coverage_rollup_log (day, pings, cells) VALUES ($1, 0, $2)
		 ON CONFLICT (day) DO NOTHING`, d, cells); err != nil {
		return fmt.Errorf("log %s: %w", d, err)
	}
	log.Printf("coverage: rolled up %s (%d cells touched)", d, cells)
	return nil
}

// RunRollup seeds any un-rolled days in the hot window on startup, then rolls up
// the previous day once every 24h. Older history (beyond hot) can be seeded
// separately from cold; the hot window alone is already dense coverage.
func (c *Coverage) RunRollup(ctx context.Context, hotDays int) {
	catchUp := func() {
		today := time.Now().UTC().Truncate(24 * time.Hour)
		for i := hotDays; i >= 1; i-- {
			day := today.AddDate(0, 0, -i)
			if err := c.rollupDay(ctx, day); err != nil {
				log.Printf("coverage: rollup %s: %v", day.Format("2006-01-02"), err)
			}
		}
	}
	catchUp() // seed on startup
	t := time.NewTicker(6 * time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			catchUp()
		}
	}
}

// ---- read endpoint ----

// handleCoverage godoc
//
//	@Summary		Signal / terrain / altitude coverage grid (GeoJSON)
//	@Description	~1.2km grid cells. metric=signal returns avg GSM signal (0-5) + dead-zone ratio (provider is a comma-separated subset, combined); metric=terrain returns ruggedness (metres of elevation change per km driven); metric=altitude returns mean elevation in metres. Optional bbox=minLon,minLat,maxLon,maxLat.
//	@Tags			coverage
//	@Produce		json
//	@Param			metric		query	string	false	"signal | terrain | altitude | lte (default signal)"
//	@Param			provider	query	string	false	"comma-separated: telkomsel,indosat,xl — omit for every operator incl. unreported (signal only)"
//	@Param			gen			query	string	false	"comma-separated: 2g,4g — omit for both device generations (signal only)"
//	@Param			bbox		query	string	false	"minLon,minLat,maxLon,maxLat"
//	@Success		200	{object}	map[string]interface{}
//	@Router			/v1/coverage [get]
func (c *Coverage) handleCoverage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == "" {
		metric = "signal"
	}

	// The grid only changes when the nightly rollup runs, so this is very
	// cacheable — an hour of browser/CDN caching removes almost all repeat load.
	w.Header().Set("Cache-Control", "public, max-age=3600")

	if metric == "signal" {
		c.signalGrid(w, r)
		return
	}

	where := "pings >= 3"
	args := []any{}
	if bbox := q.Get("bbox"); bbox != "" {
		p := strings.Split(bbox, ",")
		if len(p) == 4 {
			vals := make([]float64, 4)
			ok := true
			for i, s := range p {
				v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
				if err != nil {
					ok = false
					break
				}
				vals[i] = v
			}
			if ok {
				where += fmt.Sprintf(" AND cell_lon >= $%d AND cell_lat >= $%d AND cell_lon <= $%d AND cell_lat <= $%d",
					len(args)+1, len(args)+2, len(args)+3, len(args)+4)
				args = append(args, vals[0], vals[1], vals[2], vals[3])
			}
		}
	}

	rows, err := c.pool.Query(r.Context(),
		`SELECT cell_lat,cell_lon,pings,rug_num,rug_den,alt_sum,alt_cnt,lte_seen,lte_on
		 FROM coverage_grid WHERE `+where, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	features := []feat{}
	for rows.Next() {
		var clat, clon, rugNum, rugDen, altSum float64
		var pings, altCnt, lteSeen, lteOn int64
		if err := rows.Scan(&clat, &clon, &pings, &rugNum, &rugDen, &altSum, &altCnt, &lteSeen, &lteOn); err != nil {
			continue
		}
		var value *float64
		props := map[string]any{"pings": pings}
		switch metric {
		case "terrain":
			if rugDen > 0 {
				v := math.Round(1000*rugNum/rugDen*10) / 10
				value = &v
			}
			props["rugged_m_per_km"] = value
		case "altitude":
			if altCnt > 0 {
				v := math.Round(altSum / float64(altCnt))
				value = &v
			}
			props["altitude_m"] = value
		case "lte":
			// Percent of readings on LTE, among 4G-capable radios only. Cells
			// surveyed solely by 2G-only trucks have lte_seen = 0 and drop out
			// entirely — "never measured" must not render as "no 4G here".
			// Require a few readings so one stray ping can't paint a cell.
			if lteSeen >= 5 {
				v := math.Round(float64(lteOn) / float64(lteSeen) * 100)
				value = &v
			}
			props["lte_pct"] = value
			props["lte_seen"] = lteSeen
		}
		if value == nil {
			continue // no data for this metric in this cell
		}
		props["value"] = value
		features = append(features, cellFeature(clat, clon, props))
	}
	writeJSON(w, http.StatusOK, map[string]any{"type": "FeatureCollection", "metric": metric, "features": features})
}

// feat is one GeoJSON cell.
type feat struct {
	Type       string         `json:"type"`
	Geometry   map[string]any `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

// cellFeature builds the square polygon for a grid cell (SW corner + one step).
// Coordinates are rounded to 4dp (~11 m, far finer than the 1.2 km cell) — full
// float64 precision would bloat the payload ~40% for zero visible benefit.
func cellFeature(clat, clon float64, props map[string]any) feat {
	r4 := func(f float64) float64 { return math.Round(f*10000) / 10000 }
	x0, y0 := r4(clon), r4(clat)
	x1, y1 := r4(clon+gridSize), r4(clat+gridSize)
	return feat{
		Type:       "Feature",
		Geometry:   map[string]any{"type": "Polygon", "coordinates": [][][]float64{{{x0, y0}, {x1, y0}, {x1, y1}, {x0, y1}, {x0, y0}}}},
		Properties: props,
	}
}

// signalGrid serves metric=signal from the per-(generation, carrier) breakdown,
// re-aggregating whichever combination the caller ticked.
//
// Omitting a filter means "everything" — deliberately NOT the same as listing
// all named carriers, because ~15% of readings carry no operator at all and
// would otherwise vanish from the default view.
func (c *Coverage) signalGrid(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	csv := func(key string, allowed map[string]string) []string {
		out := []string{}
		for _, p := range strings.Split(strings.ToLower(q.Get(key)), ",") {
			if v, ok := allowed[strings.TrimSpace(p)]; ok {
				out = append(out, v)
			}
		}
		return out
	}
	gens := csv("gen", map[string]string{"2g": "2g", "4g": "4g"})
	provs := csv("provider", map[string]string{"telkomsel": "tsel", "indosat": "isat", "xl": "xl"})

	where := []string{"true"}
	args := []any{}
	if len(gens) > 0 && len(gens) < 2 {
		args = append(args, gens)
		where = append(where, fmt.Sprintf("gen = ANY($%d)", len(args)))
	}
	if len(provs) > 0 && len(provs) < 3 {
		args = append(args, provs)
		where = append(where, fmt.Sprintf("provider = ANY($%d)", len(args)))
	}
	if bbox := q.Get("bbox"); bbox != "" {
		if p := strings.Split(bbox, ","); len(p) == 4 {
			vals := make([]float64, 4)
			ok := true
			for i, s := range p {
				v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
				if err != nil {
					ok = false
					break
				}
				vals[i] = v
			}
			if ok {
				where = append(where, fmt.Sprintf("cell_lon >= $%d AND cell_lat >= $%d AND cell_lon <= $%d AND cell_lat <= $%d",
					len(args)+1, len(args)+2, len(args)+3, len(args)+4))
				args = append(args, vals[0], vals[1], vals[2], vals[3])
			}
		}
	}

	rows, err := c.pool.Query(r.Context(), `
SELECT cell_lat, cell_lon, sum(sig_sum), sum(sig_cnt), sum(weak), sum(pings)
FROM coverage_signal WHERE `+strings.Join(where, " AND ")+`
GROUP BY cell_lat, cell_lon
HAVING sum(sig_cnt) > 0 AND sum(pings) >= 3`, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	features := []feat{}
	for rows.Next() {
		var clat, clon, sigSum float64
		var sigCnt, weak, pings int64
		if err := rows.Scan(&clat, &clon, &sigSum, &sigCnt, &weak, &pings); err != nil {
			continue
		}
		v := math.Round(sigSum/float64(sigCnt)*100) / 100
		props := map[string]any{
			"pings":      pings,
			"avg_signal": v,
			"value":      v,
			"dead_ratio": math.Round(float64(weak)/float64(pings)*100) / 100,
		}
		features = append(features, cellFeature(clat, clon, props))
	}
	writeJSON(w, http.StatusOK, map[string]any{"type": "FeatureCollection", "metric": "signal", "features": features})
}

// Route registers the public coverage endpoint.
func (c *Coverage) Route(r chiRouter) { r.Get("/v1/coverage", c.handleCoverage) }

// chiRouter is the tiny slice of chi.Router we need (avoids an import here).
type chiRouter interface {
	Get(pattern string, h http.HandlerFunc)
}
