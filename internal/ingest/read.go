package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHistoryLimit = 1000
	maxHistoryLimit     = 10000
)

// Position is a device reading returned by the read endpoints (geom flattened
// to lat/lon). Nullable telemetry fields come back as null.
type Position struct {
	IMEI       string          `json:"imei"`
	Time       time.Time       `json:"time"` // gpsCreatedAt
	ReceivedAt *time.Time      `json:"received_at"`
	Lat        *float64        `json:"lat"`
	Lon        *float64        `json:"lon"`
	Speed      *float64        `json:"speed"`
	Bearing    *float64        `json:"bearing"`
	Ignition   *bool           `json:"ignition"`
	Kecamatan  string          `json:"kecamatan"`
	Kelurahan  string          `json:"kelurahan"`
	Kabupaten  string          `json:"kabupaten"`
	Kota       string          `json:"kota"`
	Provinsi   string          `json:"provinsi"`
	Jalan      string          `json:"jalan"`
	Metadata   json.RawMessage `json:"metadata" swaggertype:"object"` // vendor io + extras (raw payload excluded)
}

// Reader serves live (current state) and historical telemetry. History reads
// hot data from Timescale and, transparently, cold data (older than the
// retention window) from Athena/Parquet when a ColdReader is configured.
type Reader struct {
	pool          *pgxpool.Pool
	cold          *ColdReader // nil => hot-only
	retentionDays int
}

func NewReader(pool *pgxpool.Pool, cold *ColdReader, retentionDays int) *Reader {
	return &Reader{pool: pool, cold: cold, retentionDays: retentionDays}
}

const posCols = `imei, time, received_at, ST_Y(geom::geometry), ST_X(geom::geometry),
                 speed, bearing, ignition,
                 coalesce(kecamatan,''), coalesce(kelurahan,''), coalesce(kabupaten,''),
                 coalesce(kota,''), coalesce(provinsi,''), coalesce(jalan,''),
                 coalesce(metadata, '{}'::jsonb) - '_raw'`

// ---- field projection (?fields=) -------------------------------------------
//
// A caller can restrict the columns a read returns. The big win is dropping
// `metadata` (a KB-sized jsonb per row) so the columnar-compressed hypertable
// never decompresses it. `imei` and `time` are always present (time is the
// ordering + cursor key). hotExpr is aliased to the field name so a generic
// scan can key results by column name.
var fieldHotExpr = map[string]string{
	"received_at": "received_at",
	"lat":         "ST_Y(geom::geometry)",
	"lon":         "ST_X(geom::geometry)",
	"speed":       "speed::float8",
	"bearing":     "bearing::float8",
	"ignition":    "ignition",
	"kecamatan":   "coalesce(kecamatan,'')",
	"kelurahan":   "coalesce(kelurahan,'')",
	"kabupaten":   "coalesce(kabupaten,'')",
	"kota":        "coalesce(kota,'')",
	"provinsi":    "coalesce(provinsi,'')",
	"jalan":       "coalesce(jalan,'')",
	"metadata":    "(coalesce(metadata,'{}'::jsonb) - '_raw')::text",
}

// fieldOrder gives the default (all-fields) projection and a stable ordering.
var fieldOrder = []string{
	"received_at", "lat", "lon", "speed", "bearing", "ignition",
	"kecamatan", "kelurahan", "kabupaten", "kota", "provinsi", "jalan", "metadata",
}

// projection is the validated set of output fields (excluding the always-present
// imei/time), kept in fieldOrder order.
type projection struct{ fields []string }

// parseProjection turns a comma-separated ?fields= value into a projection.
// Empty ⇒ all fields. Unknown field ⇒ error (400 upstream).
func parseProjection(raw string) (projection, error) {
	if strings.TrimSpace(raw) == "" {
		return projection{fields: append([]string(nil), fieldOrder...)}, nil
	}
	want := map[string]bool{}
	for _, f := range strings.Split(raw, ",") {
		f = strings.TrimSpace(f)
		if f == "" || f == "imei" || f == "time" { // always included implicitly
			continue
		}
		if _, ok := fieldHotExpr[f]; !ok {
			return projection{}, fmt.Errorf("unknown field %q", f)
		}
		want[f] = true
	}
	var fs []string
	for _, f := range fieldOrder {
		if want[f] {
			fs = append(fs, f)
		}
	}
	return projection{fields: fs}, nil
}

// hotSelect is the SELECT column list for telemetry/latest: imei, time, then the
// projected expressions aliased to their field names.
func (p projection) hotSelect() string {
	cols := []string{"imei", "time"}
	for _, f := range p.fields {
		cols = append(cols, fieldHotExpr[f]+" AS "+f)
	}
	return strings.Join(cols, ", ")
}

// scanProjected reads rows into per-row maps keyed by column name. metadata
// (selected as text) is wrapped as json.RawMessage so it serializes as raw JSON.
func scanProjected(rows pgx.Rows) ([]map[string]any, error) {
	fds := rows.FieldDescriptions()
	out := make([]map[string]any, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		m := make(map[string]any, len(vals))
		for i, fd := range fds {
			name := fd.Name
			v := vals[i]
			if name == "metadata" {
				switch t := v.(type) {
				case string:
					v = json.RawMessage(t)
				case []byte:
					v = json.RawMessage(t)
				}
			}
			m[name] = v
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// positionsToMaps projects fully-scanned Positions (from the cold reader) down
// to the requested fields, matching the shape scanProjected produces.
func positionsToMaps(ps []Position, fields []string) []map[string]any {
	out := make([]map[string]any, 0, len(ps))
	for _, p := range ps {
		m := map[string]any{"imei": p.IMEI, "time": p.Time}
		for _, f := range fields {
			switch f {
			case "received_at":
				m[f] = p.ReceivedAt
			case "lat":
				m[f] = p.Lat
			case "lon":
				m[f] = p.Lon
			case "speed":
				m[f] = p.Speed
			case "bearing":
				m[f] = p.Bearing
			case "ignition":
				m[f] = p.Ignition
			case "kecamatan":
				m[f] = p.Kecamatan
			case "kelurahan":
				m[f] = p.Kelurahan
			case "kabupaten":
				m[f] = p.Kabupaten
			case "kota":
				m[f] = p.Kota
			case "provinsi":
				m[f] = p.Provinsi
			case "jalan":
				m[f] = p.Jalan
			case "metadata":
				m[f] = p.Metadata
			}
		}
		out = append(out, m)
	}
	return out
}

func nonNilMaps(p []map[string]any) []map[string]any {
	if p == nil {
		return []map[string]any{}
	}
	return p
}

func scanPositions(rows pgx.Rows) ([]Position, error) {
	out := make([]Position, 0)
	for rows.Next() {
		var p Position
		if err := rows.Scan(&p.IMEI, &p.Time, &p.ReceivedAt, &p.Lat, &p.Lon,
			&p.Speed, &p.Bearing, &p.Ignition,
			&p.Kecamatan, &p.Kelurahan, &p.Kabupaten, &p.Kota, &p.Provinsi, &p.Jalan,
			&p.Metadata); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (rd *Reader) LiveAll(ctx context.Context) ([]Position, error) {
	rows, err := rd.pool.Query(ctx, `SELECT `+posCols+` FROM latest ORDER BY imei`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPositions(rows)
}

func (rd *Reader) LiveOne(ctx context.Context, imei string) (*Position, error) {
	rows, err := rd.pool.Query(ctx, `SELECT `+posCols+` FROM latest WHERE imei = $1`, imei)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ps, err := scanPositions(rows)
	if err != nil || len(ps) == 0 {
		return nil, err
	}
	return &ps[0], nil
}

// HistoryPage is one page of a device's track. Points is []Position by default,
// or []map[string]any when ?fields= / ?max_points= is used. NextCursor is the
// RFC3339 time to pass back as ?cursor= for the next page; "" means no more data
// (also empty in downsample mode, which returns the whole range at once).
type HistoryPage struct {
	Points     any    `json:"points" swaggertype:"array,object"`
	NextCursor string `json:"next_cursor"`
}

// hotHistory queries the Timescale telemetry hypertable for imei in the window,
// ascending, up to limit rows. lowerStrict makes the lower bound exclusive.
func (rd *Reader) hotHistory(ctx context.Context, imei string, lower, upper time.Time, lowerStrict bool, limit int) ([]Position, error) {
	loOp := ">="
	if lowerStrict {
		loOp = ">"
	}
	q := `SELECT ` + posCols + ` FROM telemetry
	      WHERE imei = $1 AND time ` + loOp + ` $2 AND time <= $3 ORDER BY time LIMIT $4`
	rows, err := rd.pool.Query(ctx, q, imei, lower, upper, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPositions(rows)
}

// History returns a page of a device's track in (cursor|from .. to], ascending,
// transparently merging COLD storage (Athena/Parquet, older than the retention
// window) with HOT storage (Timescale). Over-fetches by one to detect more.
func (rd *Reader) History(ctx context.Context, imei string, from, to, cursor time.Time, limit int) (HistoryPage, error) {
	lower, strict := from, false
	if !cursor.IsZero() {
		lower, strict = cursor, true
	}
	boundary := time.Now().UTC().AddDate(0, 0, -rd.retentionDays)
	need := limit + 1

	var pts []Position

	// COLD: [lower, min(to, boundary)) — older than the retention window.
	if rd.cold != nil && lower.Before(boundary) {
		upper, upperExcl := to, false
		if !to.Before(boundary) { // capped at the boundary (exclusive; hot owns it)
			upper, upperExcl = boundary, true
		}
		cps, err := rd.cold.History(ctx, imei, lower, upper, strict, upperExcl, need)
		if err != nil {
			return HistoryPage{}, err
		}
		pts = append(pts, cps...)
	}

	// HOT: [max(lower, boundary), to] — within the retention window.
	if len(pts) < need && !to.Before(boundary) {
		hl, hs := lower, strict
		if hl.Before(boundary) { // hot segment starts at the boundary (inclusive)
			hl, hs = boundary, false
		}
		hps, err := rd.hotHistory(ctx, imei, hl, to, hs, need-len(pts))
		if err != nil {
			return HistoryPage{}, err
		}
		pts = append(pts, hps...)
	}

	page := HistoryPage{Points: pts}
	if len(pts) > limit { // more remains — trim and expose a cursor
		page.Points = pts[:limit]
		page.NextCursor = pts[limit-1].Time.UTC().Format(time.RFC3339Nano)
	}
	return page, nil
}

// hotHistoryProjected is hotHistory with a caller-chosen column set (no metadata
// decompression unless requested).
func (rd *Reader) hotHistoryProjected(ctx context.Context, imei string, lower, upper time.Time, lowerStrict bool, proj projection, limit int) ([]map[string]any, error) {
	loOp := ">="
	if lowerStrict {
		loOp = ">"
	}
	q := `SELECT ` + proj.hotSelect() + ` FROM telemetry
	      WHERE imei = $1 AND time ` + loOp + ` $2 AND time <= $3 ORDER BY time LIMIT $4`
	rows, err := rd.pool.Query(ctx, q, imei, lower, upper, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProjected(rows)
}

// hotLTTB downsamples the hot segment in-database to ~n shape-preserving points
// via TimescaleDB toolkit lttb() (driven on latitude), returning the projected
// columns for the selected fixes. lttb() naturally returns <= the raw count, so
// short ranges come back whole.
func (rd *Reader) hotLTTB(ctx context.Context, imei string, lower, upper time.Time, lowerStrict bool, proj projection, n int) ([]map[string]any, error) {
	if n < 2 {
		n = 2
	}
	loOp := ">="
	if lowerStrict {
		loOp = ">"
	}
	q := `WITH s AS (
	        SELECT (unnest(lttb(time, ST_Y(geom::geometry), $4))).time AS t
	        FROM telemetry
	        WHERE imei = $1 AND time ` + loOp + ` $2 AND time <= $3)
	      SELECT ` + proj.hotSelect() + ` FROM telemetry
	      WHERE imei = $1 AND time IN (SELECT t FROM s) ORDER BY time`
	rows, err := rd.pool.Query(ctx, q, imei, lower, upper, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProjected(rows)
}

// HistoryProjected serves history with field projection and/or LTTB downsampling.
// maxPoints > 0 ⇒ downsample the whole range (no pagination); otherwise it mirrors
// History (dual-read + cursor pagination) but returns projected rows.
func (rd *Reader) HistoryProjected(ctx context.Context, imei string, from, to, cursor time.Time, limit int, proj projection, maxPoints int) (HistoryPage, error) {
	lower, strict := from, false
	if !cursor.IsZero() {
		lower, strict = cursor, true
	}
	boundary := time.Now().UTC().AddDate(0, 0, -rd.retentionDays)

	// ---- downsample mode: bounded result, no cursor ----
	if maxPoints > 0 {
		var pts []map[string]any
		// COLD [lower, min(to, boundary)) — Athena has no lttb(); downsample in Go.
		if rd.cold != nil && lower.Before(boundary) {
			upper, upperExcl := to, false
			if !to.Before(boundary) {
				upper, upperExcl = boundary, true
			}
			cps, err := rd.cold.History(ctx, imei, lower, upper, strict, upperExcl, maxHistoryLimit)
			if err != nil {
				return HistoryPage{}, err
			}
			pts = append(pts, downsampleLTTB(positionsToMaps(cps, proj.fields), maxPoints)...)
		}
		// HOT [max(lower, boundary), to] — downsample in-database.
		if !to.Before(boundary) {
			hl, hs := lower, strict
			if hl.Before(boundary) {
				hl, hs = boundary, false
			}
			hps, err := rd.hotLTTB(ctx, imei, hl, to, hs, proj, maxPoints)
			if err != nil {
				return HistoryPage{}, err
			}
			pts = append(pts, hps...)
		}
		if len(pts) > maxPoints { // merged cold+hot may exceed the target
			pts = downsampleLTTB(pts, maxPoints)
		}
		return HistoryPage{Points: nonNilMaps(pts)}, nil
	}

	// ---- raw projected mode: dual-read + cursor pagination ----
	need := limit + 1
	var pts []map[string]any
	if rd.cold != nil && lower.Before(boundary) {
		upper, upperExcl := to, false
		if !to.Before(boundary) {
			upper, upperExcl = boundary, true
		}
		cps, err := rd.cold.History(ctx, imei, lower, upper, strict, upperExcl, need)
		if err != nil {
			return HistoryPage{}, err
		}
		pts = append(pts, positionsToMaps(cps, proj.fields)...)
	}
	if len(pts) < need && !to.Before(boundary) {
		hl, hs := lower, strict
		if hl.Before(boundary) {
			hl, hs = boundary, false
		}
		hps, err := rd.hotHistoryProjected(ctx, imei, hl, to, hs, proj, need-len(pts))
		if err != nil {
			return HistoryPage{}, err
		}
		pts = append(pts, hps...)
	}

	page := HistoryPage{Points: nonNilMaps(pts)}
	if len(pts) > limit {
		page.Points = pts[:limit]
		if t, ok := pts[limit-1]["time"].(time.Time); ok {
			page.NextCursor = t.UTC().Format(time.RFC3339Nano)
		}
	}
	return page, nil
}

// LiveAllProjected is LiveAll with a caller-chosen column set (e.g. drop metadata
// from the map feed).
func (rd *Reader) LiveAllProjected(ctx context.Context, proj projection) ([]map[string]any, error) {
	rows, err := rd.pool.Query(ctx, `SELECT `+proj.hotSelect()+` FROM latest ORDER BY imei`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProjected(rows)
}

// GeoPos is a minimal live position (for geofence distance checks).
type GeoPos struct {
	Lat, Lon float64
	Time     time.Time
}

// LivePositions returns the latest lat/lon (+fix time) for the given imeis —
// only those with a live row. Lean (no metadata/geocode) for large batches.
func (rd *Reader) LivePositions(ctx context.Context, imeis []string) (map[string]GeoPos, error) {
	rows, err := rd.pool.Query(ctx,
		`SELECT imei, ST_Y(geom::geometry), ST_X(geom::geometry), time
		 FROM latest WHERE imei = ANY($1) AND geom IS NOT NULL`, imeis)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]GeoPos, len(imeis))
	for rows.Next() {
		var imei string
		var p GeoPos
		if err := rows.Scan(&imei, &p.Lat, &p.Lon, &p.Time); err != nil {
			return nil, err
		}
		out[imei] = p
	}
	return out, rows.Err()
}

// LiveMany returns the latest position of a specific set of devices (full fields).
func (rd *Reader) LiveMany(ctx context.Context, imeis []string) ([]Position, error) {
	rows, err := rd.pool.Query(ctx, `SELECT `+posCols+` FROM latest WHERE imei = ANY($1) ORDER BY imei`, imeis)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPositions(rows)
}

// LiveManyProjected returns the latest position of a specific set of devices with
// a caller-chosen column set — the quickest path for a map of N vehicles
// (?imeis=a,b,c&fields=lat,lon).
func (rd *Reader) LiveManyProjected(ctx context.Context, imeis []string, proj projection) ([]map[string]any, error) {
	rows, err := rd.pool.Query(ctx, `SELECT `+proj.hotSelect()+` FROM latest WHERE imei = ANY($1) ORDER BY imei`, imeis)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProjected(rows)
}

// ---- HTTP handlers ----

const maxLiveIMEIs = 5000

// handleLiveAll godoc
//
//	@Summary	Latest position of devices (all, or a specific set)
//	@Description	Returns the latest fix per device. Use ?imeis= to restrict to a set, and ?fields= to trim columns (e.g. a lightweight map feed: ?fields=lat,lon).
//	@Tags		read
//	@Produce	json
//	@Param		imeis	query	string	false	"Comma-separated IMEIs to return (omit for all devices). Max 5000."
//	@Param		fields	query	string	false	"Comma-separated columns to return (e.g. lat,lon,speed); omit for all. imei+time always included."
//	@Success	200	{array}	Position
//	@Failure	400	{object}	map[string]string
//	@Router		/v1/live [get]
func (h *Handler) handleLiveAll(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ctx := r.Context()

	// Optional device subset.
	var imeis []string
	if raw := q.Get("imeis"); raw != "" {
		for _, s := range strings.Split(raw, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if !validIMEI(s) {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid imei: " + s})
				return
			}
			imeis = append(imeis, s)
		}
		if len(imeis) > maxLiveIMEIs {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "too many imeis (max 5000)"})
			return
		}
	}

	// Optional column projection.
	if fieldsRaw := q.Get("fields"); fieldsRaw != "" {
		proj, err := parseProjection(fieldsRaw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var ps []map[string]any
		if imeis != nil {
			ps, err = h.r.LiveManyProjected(ctx, imeis, proj)
		} else {
			ps, err = h.r.LiveAllProjected(ctx, proj)
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
		writeJSON(w, http.StatusOK, nonNilMaps(ps))
		return
	}

	// Full fields.
	var ps []Position
	var err error
	if imeis != nil {
		ps, err = h.r.LiveMany(ctx, imeis)
	} else {
		ps, err = h.r.LiveAll(ctx)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	writeJSON(w, http.StatusOK, ps)
}

// handleLiveOne godoc
//
//	@Summary	Latest position of one device
//	@Tags		read
//	@Produce	json
//	@Param		imei	path		string	true	"Device IMEI"
//	@Success	200		{object}	Position
//	@Failure	404		{object}	map[string]string
//	@Router		/v1/live/{imei} [get]
func (h *Handler) handleLiveOne(w http.ResponseWriter, r *http.Request) {
	imei := chi.URLParam(r, "imei")
	if !validIMEI(imei) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid imei"})
		return
	}
	p, err := h.r.LiveOne(r.Context(), imei)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	if p == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no data for imei"})
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// handleHistory godoc
//
//	@Summary	Historical track for a device over a time range
//	@Tags		read
//	@Produce	json
//	@Param		imei	path		string	true	"Device IMEI"
//	@Param		from	query		string	false	"Start time (RFC3339); default now-24h"
//	@Param		to		query		string	false	"End time (RFC3339); default now"
//	@Param		limit	query		int		false	"Page size (default 1000, max 10000)"
//	@Param		cursor	query		string	false	"Pass the previous response's next_cursor to get the next page"
//	@Param		fields	query		string	false	"Comma-separated columns to return (e.g. lat,lon,speed); omit for all. imei+time always included. Dropping metadata is the biggest speed/size win."
//	@Param		max_points	query	int		false	"Downsample the whole range to ~N shape-preserving points (LTTB). When set, pagination is bypassed."
//	@Success	200		{object}	HistoryPage
//	@Failure	400		{object}	map[string]string
//	@Router		/v1/history/{imei} [get]
func (h *Handler) handleHistory(w http.ResponseWriter, r *http.Request) {
	imei := chi.URLParam(r, "imei")
	if !validIMEI(imei) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid imei"})
		return
	}
	q := r.URL.Query()

	to := time.Now().UTC()
	from := to.Add(-24 * time.Hour)
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from must be RFC3339"})
			return
		}
		from = t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "to must be RFC3339"})
			return
		}
		to = t
	}
	var cursor time.Time
	if v := q.Get("cursor"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cursor must be RFC3339"})
			return
		}
		cursor = t
	}
	limit := defaultHistoryLimit
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}

	fieldsRaw := q.Get("fields")
	maxPoints := 0
	if v := q.Get("max_points"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_points must be a positive integer"})
			return
		}
		maxPoints = n
		if maxPoints > maxHistoryLimit {
			maxPoints = maxHistoryLimit
		}
	}

	// Fast path unchanged when neither projection nor downsampling is requested.
	if fieldsRaw == "" && maxPoints == 0 {
		page, err := h.r.History(r.Context(), imei, from, to, cursor, limit)
		if err != nil {
			log.Printf("history %s: %v", imei, err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}

	proj, err := parseProjection(fieldsRaw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	page, err := h.r.HistoryProjected(r.Context(), imei, from, to, cursor, limit, proj, maxPoints)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	writeJSON(w, http.StatusOK, page)
}
