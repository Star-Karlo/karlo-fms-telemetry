package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
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
	IMEI       string     `json:"imei"`
	Time       time.Time  `json:"time"` // gpsCreatedAt
	ReceivedAt *time.Time `json:"received_at"`
	Lat        *float64   `json:"lat"`
	Lon        *float64   `json:"lon"`
	Speed      *float64   `json:"speed"`
	Bearing    *float64   `json:"bearing"`
	Ignition   *bool      `json:"ignition"`
	Kecamatan  string     `json:"kecamatan"`
	Kelurahan  string     `json:"kelurahan"`
	Kabupaten  string     `json:"kabupaten"`
	Kota       string     `json:"kota"`
	Provinsi   string     `json:"provinsi"`
	Jalan      string     `json:"jalan"`
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

// HistoryPage is one page of a device's track. NextCursor is the RFC3339 time to
// pass back as ?cursor= for the next page; "" means no more data.
type HistoryPage struct {
	Points     []Position `json:"points"`
	NextCursor string     `json:"next_cursor"`
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

// ---- HTTP handlers ----

// handleLiveAll godoc
//
//	@Summary	Latest position of every device
//	@Tags		read
//	@Produce	json
//	@Success	200	{array}	Position
//	@Router		/v1/live [get]
func (h *Handler) handleLiveAll(w http.ResponseWriter, r *http.Request) {
	ps, err := h.r.LiveAll(r.Context())
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

	page, err := h.r.History(r.Context(), imei, from, to, cursor, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	writeJSON(w, http.StatusOK, page)
}
