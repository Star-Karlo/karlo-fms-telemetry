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
	Kabupaten  string     `json:"kabupaten"`
	Kota       string     `json:"kota"`
	Provinsi   string     `json:"provinsi"`
	Metadata   json.RawMessage `json:"metadata"` // vendor io + extras (raw payload excluded)
}

// Reader serves live (current state) and historical telemetry queries.
type Reader struct{ pool *pgxpool.Pool }

func NewReader(pool *pgxpool.Pool) *Reader { return &Reader{pool: pool} }

const posCols = `imei, time, received_at, ST_Y(geom::geometry), ST_X(geom::geometry),
                 speed, bearing, ignition,
                 coalesce(kecamatan,''), coalesce(kabupaten,''), coalesce(kota,''), coalesce(provinsi,''),
                 coalesce(metadata, '{}'::jsonb) - '_raw'`

func scanPositions(rows pgx.Rows) ([]Position, error) {
	out := make([]Position, 0)
	for rows.Next() {
		var p Position
		if err := rows.Scan(&p.IMEI, &p.Time, &p.ReceivedAt, &p.Lat, &p.Lon,
			&p.Speed, &p.Bearing, &p.Ignition, &p.Kecamatan, &p.Kabupaten, &p.Kota, &p.Provinsi,
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

func (rd *Reader) History(ctx context.Context, imei string, from, to time.Time, limit int) ([]Position, error) {
	rows, err := rd.pool.Query(ctx,
		`SELECT `+posCols+` FROM telemetry
		 WHERE imei = $1 AND time >= $2 AND time < $3 ORDER BY time LIMIT $4`,
		imei, from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPositions(rows)
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
	p, err := h.r.LiveOne(r.Context(), chi.URLParam(r, "imei"))
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
//	@Param		limit	query		int		false	"Max points (default 1000, max 10000)"
//	@Success	200		{array}		Position
//	@Failure	400		{object}	map[string]string
//	@Router		/v1/history/{imei} [get]
func (h *Handler) handleHistory(w http.ResponseWriter, r *http.Request) {
	imei := chi.URLParam(r, "imei")
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
	limit := defaultHistoryLimit
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}

	ps, err := h.r.History(r.Context(), imei, from, to, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	writeJSON(w, http.StatusOK, ps)
}
