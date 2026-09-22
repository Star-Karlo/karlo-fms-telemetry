package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Mobile telemetry: positions reported by the K-Trip phone app, keyed by the
// DRIVER (a Mongo ObjectId) because a phone has no IMEI and no ignition.
//
// Deliberately its own tables (mobile_telemetry / mobile_latest, migration 0022)
// rather than driver ids smuggled into the imei column: the GPS identity space,
// the coverage signal map and the vehicle-day rollups all key on imei and would
// be skewed by phones. Everything else — canonical payload shape, per-reading
// reverse geocoding, hot→cold Parquet, dual-read history — mirrors the GPS path.

// MobileReading is the JSON body accepted by POST /v1/ingest/mobile — one object
// or an array of them. Same field names as GenericReading, with `driver`
// replacing `imei`; phone-specific values ride in `io` (a few common ones are
// also accepted top-level for convenience). received_at is stamped by the
// server, never sent.
type MobileReading struct {
	Driver       string     `json:"driver" example:"649019ee6c7c280f4e46d0a2"` // driver id (required; identity)
	Latitude     float64    `json:"latitude" example:"-6.9786741"`             // -> geom
	Longitude    float64    `json:"longitude" example:"110.3701788"`           // -> geom
	Bearing      *float64   `json:"bearing" example:"0"`                       // -> bearing column
	Speed        *float64   `json:"speed" example:"0"`                         // -> speed column
	GpsCreatedAt *time.Time `json:"gpsCreatedAt" example:"2026-07-21T09:00:00Z"`
	Altitude     *float64   `json:"altitude" example:"65"` // -> metadata

	// Phone-specific conveniences: accepted top-level OR inside io; both land in metadata.
	BatteryLevel *float64 `json:"batteryLevel" example:"41"`
	Status       string   `json:"status" example:"Idle"`
	GpsOn        *bool    `json:"gpsOn"`
	FakeGps      *bool    `json:"fakeGps"`
	IsOverride   *bool    `json:"isOverride"`

	IO map[string]any `json:"io" swaggertype:"object,string"` // full app blob -> metadata
}

// mobileRow is one normalized reading plus its reverse-geocoded location.
type mobileRow struct {
	Driver     string
	GPSTime    time.Time
	ReceivedAt time.Time
	Lat, Lon   float64
	Bearing    float64
	Speed      float64
	Metadata   map[string]any

	Kecamatan, Kelurahan, Kabupaten, Kota, Provinsi, Jalan string
}

// parseMobile accepts a single reading object or a JSON array of them.
func parseMobile(raw []byte) ([]mobileRow, error) {
	var elems []json.RawMessage
	if t := bytes.TrimSpace(raw); len(t) > 0 && t[0] == '[' {
		if err := json.Unmarshal(raw, &elems); err != nil {
			return nil, fmt.Errorf("parse array: %w", err)
		}
	} else {
		elems = []json.RawMessage{json.RawMessage(raw)}
	}

	out := make([]mobileRow, 0, len(elems))
	for i, el := range elems {
		var d MobileReading
		if err := json.Unmarshal(el, &d); err != nil {
			return nil, fmt.Errorf("reading %d: %w", i, err)
		}
		if d.Driver == "" {
			return nil, fmt.Errorf("reading %d: missing driver", i)
		}
		if !validIMEI(d.Driver) { // generic id validator (alphanumeric/-/_)
			return nil, fmt.Errorf("reading %d: invalid driver id", i)
		}

		now := time.Now().UTC()
		gps := now
		if d.GpsCreatedAt != nil {
			gps = d.GpsCreatedAt.UTC()
		}

		meta := map[string]any{}
		for k, v := range d.IO { // full app blob first
			meta[k] = v
		}
		if d.Altitude != nil {
			meta["altitude"] = *d.Altitude
		}
		if d.BatteryLevel != nil {
			meta["batteryLevel"] = *d.BatteryLevel
		}
		if d.Status != "" {
			meta["status"] = d.Status
		}
		if d.GpsOn != nil {
			meta["gpsOn"] = *d.GpsOn
		}
		if d.FakeGps != nil {
			meta["fakeGps"] = *d.FakeGps
		}
		if d.IsOverride != nil {
			meta["isOverride"] = *d.IsOverride
		}
		meta["source"] = "mobile"
		meta["_raw"] = el

		out = append(out, mobileRow{
			Driver:     d.Driver,
			GPSTime:    gps,
			ReceivedAt: now, // server-stamped, same convention as GPS ingest
			Lat:        d.Latitude,
			Lon:        d.Longitude,
			Bearing:    deref(d.Bearing),
			Speed:      deref(d.Speed),
			Metadata:   meta,
		})
	}
	return out, nil
}

// ---- storage ---------------------------------------------------------------

// MobileStore persists and serves driver-keyed telemetry.
type MobileStore struct {
	pool          *pgxpool.Pool
	cold          *ColdReader // nil => hot-only history
	retentionDays int
}

func NewMobileStore(pool *pgxpool.Pool, cold *ColdReader, retentionDays int) *MobileStore {
	return &MobileStore{pool: pool, cold: cold, retentionDays: retentionDays}
}

const mobileInsertSQL = `
INSERT INTO mobile_telemetry
  (time, received_at, driver, geom, bearing, speed,
   kecamatan, kelurahan, kabupaten, kota, provinsi, jalan, metadata)
VALUES
  ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
   $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)`

// Advance current state only when the incoming fix is newer, so buffered/late
// pings cannot overwrite a fresher position (mirrors the GPS upsert).
const mobileUpsertSQL = `
INSERT INTO mobile_latest
  (driver, time, received_at, geom, bearing, speed,
   kecamatan, kelurahan, kabupaten, kota, provinsi, jalan, metadata)
VALUES
  ($3, $1, $2, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
   $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
ON CONFLICT (driver) DO UPDATE SET
  time = excluded.time, received_at = excluded.received_at, geom = excluded.geom,
  bearing = excluded.bearing, speed = excluded.speed,
  kecamatan = excluded.kecamatan, kelurahan = excluded.kelurahan,
  kabupaten = excluded.kabupaten, kota = excluded.kota,
  provinsi = excluded.provinsi, jalan = excluded.jalan, metadata = excluded.metadata
WHERE excluded.time > mobile_latest.time`

func (m *MobileStore) WriteBatch(ctx context.Context, rows []mobileRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		meta, err := json.Marshal(r.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		args := []any{
			r.GPSTime, r.ReceivedAt, r.Driver,
			r.Lon, r.Lat, r.Bearing, r.Speed,
			nullStr(r.Kecamatan), nullStr(r.Kelurahan), nullStr(r.Kabupaten),
			nullStr(r.Kota), nullStr(r.Provinsi), nullStr(r.Jalan),
			string(meta),
		}
		batch.Queue(mobileInsertSQL, args...)
		batch.Queue(mobileUpsertSQL, args...)
	}

	br := m.pool.SendBatch(ctx, batch)
	var firstErr error
	for i := 0; i < len(rows)*2; i++ {
		if _, err := br.Exec(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// ---- reads -----------------------------------------------------------------

// MobilePosition is a driver reading returned by the mobile read endpoints.
type MobilePosition struct {
	Driver     string          `json:"driver"`
	Time       time.Time       `json:"time"`
	ReceivedAt *time.Time      `json:"received_at"`
	Lat        *float64        `json:"lat"`
	Lon        *float64        `json:"lon"`
	Speed      *float64        `json:"speed"`
	Bearing    *float64        `json:"bearing"`
	Kecamatan  string          `json:"kecamatan"`
	Kelurahan  string          `json:"kelurahan"`
	Kabupaten  string          `json:"kabupaten"`
	Kota       string          `json:"kota"`
	Provinsi   string          `json:"provinsi"`
	Jalan      string          `json:"jalan"`
	Metadata   json.RawMessage `json:"metadata" swaggertype:"object"`
}

const mobilePosCols = `driver, time, received_at, ST_Y(geom::geometry), ST_X(geom::geometry),
                 speed, bearing,
                 coalesce(kecamatan,''), coalesce(kelurahan,''), coalesce(kabupaten,''),
                 coalesce(kota,''), coalesce(provinsi,''), coalesce(jalan,''),
                 coalesce(metadata, '{}'::jsonb) - '_raw'`

func scanMobile(rows pgx.Rows) ([]MobilePosition, error) {
	out := make([]MobilePosition, 0)
	for rows.Next() {
		var p MobilePosition
		if err := rows.Scan(&p.Driver, &p.Time, &p.ReceivedAt, &p.Lat, &p.Lon,
			&p.Speed, &p.Bearing,
			&p.Kecamatan, &p.Kelurahan, &p.Kabupaten, &p.Kota, &p.Provinsi, &p.Jalan,
			&p.Metadata); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (m *MobileStore) LiveAll(ctx context.Context) ([]MobilePosition, error) {
	rows, err := m.pool.Query(ctx, `SELECT `+mobilePosCols+` FROM mobile_latest ORDER BY driver`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMobile(rows)
}

// LiveMany returns the latest row for each of the given drivers that has one.
func (m *MobileStore) LiveMany(ctx context.Context, drivers []string) ([]MobilePosition, error) {
	rows, err := m.pool.Query(ctx, `SELECT `+mobilePosCols+` FROM mobile_latest WHERE driver = ANY($1) ORDER BY driver`, drivers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMobile(rows)
}

func (m *MobileStore) LiveOne(ctx context.Context, driver string) (*MobilePosition, error) {
	rows, err := m.pool.Query(ctx, `SELECT `+mobilePosCols+` FROM mobile_latest WHERE driver = $1`, driver)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ps, err := scanMobile(rows)
	if err != nil || len(ps) == 0 {
		return nil, err
	}
	return &ps[0], nil
}

// LivePositions returns the latest lat/lon (+fix time) for the given drivers —
// only those with a live row. Lean (no metadata/geocode), the driver-keyed twin
// of Reader.LivePositions, so geofence can resolve either identity the same way.
func (m *MobileStore) LivePositions(ctx context.Context, drivers []string) (map[string]GeoPos, error) {
	rows, err := m.pool.Query(ctx,
		`SELECT driver, ST_Y(geom::geometry), ST_X(geom::geometry), time
		 FROM mobile_latest WHERE driver = ANY($1) AND geom IS NOT NULL`, drivers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]GeoPos, len(drivers))
	for rows.Next() {
		var driver string
		var p GeoPos
		if err := rows.Scan(&driver, &p.Lat, &p.Lon, &p.Time); err != nil {
			return nil, err
		}
		out[driver] = p
	}
	return out, rows.Err()
}

// MobileHistoryPage is one page of a driver's track.
type MobileHistoryPage struct {
	Points     []MobilePosition `json:"points"`
	NextCursor string           `json:"next_cursor"`
}

// hotHistory queries mobile_telemetry (the hot window) for one driver.
func (m *MobileStore) hotHistory(ctx context.Context, driver string, lower, upper time.Time, lowerStrict bool, limit int) ([]MobilePosition, error) {
	loOp := ">="
	if lowerStrict {
		loOp = ">"
	}
	q := `SELECT ` + mobilePosCols + ` FROM mobile_telemetry
	      WHERE driver = $1 AND time ` + loOp + ` $2 AND time <= $3 ORDER BY time LIMIT $4`
	rows, err := m.pool.Query(ctx, q, driver, lower, upper, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMobile(rows)
}

// hotLTTB fetches the hot segment and thins it to ~n points in Go — the same
// path as the cold segment. (Was TimescaleDB Toolkit lttb() in SQL; the
// database is plain PostgreSQL since 2026-09-16.)
func (m *MobileStore) hotLTTB(ctx context.Context, driver string, lower, upper time.Time, lowerStrict bool, n int) ([]MobilePosition, error) {
	if n < 2 {
		n = 2
	}
	ps, err := m.hotHistory(ctx, driver, lower, upper, lowerStrict, lttbFetchCap)
	if err != nil {
		return nil, err
	}
	return thinMobile(ps, n), nil
}

// History returns a page of a driver's track in (cursor|from .. to], ascending,
// transparently merging COLD storage (Athena over the mobile/ Parquet folder,
// older than the retention window) with HOT (mobile_telemetry) — the same
// dual-read the GPS history endpoint performs. maxPoints > 0 downsamples the
// whole range instead of paginating.
func (m *MobileStore) History(ctx context.Context, driver string, from, to, cursor time.Time, limit, maxPoints int) (MobileHistoryPage, error) {
	lower, strict := from, false
	if !cursor.IsZero() {
		lower, strict = cursor, true
	}
	boundary := time.Now().UTC().AddDate(0, 0, -m.retentionDays)

	if maxPoints > 0 {
		var pts []MobilePosition
		// COLD [lower, min(to, boundary)) — Athena has no lttb(); cap then thin.
		if m.cold != nil && lower.Before(boundary) {
			upper, upperExcl := to, false
			if !to.Before(boundary) {
				upper, upperExcl = boundary, true
			}
			cps, err := m.cold.HistoryMobile(ctx, driver, lower, upper, strict, upperExcl, maxHistoryLimit)
			if err != nil {
				return MobileHistoryPage{}, err
			}
			pts = append(pts, thinMobile(cps, maxPoints)...)
		}
		// HOT [max(lower, boundary), to] — downsampled in-database.
		if !to.Before(boundary) {
			hl, hs := lower, strict
			if hl.Before(boundary) {
				hl, hs = boundary, false
			}
			hps, err := m.hotLTTB(ctx, driver, hl, to, hs, maxPoints)
			if err != nil {
				return MobileHistoryPage{}, err
			}
			pts = append(pts, hps...)
		}
		return MobileHistoryPage{Points: thinMobile(pts, maxPoints)}, nil
	}

	need := limit + 1
	var pts []MobilePosition
	if m.cold != nil && lower.Before(boundary) {
		upper, upperExcl := to, false
		if !to.Before(boundary) {
			upper, upperExcl = boundary, true
		}
		cps, err := m.cold.HistoryMobile(ctx, driver, lower, upper, strict, upperExcl, need)
		if err != nil {
			return MobileHistoryPage{}, err
		}
		pts = append(pts, cps...)
	}
	if len(pts) < need && !to.Before(boundary) {
		hl, hs := lower, strict
		if hl.Before(boundary) {
			hl, hs = boundary, false
		}
		hps, err := m.hotHistory(ctx, driver, hl, to, hs, need-len(pts))
		if err != nil {
			return MobileHistoryPage{}, err
		}
		pts = append(pts, hps...)
	}
	if pts == nil {
		pts = []MobilePosition{}
	}

	page := MobileHistoryPage{Points: pts}
	if len(pts) > limit {
		page.Points = pts[:limit]
		page.NextCursor = pts[limit-1].Time.UTC().Format(time.RFC3339Nano)
	}
	return page, nil
}

// thinMobile reduces a sorted track to at most n points by even stride. Used for
// the cold/merged segments only — the hot segment gets true LTTB in-database.
func thinMobile(pts []MobilePosition, n int) []MobilePosition {
	if n < 2 || len(pts) <= n {
		return pts
	}
	out := make([]MobilePosition, 0, n)
	step := float64(len(pts)-1) / float64(n-1)
	for i := 0; i < n; i++ {
		out = append(out, pts[int(float64(i)*step)])
	}
	return out
}

// ---- HTTP handlers ---------------------------------------------------------

// handleMobileIngest godoc
//
//	@Summary		Ingest mobile-app telemetry (single reading or batch array)
//	@Description	Same canonical shape as /v1/ingest but keyed by `driver` (the master-data driver id) instead of `imei` (a phone has no IMEI). Each reading is reverse-geocoded to kecamatan/kelurahan/kota/provinsi/jalan and stored to the mobile_telemetry hypertable. received_at is stamped by the server. Phone extras (batteryLevel, accuracy, status, isOverride, …) go in `io` and are kept in metadata. Accepts an ingest key or a peer service's platform token, so a driver app can post through its own backend.
//	@Tags			ingest
//	@Accept			json
//	@Produce		json
//	@Param			payload			body	[]MobileReading	true	"Array of readings (a single reading object is also accepted)"
//	@Success		202	{object}	IngestResult
//	@Failure		400	{object}	map[string]string
//	@Failure		401	{object}	map[string]string
//	@Security	IngestKey
//	@Security	ServiceToken
//	@Router			/v1/ingest/mobile [post]
func (h *Handler) handleMobileIngest(w http.ResponseWriter, r *http.Request) {
	// An ingest key, or a peer service's platform token: the driver app
	// posts through business-service (its own login), which forwards with
	// the service token — so no ingest key is ever built into a phone.
	if !h.AuthorizedRead(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing X-Ingest-Key or service token"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body"})
		return
	}
	rows, err := parseMobile(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	ctx := r.Context()
	for i := range rows {
		// Same best-effort enrichment as the GPS path (shared geohash cache).
		if res, gerr := h.geo.Reverse(ctx, rows[i].Lat, rows[i].Lon); gerr == nil && res.Found {
			rows[i].Kecamatan = res.Kecamatan
			rows[i].Kelurahan = res.Kelurahan
			rows[i].Kabupaten = res.Kabupaten
			rows[i].Kota = res.Kota
			rows[i].Provinsi = res.Provinsi
			rows[i].Jalan = res.Jalan
		}
	}
	if err := h.mob.WriteBatch(ctx, rows); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write failed"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"stored": len(rows)})
}

// handleMobileLiveAll godoc
//
//	@Summary	Latest position of mobile drivers (all, or a specific set)
//	@Description	Returns the latest phone fix per driver. Use ?drivers= to restrict to a set — fms-backend asks only for the drivers assigned to vehicles that have no tracker or camera fix.
//	@Tags		read
//	@Produce	json
//	@Param		drivers	query	string	false	"Comma-separated driver ids to return (omit for all). Max 5000."
//	@Success	200	{array}	MobilePosition
//	@Failure	400	{object}	map[string]string
//	@Security	IngestKey
//	@Security	ServiceToken
//	@Router		/v1/mobile/live [get]
func (h *Handler) handleMobileLiveAll(w http.ResponseWriter, r *http.Request) {
	var drivers []string
	if raw := r.URL.Query().Get("drivers"); raw != "" {
		for _, s := range strings.Split(raw, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if !validIMEI(s) {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid driver: " + s})
				return
			}
			drivers = append(drivers, s)
		}
		if len(drivers) > maxLiveIMEIs {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "too many drivers (max 5000)"})
			return
		}
	}
	var ps []MobilePosition
	var err error
	if drivers != nil {
		ps, err = h.mob.LiveMany(r.Context(), drivers)
	} else {
		ps, err = h.mob.LiveAll(r.Context())
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	writeJSON(w, http.StatusOK, ps)
}

// handleMobileLiveOne godoc
//
//	@Summary	Latest position of one mobile driver
//	@Tags		read
//	@Produce	json
//	@Param		driver	path		string	true	"Driver id"
//	@Success	200		{object}	MobilePosition
//	@Failure	404		{object}	map[string]string
//	@Security	IngestKey
//	@Security	ServiceToken
//	@Router		/v1/mobile/live/{driver} [get]
func (h *Handler) handleMobileLiveOne(w http.ResponseWriter, r *http.Request) {
	driver := chi.URLParam(r, "driver")
	if !validIMEI(driver) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid driver"})
		return
	}
	p, err := h.mob.LiveOne(r.Context(), driver)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	if p == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no data for driver"})
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// handleMobileHistory godoc
//
//	@Summary	Historical track for a mobile driver over a time range
//	@Tags		read
//	@Produce	json
//	@Param		driver		path		string	true	"Driver id"
//	@Param		from		query		string	false	"Start time (RFC3339); default now-24h"
//	@Param		to			query		string	false	"End time (RFC3339); default now"
//	@Param		limit		query		int		false	"Page size (default 1000, max 10000)"
//	@Param		cursor		query		string	false	"Pass the previous response's next_cursor"
//	@Param		max_points	query		int		false	"Downsample the range to ~N shape-preserving points (LTTB); bypasses pagination"
//	@Success	200			{object}	MobileHistoryPage
//	@Failure	400			{object}	map[string]string
//	@Security	IngestKey
//	@Security	ServiceToken
//	@Router		/v1/mobile/history/{driver} [get]
func (h *Handler) handleMobileHistory(w http.ResponseWriter, r *http.Request) {
	driver := chi.URLParam(r, "driver")
	if !validIMEI(driver) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid driver"})
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

	page, err := h.mob.History(r.Context(), driver, from, to, cursor, limit, maxPoints)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	writeJSON(w, http.StatusOK, page)
}
