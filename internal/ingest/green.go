package ingest

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Green Driving ingest: pull Teltonika harsh-driving events out of the IO bag on
// each reading and store them raw, for FMS to poll and score. This is the
// tracker-sourced twin of the camera's dashcam_alarms — it means a truck with
// only a GPS tracker still feeds the harsh braking/acceleration/cornering
// pillars, no camera required.
//
// Teltonika Green Driving is an eventual IO set: AVL 253 (type) appears only
// when an event fires, 254 carries its magnitude, 243 its duration. The io keys
// as our upstream normaliser passes them are read by a candidate scan (numeric
// AVL id or a named alias) so a naming change upstream does not silently drop
// events; the exact key is confirmed against the first real reading.

// Candidate io keys, in priority order, for each Green Driving field. The
// human-readable names are what our upstream actually sends (confirmed from a
// real reading); the numeric AVL ids are kept as a fallback.
var (
	greenTypeKeys     = []string{"Green driving type", "253", "green_driving_type", "greenDrivingType"}
	greenValueKeys    = []string{"Green Driving Value", "254", "green_driving_value", "greenDrivingValue"}
	greenDurationKeys = []string{"Green driving event duration", "243", "green_driving_duration"}
)

// GreenEvent is one stored Green Driving event as the poll endpoint returns it.
type GreenEvent struct {
	ID        int64  `json:"id"`
	IMEI      string `json:"imei"`
	EventType int    `json:"event_type"` // 1 accel, 2 brake, 3 corner
	// Value is the raw Teltonika Green Driving Value (device units). FMS converts
	// it to g for severity — it is NOT a g here.
	Value      *float64 `json:"value"`
	DurationMs *int     `json:"duration_ms"`
	OccurredAt string   `json:"occurred_at"`
	Lat        *float64 `json:"lat"`
	Lon        *float64 `json:"lon"`
	Speed      *float64 `json:"speed"`
}

// GreenStore persists and serves tracker Green Driving events.
type GreenStore struct{ pool *pgxpool.Pool }

func NewGreenStore(pool *pgxpool.Pool) *GreenStore { return &GreenStore{pool: pool} }

// firstNum returns the first candidate key that holds a number.
func firstNum(m map[string]any, keys []string) (float64, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if f, ok := ioFloat(v); ok {
				return f, true
			}
		}
	}
	return 0, false
}

// ioFloat coerces a JSON io value (number or numeric string) to float64.
func ioFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f, true
		}
	case string:
		if f, err := strconv.ParseFloat(t, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// Extract scans a batch of readings and stores any Green Driving events. Called
// after readings are parsed; best-effort, so a store failure never fails the
// ingest of the underlying telemetry.
func (g *GreenStore) Extract(ctx context.Context, readings []Reading) {
	for _, rd := range readings {
		t, ok := firstNum(rd.Metadata, greenTypeKeys)
		if !ok {
			continue // no Green Driving type on this record — not an event
		}
		typ := int(t)
		if typ < 1 || typ > 3 {
			continue // only 1/2/3 are the harsh accel/brake/corner types
		}
		ev := GreenEvent{IMEI: rd.IMEI, EventType: typ, OccurredAt: rd.GPSTime.Format(time.RFC3339)}
		if v, ok := firstNum(rd.Metadata, greenValueKeys); ok && v > 0 {
			ev.Value = &v
		}
		if d, ok := firstNum(rd.Metadata, greenDurationKeys); ok {
			dm := int(d)
			ev.DurationMs = &dm
		}
		g.store(ctx, rd, ev)
	}
}

func (g *GreenStore) store(ctx context.Context, rd Reading, ev GreenEvent) {
	raw, _ := json.Marshal(rd.Metadata)
	var lat, lon, speed *float64
	if rd.Lat != 0 || rd.Lon != 0 {
		lat, lon = &rd.Lat, &rd.Lon
	}
	if rd.Speed != 0 {
		speed = &rd.Speed
	}
	_, err := g.pool.Exec(ctx, `
INSERT INTO green_driving_events
  (imei, event_type, value, duration_ms, occurred_at, lat, lon, speed, raw)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (imei, occurred_at, event_type) DO NOTHING`,
		ev.IMEI, ev.EventType, ev.Value, ev.DurationMs, rd.GPSTime, lat, lon, speed, json.RawMessage(raw))
	if err != nil {
		log.Printf("green driving: store %s type %d: %v", ev.IMEI, ev.EventType, err)
	}
}

// handleEvents godoc
//
//	@Summary		Tracker Green Driving events since an id (for FMS to poll)
//	@Description	Harsh accel/brake/corner events from Teltonika trackers, id-ascending. Poll with since=<last id>. Tenant-agnostic; FMS maps IMEI to a vehicle and scores it.
//	@Tags			driving
//	@Produce		json
//	@Param			X-Ingest-Key	header	string	true	"ingest API key"
//	@Param			since			query	int		false	"return events with id greater than this (default 0)"
//	@Param			limit			query	int		false	"max rows (default 200)"
//	@Success		200	{array}	ingest.GreenEvent
//	@Router			/v1/driving-events [get]
func (g *GreenStore) handleEvents(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	limit := 200
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 1000 {
		limit = v
	}
	rows, err := g.pool.Query(r.Context(), `
SELECT id, imei, event_type, value, duration_ms,
       to_char(occurred_at, 'YYYY-MM-DD"T"HH24:MI:SSOF:00'), lat, lon, speed
FROM green_driving_events WHERE id > $1 ORDER BY id LIMIT $2`, since, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query green events failed"})
		return
	}
	defer rows.Close()
	out := []GreenEvent{}
	for rows.Next() {
		var e GreenEvent
		if err := rows.Scan(&e.ID, &e.IMEI, &e.EventType, &e.Value, &e.DurationMs,
			&e.OccurredAt, &e.Lat, &e.Lon, &e.Speed); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "scan green events failed"})
			return
		}
		out = append(out, e)
	}
	writeJSON(w, http.StatusOK, out)
}
