package ingest

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
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

// GreenStore persists and serves tracker Green Driving events. It also derives
// events from the raw accelerometer (Axis X/Y/Z) for devices that report axes
// but not native Green Driving events — keeping a small in-memory per-device
// resting baseline (the gravity vector) and a per-event debounce.
type GreenStore struct {
	pool *pgxpool.Pool

	mu       sync.Mutex
	baseline map[string]axisVec   // imei -> resting gravity vector (mg)
	lastEvt  map[string]time.Time // "imei:type" -> last emit (debounce)
}

func NewGreenStore(pool *pgxpool.Pool) *GreenStore {
	return &GreenStore{pool: pool, baseline: map[string]axisVec{}, lastEvt: map[string]time.Time{}}
}

// axisVec is an accelerometer sample / baseline in mg.
type axisVec struct{ x, y, z float64 }

// Raw-axis harsh-driving keys (mg). Named as our upstream sends them, with the
// Teltonika AVL ids as a fallback (17/18/19 = Axis X/Y/Z).
var (
	axisXKeys = []string{"Axis X", "17", "axis_x"}
	axisYKeys = []string{"Axis Y", "18", "axis_y"}
	axisZKeys = []string{"Axis Z", "19", "axis_z"}
)

// Harsh thresholds, in g of deviation from the device's resting baseline.
// v1 uses simple per-axis thresholds; the dominant axis wins so one manoeuvre
// yields one event. X -> accel(+)/brake(-), Y -> cornering, Z -> bump/road-shock.
const (
	harshAccelG  = 0.35
	harshCornerG = 0.40
	harshBumpG   = 0.60
	restSpeedKmh = 3.0             // below this (or Movement=0) counts as resting
	axisDebounce = 10 * time.Second
	baseAlpha    = 0.1 // EMA weight when updating the resting baseline
)

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
		if t, ok := firstNum(rd.Metadata, greenTypeKeys); ok {
			typ := int(t)
			if typ >= 1 && typ <= 3 { // native Green Driving event
				ev := GreenEvent{IMEI: rd.IMEI, EventType: typ, OccurredAt: rd.GPSTime.Format(time.RFC3339)}
				if v, ok := firstNum(rd.Metadata, greenValueKeys); ok && v > 0 {
					ev.Value = &v
				}
				if d, ok := firstNum(rd.Metadata, greenDurationKeys); ok {
					dm := int(d)
					ev.DurationMs = &dm
				}
				g.store(ctx, rd, ev)
				continue // native event present — don't also derive from the axes
			}
		}
		// No native Green Driving event: derive one from the raw accelerometer.
		g.detectAxis(ctx, rd)
	}
}

// detectAxis derives a harsh-driving event from the raw accelerometer (Axis
// X/Y/Z, mg) when the device reports axes but no native Green Driving event.
// A tilted mount offsets the resting gravity vector, so we subtract a per-device
// baseline (learned while stationary) and threshold the deviation in g. The
// dominant axis wins; the event is stored in the same green_driving_events table
// with value = g×100, so FMS's poller scores it identically to a native event.
func (g *GreenStore) detectAxis(ctx context.Context, rd Reading) {
	x, okx := firstNum(rd.Metadata, axisXKeys)
	y, oky := firstNum(rd.Metadata, axisYKeys)
	z, okz := firstNum(rd.Metadata, axisZKeys)
	if !okx || !oky || !okz {
		return // no accelerometer on this reading
	}
	cur := axisVec{x, y, z}

	// Resting = not moving; used to (re)learn the gravity baseline.
	resting := rd.Speed < restSpeedKmh
	if mv, ok := firstNum(rd.Metadata, []string{"Movement"}); ok && mv == 0 {
		resting = true
	}

	g.mu.Lock()
	base, seen := g.baseline[rd.IMEI]
	if !seen {
		g.baseline[rd.IMEI] = cur // seed on first sight; no event yet
		g.mu.Unlock()
		return
	}
	if resting {
		g.baseline[rd.IMEI] = axisVec{ // EMA toward the current resting vector
			base.x + baseAlpha*(cur.x-base.x),
			base.y + baseAlpha*(cur.y-base.y),
			base.z + baseAlpha*(cur.z-base.z),
		}
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()

	// Deviation from rest, in g.
	dx := (cur.x - base.x) / 1000.0
	dy := (cur.y - base.y) / 1000.0
	dz := (cur.z - base.z) / 1000.0

	// Pick the axis whose deviation is the largest multiple of its threshold.
	bestType, bestRatio, bestG := 0, 0.0, 0.0
	consider := func(typ int, dev, thr float64) {
		a := math.Abs(dev)
		if a >= thr && a/thr > bestRatio {
			bestType, bestRatio, bestG = typ, a/thr, a
		}
	}
	accelType := 1 // +X = acceleration, -X = braking (sign is mount-relative; v1)
	if dx < 0 {
		accelType = 2
	}
	consider(accelType, dx, harshAccelG)
	consider(3, dy, harshCornerG) // cornering (Y)
	consider(4, dz, harshBumpG)   // bump / road-shock (Z)
	if bestType == 0 {
		return // nothing crossed a threshold
	}

	// Per-device, per-type debounce so one manoeuvre isn't stored many times.
	key := rd.IMEI + ":" + strconv.Itoa(bestType)
	g.mu.Lock()
	if last, ok := g.lastEvt[key]; ok && rd.GPSTime.Sub(last) < axisDebounce {
		g.mu.Unlock()
		return
	}
	g.lastEvt[key] = rd.GPSTime
	g.mu.Unlock()

	val := bestG * 100.0 // g×100, matching FMS teltonikaValueToG (0.01)
	g.store(ctx, rd, GreenEvent{
		IMEI:       rd.IMEI,
		EventType:  bestType,
		Value:      &val,
		OccurredAt: rd.GPSTime.Format(time.RFC3339),
	})
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
