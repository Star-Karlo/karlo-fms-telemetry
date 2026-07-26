package dashcam

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/karlo/fms/internal/vss"
)

// AlarmRunner holds the live VSS WebSocket subscription and persists every
// alarm it receives. One long-lived goroutine per service; FMS reads the table
// through the /v1/dashcams/alarms endpoint rather than holding its own socket.
type AlarmRunner struct {
	vss  *vss.Client
	pool *pgxpool.Pool
}

// NewAlarmRunner builds the runner.
func NewAlarmRunner(client *vss.Client, pool *pgxpool.Pool) *AlarmRunner {
	return &AlarmRunner{vss: client, pool: pool}
}

// Run maintains the subscription until ctx is cancelled, reconnecting with
// backoff. A no-op when VSS is not configured, so a deployment without cameras
// opens no socket. Reconnection is HERE, not in the client, so the backoff and
// the "why did the feed stop" logging live in one place.
func (r *AlarmRunner) Run(ctx context.Context) {
	if !r.vss.Enabled() {
		return
	}
	backoff := 2 * time.Second
	const maxBackoff = 2 * time.Minute
	for {
		if ctx.Err() != nil {
			return
		}
		err := r.vss.SubscribeAlarms(ctx, r.store)
		if ctx.Err() != nil {
			return
		}
		// The socket ended. Log why, wait, reconnect. Backoff caps so a server
		// outage does not spin, and resets on the next clean connection.
		log.Printf("dashcam alarms: subscription ended (%v); reconnecting in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// store writes one received alarm, ignoring a duplicate (same VSS event id).
func (r *AlarmRunner) store(a vss.Alarm) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw := json.RawMessage(a.Raw)
	_, err := r.pool.Exec(ctx, `
INSERT INTO dashcam_alarms (device_id, alarm_id, code, occurred_at, lat, lon, speed, gforce, raw)
VALUES ($1, NULLIF($2,''), $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (alarm_id) WHERE alarm_id IS NOT NULL DO NOTHING`,
		a.DeviceID, a.AlarmID, a.Code, a.OccurredAt, a.Lat, a.Lon, a.Speed, a.Gforce, raw)
	if err != nil {
		log.Printf("dashcam alarms: store %s/%s: %v", a.DeviceID, a.AlarmID, err)
	}
}

// AlarmDTO is one stored alarm as the poll endpoint returns it.
type AlarmDTO struct {
	ID         int64    `json:"id"`
	DeviceID   string   `json:"device_id"`
	AlarmID    string   `json:"alarm_id"`
	Code       int      `json:"code"`
	OccurredAt string   `json:"occurred_at"`
	Lat        *float64 `json:"lat"`
	Lon        *float64 `json:"lon"`
	Speed      *float64 `json:"speed"`
	// Gforce is the reported acceleration magnitude for a driving-behaviour
	// event, or null. Raw is the original VSS payload, exposed so FMS can
	// confirm and back-fill the exact G field once a real event is seen.
	Gforce *float64        `json:"gforce"`
	Raw    json.RawMessage `json:"raw,omitempty" swaggertype:"object"`
}

// alarms godoc
//
//	@Summary		Dashcam alarms since an id (for FMS to poll)
//	@Description	Events received over the VSS WebSocket, id-ascending. Poll with since=<last id you saw>. Tenant-agnostic; FMS maps device_id to a vehicle and raises its own alert.
//	@Tags			dashcams
//	@Produce		json
//	@Param			X-Ingest-Key	header	string	true	"ingest API key"
//	@Param			since			query	int		false	"return alarms with id greater than this (default 0)"
//	@Param			limit			query	int		false	"max rows (default 200)"
//	@Success		200	{array}	dashcam.AlarmDTO
//	@Router			/v1/dashcams/alarms [get]
func (h *Handler) alarms(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	limit := 200
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 1000 {
		limit = v
	}
	rows, err := h.pool.Query(r.Context(), `
SELECT id, device_id, coalesce(alarm_id,''), code,
       to_char(occurred_at, 'YYYY-MM-DD"T"HH24:MI:SSOF:00'), lat, lon, speed, gforce, raw
FROM dashcam_alarms WHERE id > $1 ORDER BY id LIMIT $2`, since, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query alarms failed"})
		return
	}
	defer rows.Close()
	out := []AlarmDTO{}
	for rows.Next() {
		var a AlarmDTO
		if err := rows.Scan(&a.ID, &a.DeviceID, &a.AlarmID, &a.Code, &a.OccurredAt, &a.Lat, &a.Lon, &a.Speed, &a.Gforce, &a.Raw); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "scan alarms failed"})
			return
		}
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, out)
}
