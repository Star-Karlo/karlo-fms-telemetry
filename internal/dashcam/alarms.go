package dashcam

import (
	"encoding/json"
	"net/http"
	"strconv"
)

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
