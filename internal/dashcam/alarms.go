package dashcam

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/karlo/fms/internal/vss"
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
	// Files are the event's video clips on the camera server, one per file and
	// channel, without their download links. Empty when the event has no video.
	Files []AlarmFile `json:"files"`
}

// AlarmFile is one video clip of an event.
type AlarmFile struct {
	Channel  int    `json:"channel"`
	FileType int    `json:"file_type"`
	Start    string `json:"start"`
	Stop     string `json:"stop"`
}

// alarms godoc
//
//	@Summary		Dashcam alarms since an id (for FMS to poll)
//	@Description	Events received over the VSS WebSocket, id-ascending. Poll with since=<last id you saw>. Tenant-agnostic; FMS maps device_id to a vehicle and raises its own alert. device_id, from and to narrow it to one camera's events in a time window.
//	@Tags			dashcams
//	@Produce		json
//	@Param			since			query	int		false	"return alarms with id greater than this (default 0)"
//	@Param			limit			query	int		false	"max rows (default 200)"
//	@Param			device_id		query	string	false	"only this camera's events"
//	@Param			from			query	string	false	"events at or after this time (RFC3339)"
//	@Param			to				query	string	false	"events before this time (RFC3339)"
//	@Success		200	{array}	dashcam.AlarmDTO
//	@Failure		400	{object}	map[string]string
//	@Security	IngestKey
//	@Security	ServiceToken
//	@Router			/v1/dashcams/alarms [get]
func (h *Handler) alarms(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	limit := 200
	if v, err := strconv.Atoi(q.Get("limit")); err == nil && v > 0 && v <= 1000 {
		limit = v
	}
	from, err1 := optionalTime(q.Get("from"))
	to, err2 := optionalTime(q.Get("to"))
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from and to must be RFC3339"})
		return
	}
	rows, err := h.pool.Query(r.Context(), `
SELECT id, device_id, coalesce(alarm_id,''), code,
       to_char(occurred_at, 'YYYY-MM-DD"T"HH24:MI:SSOF:00'), lat, lon, speed, gforce, raw
FROM dashcam_alarms
WHERE id > $1
  AND ($3 = '' OR device_id = $3)
  AND ($4::timestamptz IS NULL OR occurred_at >= $4)
  AND ($5::timestamptz IS NULL OR occurred_at < $5)
ORDER BY id LIMIT $2`, since, limit, q.Get("device_id"), from, to)
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
		a.Files = videoFiles(a.Raw)
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, out)
}

func optionalTime(v string) (*time.Time, error) {
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// videoFiles lists the video clips stored in an alarm's raw payload, leaving
// out pictures and the token-bearing download links.
func videoFiles(raw json.RawMessage) []AlarmFile {
	out := []AlarmFile{}
	var stored struct {
		Files []vss.EvidenceFile `json:"files"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &stored) != nil {
		return out
	}
	for _, f := range stored.Files {
		if isVideo(f) {
			out = append(out, AlarmFile{Channel: fileChannel(f), FileType: f.FileType, Start: f.StartTime, Stop: f.StopTime})
		}
	}
	return out
}
