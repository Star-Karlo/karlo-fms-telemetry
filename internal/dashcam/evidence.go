package dashcam

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/karlo/fms/internal/vss"
)

// EvidenceRunner polls VSS for recent alarm evidence per device and persists
// each event into dashcam_alarms — the reliable, HTTP-polled alternative to the
// push WebSocket (which the vendor server rejects at the handshake here). FMS
// reads the same table via /v1/dashcams/alarms, so alerts, notifications and
// webhooks flow unchanged; the difference is each alarm now also carries its
// evidence clip + snapshot download URLs.
type EvidenceRunner struct {
	vss    *vss.Client
	pool   *pgxpool.Pool
	every  time.Duration
	window time.Duration
}

func NewEvidenceRunner(client *vss.Client, pool *pgxpool.Pool) *EvidenceRunner {
	return &EvidenceRunner{
		vss:    client,
		pool:   pool,
		every:  90 * time.Second,
		// Wide look-back: catches infrequent events and absorbs any timezone skew
		// between our query window and the server's clock. Dedup on alarmGuid
		// makes the repeated overlap idempotent.
		window: 24 * time.Hour,
	}
}

// wib is the fleet's timezone. VSS reports and expects device-local times, and
// the devices run WIB (UTC+7); a FixedZone avoids depending on tzdata being in
// the container image.
var wib = time.FixedZone("WIB", 7*3600)

func (r *EvidenceRunner) Run(ctx context.Context) {
	if !r.vss.Enabled() {
		return
	}
	// First sweep shortly after start, then on the ticker.
	r.tick(ctx)
	t := time.NewTicker(r.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

func (r *EvidenceRunner) tick(ctx context.Context) {
	devices, err := r.vss.ListDevices(ctx, "")
	if err != nil {
		log.Printf("evidence poller: list devices: %v", err)
		return
	}
	now := time.Now().In(wib)
	to := now.Add(2 * time.Hour) // small forward buffer for any clock skew
	from := now.Add(-r.window)
	for _, d := range devices {
		evs, err := r.vss.SearchAlarmEvidence(ctx, d.DeviceNo, from, to, "")
		if err != nil {
			log.Printf("evidence poller: %s: %v", d.DeviceNo, err)
			continue
		}
		for _, e := range evs {
			r.store(ctx, e)
		}
	}
}

// store upserts one alarm (dedup on alarmGuid). The evidence files — clip and
// snapshot download URLs — are kept in raw so FMS can surface and download them.
func (r *EvidenceRunner) store(ctx context.Context, e vss.AlarmEvidence) {
	if e.AlarmGuid == "" {
		return
	}
	occurred := time.Now()
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", e.AlarmTime, wib); err == nil {
		occurred = t
	}
	raw, _ := json.Marshal(map[string]any{"alarm_type": e.AlarmType, "files": e.Files})
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.pool.Exec(c, `
INSERT INTO dashcam_alarms (device_id, alarm_id, code, occurred_at, lat, lon, raw)
VALUES ($1, NULLIF($2,''), $3, $4, $5, $6, $7)
ON CONFLICT (alarm_id) WHERE alarm_id IS NOT NULL DO NOTHING`,
		e.DeviceID, e.AlarmGuid, e.AlarmType, occurred, e.Lat, e.Lon, json.RawMessage(raw))
	if err != nil {
		log.Printf("evidence poller: store %s/%s: %v", e.DeviceID, e.AlarmGuid, err)
	}
}
