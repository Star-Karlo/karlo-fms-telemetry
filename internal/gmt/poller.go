package gmt

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/karlo/fms/internal/geocode"
	"github.com/karlo/fms/internal/ingest"
)

// DefaultInterval: pull + store every 90s (per the rajabesi integration spec).
const DefaultInterval = 90 * time.Second

// Poller pulls the GMT fleet status on a fixed interval and writes each vehicle
// as a telemetry reading — the same rows a device would POST to /ingest, so the
// rest of FMS (live map, history, fuel, scoring) treats GMT devices identically.
type Poller struct {
	client   *Client
	geo      *geocode.Service
	w        *ingest.Writer
	interval time.Duration
}

func NewPoller(client *Client, geo *geocode.Service, w *ingest.Writer) *Poller {
	return &Poller{client: client, geo: geo, w: w, interval: DefaultInterval}
}

// Run blocks, polling until ctx is cancelled. A no-op (logged) when no API key
// is configured, so leaving GMT unset simply disables it.
func (p *Poller) Run(ctx context.Context) {
	if !p.client.Enabled() {
		log.Printf("gmt: poller disabled (no GMT_API_KEY)")
		return
	}
	log.Printf("gmt: poller started, every %s", p.interval)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	p.tick(ctx) // first pull immediately, not after one interval
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *Poller) tick(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	statuses, err := p.client.VehicleStatuses(cctx)
	if err != nil {
		log.Printf("gmt: poll: %v", err)
		return
	}
	now := time.Now().UTC()
	readings := make([]ingest.Reading, 0, len(statuses))
	for _, s := range statuses {
		imei := normalizeIMEI(s.IMEI)
		if imei == "" {
			continue
		}
		// Skip a no-fix row (0,0) rather than parking the truck off West Africa.
		if s.Latitude == 0 && s.Longitude == 0 {
			continue
		}
		ign := s.EngineOn
		meta := map[string]any{
			"source":         "gmt",
			"altitude":       s.Altitude,
			"motionStatus":   s.MotionStatus,
			"battery":        s.Battery,
			"signalStrength": s.SignalStrength,
			"door1":          s.Door1Status,
			"door2":          s.Door2Status,
			"licensePlate":   s.LicensePlate,
			"hullNo":         s.HullNo,
		}
		if s.FuelCapacity > 0 {
			meta["fuelCapacity"] = s.FuelCapacity
			meta["fuelFiltered"] = s.FuelFiltered
			// A percentage FMS's fuel analysis can read directly.
			meta["fuel_percentage"] = s.FuelFiltered / s.FuelCapacity * 100
		}
		if len(s.Fuel) > 0 {
			meta["fuel"] = s.Fuel
		}
		if len(s.Temperature) > 0 {
			meta["temperature"] = s.Temperature
		}
		readings = append(readings, ingest.Reading{
			IMEI:       imei,
			GPSTime:    parseTime(s.LastPacket, now),
			ReceivedAt: now,
			Lat:        s.Latitude,
			Lon:        s.Longitude,
			Bearing:    s.Direction,
			Speed:      s.Speed,
			Ignition:   &ign,
			Metadata:   meta,
		})
	}
	if len(readings) == 0 {
		return
	}
	n, err := ingest.WriteReadings(cctx, p.geo, p.w, readings)
	if err != nil {
		log.Printf("gmt: write %d readings: %v", len(readings), err)
		return
	}
	log.Printf("gmt: stored %d/%d vehicle statuses", n, len(statuses))
}

// normalizeIMEI takes GMT's device id (e.g. "353742378917173_1") down to the
// canonical 15-digit IMEI our trackers are keyed by. Non-digits and any "_N"
// suffix are dropped; anything shorter than 15 digits is rejected.
func normalizeIMEI(raw string) string {
	if i := strings.IndexByte(raw, '_'); i >= 0 {
		raw = raw[:i]
	}
	var b strings.Builder
	for _, r := range raw {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	d := b.String()
	if len(d) < 15 {
		return ""
	}
	return d[:15]
}

// parseTime accepts the timestamp formats GMT is seen to emit, falling back to
// `def` (the receive time) when it can't be parsed — the writer/latest upsert
// only advances on a newer fix, so a bad time can't freeze the live position.
func parseTime(s string, def time.Time) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return def
}
