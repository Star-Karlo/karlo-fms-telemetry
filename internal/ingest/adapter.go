package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/karlo/fms/internal/model"
)

// Adapter converts a vendor's raw payload into canonical TelemetryEvents.
// One adapter per vendor; everything downstream is vendor-agnostic.
type Adapter interface {
	Parse(raw []byte) ([]model.TelemetryEvent, error)
}

// Adapters maps a vendor/source key (the {vendor} path segment) to its Adapter.
var Adapters = map[string]Adapter{
	"generic": genericAdapter{},
}

// reading is the normalized tracking-document shape. Top-level fields are
// consistent across our vendors; the `io` object varies and is kept whole.
// Duplicated signals (e.g. Ignition/Speed also inside io) are resolved to the
// canonical top-level field here; io is preserved verbatim in attrs.
type reading struct {
	IMEI       string         `json:"imei"`
	Latitude   float64        `json:"latitude"`
	Longitude  float64        `json:"longitude"`
	Altitude   *float64       `json:"altitude"`
	Bearing    *float64       `json:"bearing"`
	Speed      *float64       `json:"speed"`
	IgnitionOn *bool          `json:"ignitionOn"`
	Sat        *int           `json:"sat"`
	CreatedAt  *time.Time     `json:"createdAt"` // RFC3339; nil -> now
	IO         map[string]any `json:"io"`
}

type genericAdapter struct{}

// Parse accepts either a single reading object or a JSON array of readings.
func (genericAdapter) Parse(raw []byte) ([]model.TelemetryEvent, error) {
	var elems []json.RawMessage
	if t := bytes.TrimSpace(raw); len(t) > 0 && t[0] == '[' {
		if err := json.Unmarshal(raw, &elems); err != nil {
			return nil, fmt.Errorf("parse array: %w", err)
		}
	} else {
		elems = []json.RawMessage{json.RawMessage(raw)}
	}

	events := make([]model.TelemetryEvent, 0, len(elems))
	for i, el := range elems {
		var rd reading
		if err := json.Unmarshal(el, &rd); err != nil {
			return nil, fmt.Errorf("reading %d: %w", i, err)
		}
		if rd.IMEI == "" {
			return nil, fmt.Errorf("reading %d: missing imei", i)
		}

		ts := time.Now().UTC()
		if rd.CreatedAt != nil {
			ts = rd.CreatedAt.UTC()
		}

		// Keep the full io blob (queryable) + the raw doc (fidelity).
		attrs := map[string]any{}
		for k, v := range rd.IO {
			attrs[k] = v
		}
		attrs["_raw"] = el

		var speed, heading float64
		if rd.Speed != nil {
			speed = *rd.Speed
		}
		if rd.Bearing != nil {
			heading = *rd.Bearing
		}

		events = append(events, model.TelemetryEvent{
			Source:      model.Source("generic"),
			DeviceIdent: rd.IMEI,
			Time:        ts,
			Lat:         rd.Latitude,
			Lon:         rd.Longitude,
			SpeedKmh:    speed,
			Heading:     heading,
			Ignition:    rd.IgnitionOn,
			AltitudeM:   rd.Altitude,
			Satellites:  rd.Sat,
			Attrs:       attrs,
		})
	}
	return events, nil
}
