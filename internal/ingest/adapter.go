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
// Add teltonika/safetrucks etc. here later.
var Adapters = map[string]Adapter{
	"generic": genericAdapter{},
}

// genericReading is the JSON shape the generic adapter accepts. Missing numeric
// fields default to 0; the original payload is preserved in attrs._raw.
type genericReading struct {
	IMEI       string         `json:"imei"`
	Timestamp  *int64         `json:"timestamp"` // unix seconds; nil -> now
	Lat        float64        `json:"lat"`
	Lon        float64        `json:"lon"`
	SpeedKmh   float64        `json:"speed_kmh"`
	Heading    float64        `json:"heading"`
	Ignition   *bool          `json:"ignition"`
	AltitudeM  *float64       `json:"altitude_m"`
	Satellites *int           `json:"satellites"`
	Attrs      map[string]any `json:"attrs"` // vendor-specific extras
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
		var rd genericReading
		if err := json.Unmarshal(el, &rd); err != nil {
			return nil, fmt.Errorf("reading %d: %w", i, err)
		}
		if rd.IMEI == "" {
			return nil, fmt.Errorf("reading %d: missing imei", i)
		}

		ts := time.Now().UTC()
		if rd.Timestamp != nil {
			ts = time.Unix(*rd.Timestamp, 0).UTC()
		}

		attrs := map[string]any{}
		for k, v := range rd.Attrs {
			attrs[k] = v
		}
		attrs["_raw"] = el // original payload for this reading (re-processable)

		events = append(events, model.TelemetryEvent{
			Source:      model.Source("generic"),
			DeviceIdent: rd.IMEI,
			Time:        ts,
			Lat:         rd.Lat,
			Lon:         rd.Lon,
			SpeedKmh:    rd.SpeedKmh,
			Heading:     rd.Heading,
			Ignition:    rd.Ignition,
			AltitudeM:   rd.AltitudeM,
			Satellites:  rd.Satellites,
			Attrs:       attrs,
		})
	}
	return events, nil
}
