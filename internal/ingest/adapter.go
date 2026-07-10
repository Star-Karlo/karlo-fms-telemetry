package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// Reading is one normalized telemetry record: the required core + a metadata
// bag for everything else. Adapters produce these; the handler enriches with
// location and the writer persists them.
type Reading struct {
	IMEI       string
	GPSTime    time.Time // dateGps — device fix time (canonical)
	ReceivedAt time.Time // dateCreated — when our backend received it
	Lat, Lon   float64
	Bearing    float64
	Speed      float64
	Ignition   *bool
	Metadata   map[string]any // altitude, satellites, full io, source, _raw, …
}

// Adapter converts a vendor's raw payload into normalized Readings. One adapter
// per vendor; everything downstream is vendor-agnostic.
type Adapter interface {
	Parse(raw []byte) ([]Reading, error)
}

// Adapters maps a vendor/source key (the {vendor} path segment) to its Adapter.
var Adapters = map[string]Adapter{
	"generic": genericAdapter{},
}

// GenericReading is the JSON body accepted by POST /v1/ingest/generic — either
// a single object or a JSON array of these. Top-level fields are consistent
// across our vendors; the `io` object varies and is kept whole in metadata.
type GenericReading struct {
	IMEI            string         `json:"imei" example:"864454076648348"`                        // device IMEI (required)
	Latitude        float64        `json:"latitude" example:"-6.68211"`                           // GPS latitude  -> geom column
	Longitude       float64        `json:"longitude" example:"111.4544916"`                       // GPS longitude -> geom column
	Bearing         *float64       `json:"bearing" example:"106"`                                 // heading degrees -> bearing column
	Speed           *float64       `json:"speed" example:"0"`                                     // speed -> speed column
	IgnitionOn      *bool          `json:"ignitionOn" example:"false"`                            // ignition on/off -> ignition column
	CreatedAt       *time.Time     `json:"createdAt" example:"2026-07-10T13:12:31.530Z"`          // dateCreated (server receive) -> received_at column
	LatestCreatedAt *time.Time     `json:"latestCreatedAt" example:"2026-07-10T06:12:29Z"`        // dateGps (device fix time) -> time column
	Altitude        *float64       `json:"altitude" example:"20"`                                 // altitude -> metadata
	Sat             *int           `json:"sat" example:"14"`                                      // satellite count -> metadata
	IO              map[string]any `json:"io" swaggertype:"object,string"`                        // full vendor io blob -> metadata
}

type genericAdapter struct{}

// Parse accepts either a single reading object or a JSON array of readings.
func (genericAdapter) Parse(raw []byte) ([]Reading, error) {
	var elems []json.RawMessage
	if t := bytes.TrimSpace(raw); len(t) > 0 && t[0] == '[' {
		if err := json.Unmarshal(raw, &elems); err != nil {
			return nil, fmt.Errorf("parse array: %w", err)
		}
	} else {
		elems = []json.RawMessage{json.RawMessage(raw)}
	}

	readings := make([]Reading, 0, len(elems))
	for i, el := range elems {
		var d GenericReading
		if err := json.Unmarshal(el, &d); err != nil {
			return nil, fmt.Errorf("reading %d: %w", i, err)
		}
		if d.IMEI == "" {
			return nil, fmt.Errorf("reading %d: missing imei", i)
		}

		now := time.Now().UTC()
		// dateGps: prefer device fix time, fall back to createdAt, then now.
		gps := now
		switch {
		case d.LatestCreatedAt != nil:
			gps = d.LatestCreatedAt.UTC()
		case d.CreatedAt != nil:
			gps = d.CreatedAt.UTC()
		}
		// dateCreated: their system's creation time, else now.
		received := now
		if d.CreatedAt != nil {
			received = d.CreatedAt.UTC()
		}

		// metadata = full io + non-core fields + raw doc (fidelity).
		meta := map[string]any{}
		for k, v := range d.IO {
			meta[k] = v
		}
		if d.Altitude != nil {
			meta["altitude"] = *d.Altitude
		}
		if d.Sat != nil {
			meta["satellites"] = *d.Sat
		}
		meta["source"] = "generic"
		meta["_raw"] = el

		readings = append(readings, Reading{
			IMEI:       d.IMEI,
			GPSTime:    gps,
			ReceivedAt: received,
			Lat:        d.Latitude,
			Lon:        d.Longitude,
			Bearing:    deref(d.Bearing),
			Speed:      deref(d.Speed),
			Ignition:   d.IgnitionOn,
			Metadata:   meta,
		})
	}
	return readings, nil
}

func deref(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}
