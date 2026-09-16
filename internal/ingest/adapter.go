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

// Adapter converts a raw device payload into normalized Readings. Today there's
// a single generic adapter (our upstream already normalizes every vendor to one
// shape); the interface stays so a new device format can be added later without
// touching the handler.
type Adapter interface {
	Parse(raw []byte) ([]Reading, error)
}

// defaultAdapter parses the normalized payload all sources currently send.
var defaultAdapter Adapter = genericAdapter{}

// GenericReading is the JSON body accepted by POST /v1/ingest/generic — either
// a single object or a JSON array of these. Top-level fields are consistent
// across our vendors; the `io` object varies and is kept whole in metadata.
type GenericReading struct {
	IMEI         string         `json:"imei" example:"864454076648348"`              // device IMEI (required)
	Latitude     float64        `json:"latitude" example:"-6.68211"`                 // GPS latitude  -> geom column
	Longitude    float64        `json:"longitude" example:"111.4544916"`             // GPS longitude -> geom column
	Bearing      *float64       `json:"bearing" example:"106.5"`                     // heading degrees (float) -> bearing column
	Speed        *float64       `json:"speed" example:"0"`                           // speed -> speed column
	IgnitionOn   *bool          `json:"ignitionOn" example:"false"`                  // ignition on/off -> ignition column
	GpsCreatedAt *time.Time     `json:"gpsCreatedAt" example:"2026-07-10T06:12:29Z"` // dateGps (device fix time) -> time column. received_at is stamped by the server, not sent here.
	Altitude     *float64       `json:"altitude" example:"20"`                       // altitude -> metadata
	Sat          *int           `json:"sat" example:"14"`                            // satellite count -> metadata
	IO           map[string]any `json:"io" swaggertype:"object,string"`              // full vendor io blob -> metadata
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
		// dateGps: device fix time, fall back to now if the device didn't send one.
		gps := now
		if d.GpsCreatedAt != nil {
			gps = d.GpsCreatedAt.UTC()
		}
		// received_at is always stamped by us at ingest time (server clock),
		// never client-supplied — it records when the backend actually got it.
		received := now

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

		// Reject impossible device clocks. A GPS fix can't be in the future
		// (allow a little skew for clock drift) and shouldn't be absurdly old;
		// such values otherwise poison the `latest` upsert (which only advances
		// on a newer time), freezing the truck's live position. Fall back to the
		// server receive time, keeping the raw value in metadata for forensics.
		const maxClockSkew = 5 * time.Minute
		if gps.After(received.Add(maxClockSkew)) || gps.Before(received.AddDate(-1, 0, 0)) {
			meta["gps_time_raw"] = gps.Format(time.RFC3339)
			meta["gps_time_invalid"] = true
			gps = received
		}

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
