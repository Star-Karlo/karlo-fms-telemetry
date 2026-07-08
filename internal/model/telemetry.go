// Package model defines the canonical shapes shared across the pipeline.
//
// TelemetryEvent is THE contract: every source adapter (Teltonika TCP,
// Safetrucks REST, future vendors) converts its wire format into this one
// struct. Everything downstream — validation, storage, live state, geofencing —
// consumes TelemetryEvent and never knows or cares where the ping came from.
// Adding a new tracker vendor means writing one adapter, not touching the core.
package model

import "time"

// Source identifies which adapter produced an event.
type Source string

const (
	SourceTeltonika  Source = "teltonika"
	SourceSafetrucks Source = "safetrucks"
)

// TelemetryEvent is a single normalized position/status report from a device.
type TelemetryEvent struct {
	Source Source // which adapter produced this

	// DeviceIdent is the wire-level device identity: Teltonika IMEI, or the
	// Safetrucks external id. Ingest resolves this to a device (and, via the
	// temporal assignment, to a vehicle) before writing.
	DeviceIdent string

	Time time.Time // device timestamp of the fix (UTC)

	Lat float64
	Lon float64

	SpeedKmh float64
	Heading  float64 // degrees, 0-360

	// Optional fields — nil when the device/protocol didn't report them.
	Ignition   *bool
	AltitudeM  *float64
	Satellites *int

	// Attrs carries vendor-specific IO elements (fuel level, temperatures,
	// custom Teltonika IO IDs, etc.) that don't have a first-class column.
	Attrs map[string]any
}
