// Package rds carries the RDS telemetry schema so that tools (cmd/telmigrate)
// embed the one canonical file rather than a copy that can drift.
package rds

import _ "embed"

//go:embed schema.sql
var Schema string
