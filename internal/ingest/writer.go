package ingest

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/karlo/fms/internal/model"
)

// Row is a reading ready to persist: the canonical event, its IMEI, and the
// reverse-geocoded location. tenant/device/vehicle are deferred (NULL for now).
type Row struct {
	Event     model.TelemetryEvent
	IMEI      string
	Kecamatan string
	Kabupaten string
	Provinsi  string
}

// Writer persists telemetry rows in a single batch.
type Writer struct {
	pool *pgxpool.Pool
}

func NewWriter(pool *pgxpool.Pool) *Writer {
	return &Writer{pool: pool}
}

const insertSQL = `
INSERT INTO telemetry
  (time, imei, source, geom, speed_kmh, heading, ignition, altitude_m,
   satellites, kecamatan, kabupaten, provinsi, attrs)
VALUES
  ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
   $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)`

func (w *Writer) WriteBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, r := range rows {
		attrs, err := json.Marshal(r.Event.Attrs)
		if err != nil {
			return fmt.Errorf("marshal attrs: %w", err)
		}
		batch.Queue(insertSQL,
			r.Event.Time, r.IMEI, string(r.Event.Source),
			r.Event.Lon, r.Event.Lat,
			r.Event.SpeedKmh, r.Event.Heading, r.Event.Ignition, r.Event.AltitudeM,
			r.Event.Satellites, nullStr(r.Kecamatan), nullStr(r.Kabupaten), nullStr(r.Provinsi),
			string(attrs),
		)
	}

	br := w.pool.SendBatch(ctx, batch)
	var firstErr error
	for range rows {
		if _, err := br.Exec(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
