package ingest

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is a Reading plus its reverse-geocoded location, ready to persist.
type Row struct {
	Reading   Reading
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

// insertSQL appends a reading to the historical hypertable.
const insertSQL = `
INSERT INTO telemetry
  (time, received_at, imei, geom, bearing, speed, ignition,
   kecamatan, kabupaten, provinsi, metadata)
VALUES
  ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
   $6, $7, $8, $9, $10, $11, $12::jsonb)`

// upsertLatestSQL keeps one current-state row per device, advancing it only when
// the incoming GPS fix is newer (so buffered old readings can't overwrite it).
const upsertLatestSQL = `
INSERT INTO latest
  (imei, time, received_at, geom, bearing, speed, ignition,
   kecamatan, kabupaten, provinsi, metadata)
VALUES
  ($3, $1, $2, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
   $6, $7, $8, $9, $10, $11, $12::jsonb)
ON CONFLICT (imei) DO UPDATE SET
  time = excluded.time, received_at = excluded.received_at, geom = excluded.geom,
  bearing = excluded.bearing, speed = excluded.speed, ignition = excluded.ignition,
  kecamatan = excluded.kecamatan, kabupaten = excluded.kabupaten,
  provinsi = excluded.provinsi, metadata = excluded.metadata
WHERE excluded.time > latest.time`

func (w *Writer) WriteBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, r := range rows {
		meta, err := json.Marshal(r.Reading.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		// Same params for both statements (upsert reorders them by $N).
		args := []any{
			r.Reading.GPSTime, r.Reading.ReceivedAt, r.Reading.IMEI,
			r.Reading.Lon, r.Reading.Lat,
			r.Reading.Bearing, r.Reading.Speed, r.Reading.Ignition,
			nullStr(r.Kecamatan), nullStr(r.Kabupaten), nullStr(r.Provinsi),
			string(meta),
		}
		batch.Queue(insertSQL, args...)       // historical (append)
		batch.Queue(upsertLatestSQL, args...) // current state (conditional upsert)
	}

	br := w.pool.SendBatch(ctx, batch)
	var firstErr error
	for i := 0; i < len(rows)*2; i++ { // two statements queued per row
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
