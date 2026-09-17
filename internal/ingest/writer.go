package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is a Reading plus its reverse-geocoded location, ready to persist.
type Row struct {
	Reading   Reading
	Kecamatan string
	Kelurahan string
	Kabupaten string
	Kota      string
	Provinsi  string
	Jalan     string
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
   kecamatan, kelurahan, kabupaten, kota, provinsi, jalan, metadata)
VALUES
  ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
   $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb)`

// upsertLatestSQL keeps one current-state row per device, advancing it only when
// the incoming GPS fix is newer (so buffered old readings can't overwrite it).
const upsertLatestSQL = `
INSERT INTO latest
  (imei, time, received_at, geom, bearing, speed, ignition,
   kecamatan, kelurahan, kabupaten, kota, provinsi, jalan, metadata)
VALUES
  ($3, $1, $2, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
   $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb)
ON CONFLICT (imei) DO UPDATE SET
  time = excluded.time, received_at = excluded.received_at, geom = excluded.geom,
  bearing = excluded.bearing, speed = excluded.speed, ignition = excluded.ignition,
  kecamatan = excluded.kecamatan, kelurahan = excluded.kelurahan,
  kabupaten = excluded.kabupaten, kota = excluded.kota,
  provinsi = excluded.provinsi, jalan = excluded.jalan,
  -- Fuel level carries forward. Trackers that read the tank through an LLS
  -- or CAN only include it while the engine runs; the "on stop" records
  -- omit it, and the current-state row would show a truck with no fuel
  -- sensor every night. Keep the last reading (and _fuel_at, stamped by the
  -- writer when a reading arrives) until a fresher one replaces it. The
  -- history rows stay raw.
  metadata = excluded.metadata || COALESCE(
    (SELECT jsonb_object_agg(k, v) FROM jsonb_each(latest.metadata) AS e(k, v)
      WHERE k IN ('Fuel Level', 'Fuel Level (CAN)', '_fuel_at') AND NOT (excluded.metadata ? k)),
    '{}'::jsonb)
WHERE excluded.time > latest.time`

// fuelKeys are the metadata keys that carry a tank reading, in the shape the
// adapters normalise them to.
var fuelKeys = []string{"Fuel Level", "Fuel Level (CAN)"}

// stampFuel adds _fuel_at (the fix time, RFC3339 UTC) when the reading
// carries a fuel level, so a carried-forward value can say how old it is.
func stampFuel(md map[string]any, at time.Time) map[string]any {
	for _, k := range fuelKeys {
		if _, ok := md[k]; ok {
			out := make(map[string]any, len(md)+1)
			for kk, vv := range md {
				out[kk] = vv
			}
			out["_fuel_at"] = at.UTC().Format(time.RFC3339)
			return out
		}
	}
	return md
}

func (w *Writer) WriteBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, r := range rows {
		meta, err := json.Marshal(stampFuel(r.Reading.Metadata, r.Reading.GPSTime))
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		// Same params for both statements (upsert reorders them by $N).
		args := []any{
			r.Reading.GPSTime, r.Reading.ReceivedAt, r.Reading.IMEI,
			r.Reading.Lon, r.Reading.Lat,
			r.Reading.Bearing, r.Reading.Speed, r.Reading.Ignition,
			nullStr(r.Kecamatan), nullStr(r.Kelurahan), nullStr(r.Kabupaten),
			nullStr(r.Kota), nullStr(r.Provinsi), nullStr(r.Jalan),
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
