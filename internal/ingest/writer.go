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
  --
  -- _state_since: when the truck's drive state (moving / idle / parking)
  -- last changed, so a consumer can say "idle for 40 min" without walking
  -- history. The writer stamps every reading with its own fix time; here the
  -- previous stamp survives while the class is unchanged. The class rule is
  -- fms-backend's (liveview.movingSpeed = 3 km/h, then ignition); "offline"
  -- is a function of the fix's age, which the reader derives.
  metadata = excluded.metadata || COALESCE(
    (SELECT jsonb_object_agg(k, v) FROM jsonb_each(latest.metadata) AS e(k, v)
      WHERE k IN ('Fuel Level', 'Fuel Level (CAN)', '_fuel_at') AND NOT (excluded.metadata ? k)),
    '{}'::jsonb)
    || CASE
         WHEN latest.metadata ? '_state_since'
          AND (CASE WHEN latest.speed > 3 THEN 'moving' WHEN latest.ignition THEN 'idle' ELSE 'parking' END)
            = (CASE WHEN excluded.speed > 3 THEN 'moving' WHEN excluded.ignition THEN 'idle' ELSE 'parking' END)
         THEN jsonb_build_object('_state_since', latest.metadata->'_state_since')
         ELSE '{}'::jsonb
       END
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

	// _state_since (see upsertLatestSQL): every reading is stamped with its
	// own fix time; the upsert keeps the older stamp while the drive state
	// holds. History rows are written without it.
	stateSince := func(md map[string]any, at time.Time) map[string]any {
		out := make(map[string]any, len(md)+1)
		for k, v := range md {
			out[k] = v
		}
		out["_state_since"] = at.UTC().Format(time.RFC3339)
		return out
	}

	// History only accepts fixes inside the partition window (retention
	// days back, a few days ahead — partition_maintenance keeps 8 and 3).
	// Outside it, INSERT fails with "no partition … found" and, because a
	// batch is one transaction, took every other row down with it: on
	// 2026-09-17 one GMT truck whose last packet was 9 days old put all 20
	// Raja Besi trucks offline for 14 hours. Such a row still refreshes the
	// current-state row (the upsert ignores anything older than what it
	// holds), it just is not appended to history.
	now := time.Now()
	inWindow := func(t time.Time) bool {
		return t.After(now.AddDate(0, 0, -historyDaysBack)) && t.Before(now.AddDate(0, 0, historyDaysAhead))
	}

	batch := &pgx.Batch{}
	queued := 0
	for _, r := range rows {
		stamped := stampFuel(r.Reading.Metadata, r.Reading.GPSTime)
		meta, err := json.Marshal(stamped)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		latestMeta, err := json.Marshal(stateSince(stamped, r.Reading.GPSTime))
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		// Same params for both statements (upsert reorders them by $N); only
		// the metadata differs.
		args := []any{
			r.Reading.GPSTime, r.Reading.ReceivedAt, r.Reading.IMEI,
			r.Reading.Lon, r.Reading.Lat,
			r.Reading.Bearing, r.Reading.Speed, r.Reading.Ignition,
			nullStr(r.Kecamatan), nullStr(r.Kelurahan), nullStr(r.Kabupaten),
			nullStr(r.Kota), nullStr(r.Provinsi), nullStr(r.Jalan),
			string(meta),
		}
		if inWindow(r.Reading.GPSTime) {
			batch.Queue(insertSQL, args...) // historical (append)
			queued++
		}
		latestArgs := append(append([]any{}, args[:14]...), string(latestMeta))
		batch.Queue(upsertLatestSQL, latestArgs...) // current state (conditional upsert)
		queued++
	}

	br := w.pool.SendBatch(ctx, batch)
	var firstErr error
	for i := 0; i < queued; i++ {
		if _, err := br.Exec(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr == nil || len(rows) == 1 {
		return firstErr
	}
	// The batch failed as a whole. Retry one row at a time so a single bad
	// reading costs itself, not the other 19 trucks; report the first error.
	var rowErr error
	for i := range rows {
		if err := w.WriteBatch(ctx, rows[i:i+1]); err != nil && rowErr == nil {
			rowErr = fmt.Errorf("%w (batch of %d retried row by row)", err, len(rows))
		}
	}
	return rowErr
}

// historyDaysBack / historyDaysAhead mirror partition_maintenance(tbl, 8, 3)
// with a day of slack on the old side so a fix from exactly 8 days ago,
// arriving before tonight's drop, is still accepted.
const (
	historyDaysBack  = 7
	historyDaysAhead = 3
)

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
