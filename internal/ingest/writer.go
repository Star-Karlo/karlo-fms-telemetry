package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/karlo/fms/internal/model"
)

// Row is a resolved reading ready to persist: the canonical event plus the
// tenant/device/vehicle it was resolved to (stamped at write time).
type Row struct {
	Event     model.TelemetryEvent
	TenantID  int64
	DeviceID  int64
	VehicleID *int64
}

// Writer persists telemetry rows and updates Redis live state.
type Writer struct {
	pool *pgxpool.Pool
	rdb  *redis.Client
}

func NewWriter(pool *pgxpool.Pool, rdb *redis.Client) *Writer {
	return &Writer{pool: pool, rdb: rdb}
}

const insertSQL = `
INSERT INTO telemetry
  (time, tenant_id, device_id, vehicle_id, source, geom,
   speed_kmh, heading, ignition, altitude_m, satellites, attrs)
VALUES
  ($1, $2, $3, $4, $5, ST_SetSRID(ST_MakePoint($6, $7), 4326)::geography,
   $8, $9, $10, $11, $12, $13::jsonb)`

// WriteBatch inserts all rows in a single pgx batch, then best-effort updates
// live state. vehicle_id is denormalized onto each telemetry row here.
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
			r.Event.Time, r.TenantID, r.DeviceID, r.VehicleID, string(r.Event.Source),
			r.Event.Lon, r.Event.Lat,
			r.Event.SpeedKmh, r.Event.Heading, r.Event.Ignition, r.Event.AltitudeM, r.Event.Satellites,
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
	if firstErr != nil {
		return firstErr
	}

	w.updateLiveState(ctx, rows)
	return nil
}

// updateLiveState HSETs the last-known position per vehicle (best-effort; a
// Redis hiccup must never fail an ingest).
func (w *Writer) updateLiveState(ctx context.Context, rows []Row) {
	if w.rdb == nil {
		return
	}
	for _, r := range rows {
		if r.VehicleID == nil {
			continue
		}
		key := "vehicle:" + strconv.FormatInt(r.TenantID, 10) + ":" + strconv.FormatInt(*r.VehicleID, 10)
		w.rdb.HSet(ctx, key, map[string]any{
			"lat":     r.Event.Lat,
			"lon":     r.Event.Lon,
			"speed":   r.Event.SpeedKmh,
			"heading": r.Event.Heading,
			"time":    r.Event.Time.Unix(),
			"device":  r.DeviceID,
		})
		w.rdb.Expire(ctx, key, 24*time.Hour)
	}
}
