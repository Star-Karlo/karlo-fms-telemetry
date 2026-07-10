// Package ingest is the multi-vendor telemetry intake service: devices POST
// readings, registered IMEIs are normalized and written to the telemetry
// hypertable. It authorizes devices against an in-memory registry (refreshed
// from Postgres) so it never queries the DB per reading.
package ingest

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Entry is a registered device resolved to its current vehicle.
type Entry struct {
	DeviceID  int64
	TenantID  int64
	VehicleID *int64 // nil when the device isn't currently assigned to a vehicle
	Status    string
}

// Registry is an in-memory snapshot of registered devices keyed by wire
// identifier (IMEI). It refreshes from Postgres on a ticker, so a Lookup on the
// ingest hot path is a plain map read — zero DB round-trips per reading.
type Registry struct {
	pool *pgxpool.Pool
	m    atomic.Pointer[map[string]Entry] // swapped atomically on each refresh
}

func NewRegistry(pool *pgxpool.Pool) *Registry {
	r := &Registry{pool: pool}
	empty := map[string]Entry{}
	r.m.Store(&empty)
	return r
}

// Refresh loads the full active-device registry (with current vehicle) into a
// new map and swaps it in atomically.
func (r *Registry) Refresh(ctx context.Context) (int, error) {
	const q = `
SELECT d.identifier, d.id, d.tenant_id, d.status, da.vehicle_id
FROM devices d
LEFT JOIN device_assignments da
  ON da.device_id = d.id AND da.valid_to IS NULL
WHERE d.status = 'active'`

	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	next := make(map[string]Entry)
	for rows.Next() {
		var (
			ident string
			e     Entry
			veh   *int64
		)
		if err := rows.Scan(&ident, &e.DeviceID, &e.TenantID, &e.Status, &veh); err != nil {
			return 0, err
		}
		e.VehicleID = veh
		next[ident] = e
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	r.m.Store(&next)
	return len(next), nil
}

// StartRefresh loads once (failing fast at startup) then refreshes on interval.
func (r *Registry) StartRefresh(ctx context.Context, interval time.Duration, onErr func(error)) error {
	if _, err := r.Refresh(ctx); err != nil {
		return err
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := r.Refresh(ctx); err != nil && onErr != nil {
					onErr(err)
				}
			}
		}
	}()
	return nil
}

// Lookup returns the entry for an identifier (in-memory map read).
func (r *Registry) Lookup(identifier string) (Entry, bool) {
	m := *r.m.Load()
	e, ok := m[identifier]
	return e, ok
}

// Size is the number of registered devices currently in memory.
func (r *Registry) Size() int { return len(*r.m.Load()) }
