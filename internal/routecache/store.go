package routecache

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultTTL is how long a MAPID answer is kept. A road's geometry and a
// toll tariff both change on the scale of months, and the cost of a stale
// route is one re-plan; the cost of no cache is a bill.
const DefaultTTL = 90 * 24 * time.Hour

// Entry is a stored answer.
type Entry struct {
	Payload  json.RawMessage
	CachedAt time.Time
	Hits     int64
}

// Store is the route_cache table.
type Store struct {
	pool *pgxpool.Pool
	ttl  time.Duration
}

func NewStore(pool *pgxpool.Pool, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Store{pool: pool, ttl: ttl}
}

// Get returns a live entry and counts the hit. An expired row is treated as
// absent and left for the sweep — deleting it here would put a write in the
// path of a request that is about to do a slower thing anyway.
func (s *Store) Get(ctx context.Context, key []byte) (Entry, bool, error) {
	var e Entry
	err := s.pool.QueryRow(ctx, `
		UPDATE route_cache
		   SET hits = hits + 1, last_used_at = now()
		 WHERE key = $1 AND expires_at > now()
		RETURNING payload, created_at, hits`, key).Scan(&e.Payload, &e.CachedAt, &e.Hits)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

// Put stores an answer, replacing any expired one under the same key.
func (s *Store) Put(ctx context.Context, key []byte, r Request, payload []byte) error {
	n := r.Normalise()
	pts, err := json.Marshal(n.Points)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO route_cache (key, endpoint, profile, points, payload, expires_at)
		VALUES ($1, $2, $3, $4, $5, now() + $6::interval)
		ON CONFLICT (key) DO UPDATE
		   SET payload = excluded.payload, points = excluded.points,
		       created_at = now(), last_used_at = now(), expires_at = excluded.expires_at, hits = 0`,
		key, n.Endpoint, n.Profile, pts, payload, s.ttl.String())
	return err
}

// Sweep deletes expired rows and returns how many went.
func (s *Store) Sweep(ctx context.Context) (int64, error) {
	ct, err := s.pool.Exec(ctx, `DELETE FROM route_cache WHERE expires_at <= now()`)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

// Lock serialises the callers that miss on the SAME key.
//
// A lane ten trucks are planned onto at 07:00 is ten identical misses and,
// without this, ten paid MAPID calls before the first answer is stored. The
// first caller takes a transaction-scoped advisory lock and buys the answer;
// the others wait on the lock, then find the row. The lock is derived from
// the key, so different journeys never block each other.
func (s *Store) Lock(ctx context.Context, key []byte) (release func(), err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	id := int64(binary.BigEndian.Uint64(key[:8]))
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, id); err != nil {
		conn.Release()
		return nil, err
	}
	return func() {
		// Best effort: a lost unlock is released when the connection closes.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, id)
		conn.Release()
	}, nil
}
