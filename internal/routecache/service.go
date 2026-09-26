package routecache

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"
)

// Service answers a routing question from the cache, and buys it from MAPID
// only when the cache cannot.
type Service struct {
	store *Store
	up    *MAPID
}

func NewService(store *Store, up *MAPID) *Service { return &Service{store: store, up: up} }

// Answer is what a caller gets: MAPID's payload untouched, plus enough for
// the caller to know what it is holding.
type Answer struct {
	Payload  json.RawMessage `json:"payload" swaggertype:"object"`
	Cached   bool            `json:"cached"`
	CachedAt *time.Time      `json:"cached_at,omitempty"`
	Hits     int64           `json:"hits,omitempty"`
	Source   string          `json:"source"` // mapid — the provider that produced it
}

// ErrNoPoints is a request that cannot be a journey.
var ErrNoPoints = errors.New("at least two points are required")

// Get returns the answer for r, buying it upstream on a miss.
func (s *Service) Get(ctx context.Context, r Request) (Answer, error) {
	if len(r.Points) < 2 {
		return Answer{}, ErrNoPoints
	}
	n := r.Normalise()
	key := n.Key()

	if e, ok, err := s.store.Get(ctx, key); err != nil {
		// A cache that cannot be read must not stop the answer: fall
		// through and pay for it.
		log.Printf("routecache: read %x: %v", key[:6], err)
	} else if ok {
		at := e.CachedAt
		return Answer{Payload: e.Payload, Cached: true, CachedAt: &at, Hits: e.Hits, Source: "mapid"}, nil
	}

	// Miss. Take the per-key lock so ten planners opening the same lane at
	// 07:00 buy it once; the nine that wait find the row below.
	release, err := s.store.Lock(ctx, key)
	if err != nil {
		log.Printf("routecache: lock %x: %v — proceeding unserialised", key[:6], err)
	} else {
		defer release()
		if e, ok, err := s.store.Get(ctx, key); err == nil && ok {
			at := e.CachedAt
			return Answer{Payload: e.Payload, Cached: true, CachedAt: &at, Hits: e.Hits, Source: "mapid"}, nil
		}
	}

	var payload []byte
	switch n.Endpoint {
	case "route_toll":
		payload, err = s.up.RouteToll(ctx, n.Profile, n.Points)
	default:
		payload, err = s.up.Route(ctx, n.Profile, n.Points)
	}
	if err != nil {
		return Answer{}, err
	}
	if err := s.store.Put(ctx, key, n, payload); err != nil {
		// Storing failed: the caller still gets its answer, we just paid
		// for one we will pay for again.
		log.Printf("routecache: write %x: %v", key[:6], err)
	}
	return Answer{Payload: payload, Cached: false, Source: "mapid"}, nil
}

// Sweep deletes expired rows on a ticker. Best-effort by design: a lookup
// already filters on expires_at, so a failed sweep costs disk, not
// correctness.
func (s *Service) Sweep(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = time.Hour
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			n, err := s.store.Sweep(cctx)
			cancel()
			if err != nil {
				log.Printf("routecache: sweep: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("routecache: swept %d expired entries", n)
			}
		}
	}
}
