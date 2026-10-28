package routecache

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"
)

// Service answers a routing question from the cache, and goes upstream only
// when the cache cannot.
//
// TWO UPSTREAMS, ONE DOOR. up is the complete provider (MAPID) and answers
// anything. router is optional and free per call (Karlo's own Valhalla), and
// answers plain coordinate routing only. Callers never choose; they ask this
// service and read `source` to find out who answered. That is the whole point
// of putting the switch here rather than in each caller: TMS deployed against
// these endpoints on 2026-10-27 and does not have to be touched again to move
// off a paid provider.
type Service struct {
	store  *Store
	up     Upstream
	router Upstream
	shadow *Shadow
}

func NewService(store *Store, up Upstream) *Service { return &Service{store: store, up: up} }

// WithRouter sets the engine used for plain coordinate routing. Nil, or an
// unconfigured one, leaves every endpoint on up.
func (s *Service) WithRouter(r Upstream) *Service {
	s.router = r
	return s
}

// WithShadow prices every toll answer a second time, against Karlo's own
// tables, and records both. It never changes what is served — see Shadow.
func (s *Service) WithShadow(sh *Shadow) *Service {
	if sh.Configured() {
		s.shadow = sh
	}
	return s
}

// pick chooses who answers r.
//
// Everything that is not plain coordinate routing stays on up, and so does
// anything the router would have to ignore to answer:
//
//   - route_ids names a journey by ids registered with MAPID.
//   - route_toll needs a fare, which the router does not compute.
//   - alternatives asks for several routes; the router returns one, and a
//     single route handed back for a request that asked for three reads as a
//     sparse day rather than a provider that cannot.
//
// Options the router CAN honour are not grounds to fall back: snap_preventions
// is passed through and mapped onto Valhalla's search filters.
func (s *Service) pick(n Request) Upstream {
	if s.router == nil || n.Endpoint == "route_ids" || n.Endpoint == "route_toll" || n.Alternatives > 1 {
		return s.up
	}
	return s.router
}

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

// ErrNoIDs is an id-routed request without exactly two ids.
var ErrNoIDs = errors.New("exactly two location ids are required")

// ErrAlternativesNeedTwoPoints is alternatives asked for on a journey with
// waypoints, which MAPID cannot answer.
var ErrAlternativesNeedTwoPoints = errors.New("alternatives are only available for a journey of exactly two points")

// Get returns the answer for r, going upstream on a miss.
func (s *Service) Get(ctx context.Context, r Request) (Answer, error) {
	if r.Endpoint == "route_ids" {
		if len(r.LocationIDs) != 2 {
			return Answer{}, ErrNoIDs
		}
	} else if len(r.Points) < 2 {
		return Answer{}, ErrNoPoints
	}
	// Refused rather than quietly answered with one route: MAPID honours
	// alternatives only between two points, and a caller that asked for
	// three and silently received one would not know to look.
	if r.Alternatives > 1 && len(r.Points) != 2 {
		return Answer{}, ErrAlternativesNeedTwoPoints
	}
	n := r.Normalise()
	// Who answers is decided BEFORE the key, because who answered is part of
	// what the key identifies.
	up := s.pick(n)
	answer, err := s.serve(ctx, n, up)
	// The router refusing an endpoint is a routing decision, not a failure.
	// Retry on the complete provider, which re-keys: the answer now coming
	// back is a different engine's and must not land in the router's entry.
	if errors.Is(err, ErrUnsupported) && up != s.up {
		log.Printf("routecache: %s does not answer %s — using %s", up.Name(), n.Endpoint, s.up.Name())
		answer, err = s.serve(ctx, n, s.up)
		up = s.up
	}

	// Price it again against our own tables and write both down. Deliberately
	// on the answer being RETURNED, cached or fresh: the toll hit rate is
	// around 92%, so shadowing only misses would yield a handful of
	// comparisons a day instead of one per distinct journey. Compare returns
	// before doing any work and cannot affect this answer.
	if err == nil && n.Endpoint == "route_toll" && s.shadow != nil {
		n.Provider = up.Fingerprint()
		s.shadow.Compare(n, answer.Payload)
	}
	return answer, err
}

// serve answers n from up, through the cache.
func (s *Service) serve(ctx context.Context, n Request, up Upstream) (Answer, error) {
	n.Provider = up.Fingerprint()
	key := n.Key()

	if e, ok, err := s.store.Get(ctx, key); err != nil {
		// A cache that cannot be read must not stop the answer: fall
		// through and fetch it.
		log.Printf("routecache: read %x: %v", key[:6], err)
	} else if ok {
		at := e.CachedAt
		return Answer{Payload: e.Payload, Cached: true, CachedAt: &at, Hits: e.Hits, Source: up.Name()}, nil
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
			return Answer{Payload: e.Payload, Cached: true, CachedAt: &at, Hits: e.Hits, Source: up.Name()}, nil
		}
	}

	var payload []byte
	switch n.Endpoint {
	case "route_toll":
		payload, err = up.RouteToll(ctx, n.Profile, n.Points)
	case "route_ids":
		payload, err = up.RouteByIDs(ctx, n.LocationIDs, false)
	default:
		payload, err = up.Route(ctx, n)
	}
	if err != nil {
		return Answer{}, err
	}
	if err := s.store.Put(ctx, key, n, payload); err != nil {
		// Storing failed: the caller still gets its answer, we just fetched
		// one we will fetch again.
		log.Printf("routecache: write %x: %v", key[:6], err)
	}
	return Answer{Payload: payload, Cached: false, Source: up.Name()}, nil
}

// PurgeLocation drops the cached journeys that name a location id.
func (s *Service) PurgeLocation(ctx context.Context, id string) (int64, error) {
	return s.store.PurgeByLocationID(ctx, id)
}

// PurgeNear drops the cached journeys passing within radiusM of a point.
func (s *Service) PurgeNear(ctx context.Context, lon, lat, radiusM float64) (int64, error) {
	return s.store.PurgeNear(ctx, lon, lat, radiusM)
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
