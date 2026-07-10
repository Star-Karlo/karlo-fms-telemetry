// Package geocode is a standalone reverse-geocoding service: it maps a
// coordinate to its Indonesian kecamatan (district), kota/kabupaten (city), and
// provinsi via a PostGIS point-in-polygon lookup, fronted by a geohash-keyed
// Redis cache. It has no knowledge of tenants, vehicles, or telemetry.
package geocode

import (
	"context"
	"time"

	"github.com/mmcloughlin/geohash"
)

// Place is the administrative location a coordinate falls in.
type Place struct {
	Kecamatan     string `json:"kecamatan"`
	Kabupaten     string `json:"kabupaten"` // kota/kabupaten = the "city" level
	Provinsi      string `json:"provinsi"`
	KecamatanCode string `json:"kecamatan_code"` // BPS P-code
}

// Result is the reverse-geocode response for one coordinate.
type Result struct {
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	Found  bool    `json:"found"`
	Cached bool    `json:"cached"` // true if served from the Redis cache
	*Place `json:",inline"`
}

// entry is what we persist in the cache (positive and negative results alike).
type entry struct {
	Found bool  `json:"f"`
	Place Place `json:"p"`
}

// Service resolves coordinates, caching by geohash cell.
type Service struct {
	store     *Store
	cache     *Cache
	precision uint          // geohash character length (7 ≈ 150m cells)
	ttl       time.Duration // cache entry lifetime
}

func NewService(store *Store, cache *Cache, precision uint, ttl time.Duration) *Service {
	return &Service{store: store, cache: cache, precision: precision, ttl: ttl}
}

// Reverse maps (lat, lon) to a Place. Cache hit → no DB query. Cache miss →
// PostGIS lookup, then the result (found or not) is cached for the geohash cell.
func (s *Service) Reverse(ctx context.Context, lat, lon float64) (Result, error) {
	hash := geohash.EncodeWithPrecision(lat, lon, s.precision)

	if e, ok := s.cache.Get(ctx, hash); ok {
		return result(lat, lon, e, true), nil
	}

	place, found, err := s.store.Lookup(ctx, lat, lon)
	if err != nil {
		return Result{}, err
	}
	e := entry{Found: found, Place: place}
	s.cache.Set(ctx, hash, e, s.ttl) // cache negatives too (points at sea)
	return result(lat, lon, e, false), nil
}

func result(lat, lon float64, e entry, cached bool) Result {
	r := Result{Lat: lat, Lon: lon, Found: e.Found, Cached: cached}
	if e.Found {
		p := e.Place
		r.Place = &p
	}
	return r
}
