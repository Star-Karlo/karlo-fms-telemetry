// Package geocode is a standalone reverse-geocoding service: it maps a
// coordinate to its Indonesian kecamatan (district), kota/kabupaten (city), and
// provinsi via a PostGIS point-in-polygon lookup, fronted by a geohash-keyed
// Redis cache. It has no knowledge of tenants, vehicles, or telemetry.
package geocode

import (
	"context"
	"strings"
	"time"

	"github.com/mmcloughlin/geohash"
)

// Place is the location a coordinate falls in: the administrative region
// (kecamatan → kabupaten → provinsi) plus the nearest OSM street, when one is
// within StreetMaxDistM.
type Place struct {
	Kelurahan     string `json:"kelurahan"` // ADM4 village / urban village
	Kecamatan     string `json:"kecamatan"`
	Kabupaten     string `json:"kabupaten"` // kota/kabupaten = the "city" level
	Provinsi      string `json:"provinsi"`
	KelurahanCode string `json:"kelurahan_code"` // BPS ADM4 P-code

	Street      string  `json:"street,omitempty"`        // nearest OSM road name
	StreetDistM float64 `json:"street_dist_m,omitempty"` // metres to that road
}

// Result is the reverse-geocode response for one coordinate. Location fields are
// always present (empty string when not found / not available), never omitted.
type Result struct {
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Found     bool    `json:"found"`
	Cached    bool    `json:"cached"` // true if served from the Redis cache
	Provinsi  string  `json:"provinsi"`
	Kabupaten string  `json:"kabupaten"` // regency (empty if the ADM2 is a city)
	Kota      string  `json:"Kota"`      // city (empty if the ADM2 is a regency)
	Kecamatan string  `json:"kecamatan"`
	Kelurahan string  `json:"kelurahan"` // village (empty until ADM4 data is loaded)
	Jalan     string  `json:"jalan"`     // nearest OSM street (empty if none within range)
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
		r.Provinsi = e.Place.Provinsi
		r.Kecamatan = e.Place.Kecamatan
		r.Kelurahan = e.Place.Kelurahan
		r.Jalan = e.Place.Street
		// ADM2 name is "Kota X" for cities, else a regency → split into Kota/kabupaten.
		if kab := e.Place.Kabupaten; strings.HasPrefix(kab, "Kota ") {
			r.Kota = strings.TrimPrefix(kab, "Kota ")
		} else {
			r.Kabupaten = kab
		}
	}
	// Kelurahan (ADM4) is always present but empty until that data is loaded.
	return r
}
