package geocode

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StreetMaxDistM caps how far the nearest road may be before we treat the point
// as "not on a street" (e.g. mid-field, at sea). Roads beyond this are dropped.
const StreetMaxDistM = 150.0

// Store runs the PostGIS lookups: point-in-polygon against the COD-AB
// `kelurahan` boundaries (each ADM4 polygon carries its full parent chain) and
// nearest-line KNN against the OSM `road` table for the street.
type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Lookup resolves (lat, lon) to its admin hierarchy and nearest named street in
// one round-trip. found=false when the point is outside all kelurahan polygons
// and has no named road within StreetMaxDistM.
func (s *Store) Lookup(ctx context.Context, lat, lon float64) (Place, bool, error) {
	// NOTE: ST_Point takes (x, y) = (lon, lat). One point-in-polygon against the
	// kelurahan (ADM4) polygon returns the whole chain; the road subquery orders
	// by the `<->` KNN operator (GiST) then measures the true geodesic distance.
	const q = `
WITH p AS (SELECT ST_SetSRID(ST_Point($1, $2), 4326) AS pt)
SELECT k.prov_name, k.kab_name, k.kec_name, k.kel_name, k.kel_code,
       r.name, r.dist_m
FROM p
LEFT JOIN LATERAL (
  SELECT prov_name, kab_name, kec_name, kel_name, kel_code
  FROM kelurahan WHERE ST_Contains(geom, p.pt) LIMIT 1
) k ON true
LEFT JOIN LATERAL (
  SELECT name, ST_Distance(geom::geography, p.pt::geography) AS dist_m
  FROM road ORDER BY geom <-> p.pt LIMIT 1
) r ON true`

	var (
		prov, kab, kec, kel, kelCode, streetName *string
		streetDist                               *float64
	)
	if err := s.pool.QueryRow(ctx, q, lon, lat).
		Scan(&prov, &kab, &kec, &kel, &kelCode, &streetName, &streetDist); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Place{}, false, nil
		}
		return Place{}, false, err
	}

	var p Place
	found := prov != nil || kab != nil || kec != nil || kel != nil
	if found {
		p.Provinsi = deref(prov)
		p.Kabupaten = deref(kab)
		p.Kecamatan = deref(kec)
		p.Kelurahan = deref(kel)
		p.KelurahanCode = deref(kelCode)
	}
	if streetName != nil && streetDist != nil && *streetDist <= StreetMaxDistM {
		p.Street = *streetName
		p.StreetDistM = *streetDist
		found = true // a resolved street alone counts as a hit
	}
	if !found {
		return Place{}, false, nil
	}
	return p, true, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
