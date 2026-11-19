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

// AdminMaxSnapM caps how far a point may sit outside every kelurahan polygon and
// still take that polygon's admin chain.
//
// The COD-AB boundaries follow the coastline, so a port's quay, jetty or
// reclaimed apron falls outside them by a few hundred metres. Without a snap,
// every loading point in the country reverse-geocodes to a street with no
// region at all — which is what Pelabuhan Tanjung Buton (0.9661, 102.2531) did:
// a road name, and empty provinsi / kabupaten / kecamatan / kelurahan, while
// the Mengkapan polygon carrying the right chain sat a few hundred metres away.
//
// The cap is what keeps that useful rather than reckless: a genuinely offshore
// point, such as a truck mid-strait on a ferry, stays unresolved instead of
// claiming whichever coast happens to be nearest. Even where the snapped
// kelurahan is a guess at this range, the kabupaten and provinsi above it are
// not.
const AdminMaxSnapM = 2000.0

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
		p.Provinsi = canonProvince(deref(prov))
		p.Kabupaten = deref(kab)
		p.Kecamatan = deref(kec)
		p.Kelurahan = deref(kel)
		p.KelurahanCode = deref(kelCode)
	} else {
		// No polygon contains the point. Rather than return an empty region,
		// take the nearest one when it is close enough to mean something, and
		// mark it approximate so a caller that must not guess can tell.
		snapped, dist, ok, err := s.nearestAdmin(ctx, lat, lon)
		if err != nil {
			return Place{}, false, err
		}
		if ok && dist <= AdminMaxSnapM {
			p.Provinsi = canonProvince(snapped.Provinsi)
			p.Kabupaten = snapped.Kabupaten
			p.Kecamatan = snapped.Kecamatan
			p.Kelurahan = snapped.Kelurahan
			p.KelurahanCode = snapped.KelurahanCode
			p.AdminApprox = true
			p.AdminDistM = dist
			found = true
		}
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

// nearestAdmin returns the kelurahan polygon closest to the point and the
// geodesic distance to it.
//
// Ordering by the `<->` KNN operator rides the same GiST index as the
// containment test, so the scan is an index walk, not 82k distance
// calculations; the true geodesic distance is then measured on the single row
// that wins. This runs only when containment missed, which means inland points
// — the overwhelming majority of every reading we geocode — never pay for it.
func (s *Store) nearestAdmin(ctx context.Context, lat, lon float64) (Place, float64, bool, error) {
	const q = `
WITH p AS (SELECT ST_SetSRID(ST_Point($1, $2), 4326) AS pt)
SELECT n.prov_name, n.kab_name, n.kec_name, n.kel_name, n.kel_code, n.dist_m
FROM p
JOIN LATERAL (
  SELECT prov_name, kab_name, kec_name, kel_name, kel_code,
         ST_Distance(geom::geography, p.pt::geography) AS dist_m
  FROM kelurahan ORDER BY geom <-> p.pt LIMIT 1
) n ON true`

	var (
		prov, kab, kec, kel, kelCode *string
		dist                         *float64
	)
	if err := s.pool.QueryRow(ctx, q, lon, lat).
		Scan(&prov, &kab, &kec, &kel, &kelCode, &dist); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Place{}, 0, false, nil // no boundary data loaded
		}
		return Place{}, 0, false, err
	}
	if dist == nil {
		return Place{}, 0, false, nil
	}
	return Place{
		Provinsi:      deref(prov),
		Kabupaten:     deref(kab),
		Kecamatan:     deref(kec),
		Kelurahan:     deref(kel),
		KelurahanCode: deref(kelCode),
	}, *dist, true, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
