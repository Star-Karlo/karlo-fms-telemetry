package geocode

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store runs the PostGIS point-in-polygon query against the kecamatan table.
type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Lookup returns the kecamatan containing (lat, lon). found=false when the point
// falls outside all kecamatan polygons (e.g. at sea / outside Indonesia).
func (s *Store) Lookup(ctx context.Context, lat, lon float64) (Place, bool, error) {
	// NOTE: ST_Point takes (x, y) = (lon, lat).
	const q = `
SELECT kec_name, kab_name, prov_name, kec_code
FROM kecamatan
WHERE ST_Contains(geom, ST_SetSRID(ST_Point($1, $2), 4326))
LIMIT 1`

	var p Place
	err := s.pool.QueryRow(ctx, q, lon, lat).
		Scan(&p.Kecamatan, &p.Kabupaten, &p.Provinsi, &p.KecamatanCode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Place{}, false, nil
		}
		return Place{}, false, err
	}
	return p, true, nil
}
