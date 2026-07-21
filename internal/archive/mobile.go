package archive

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

// MobileRow is one mobile (driver-keyed) telemetry record in Parquet — the same
// layout as Row with `driver` in place of `imei` and no `ignition` (phones have
// none), so the cold flow is identical to the GPS side.
type MobileRow struct {
	TimeMs     int64    `parquet:"time_ms"`     // device fix time, unix millis
	ReceivedMs *int64   `parquet:"received_ms"` // server receive time, unix millis
	Driver     string   `parquet:"driver"`
	Lat        *float64 `parquet:"lat"`
	Lon        *float64 `parquet:"lon"`
	Bearing    *float64 `parquet:"bearing"`
	Speed      *float64 `parquet:"speed"`
	Kecamatan  string   `parquet:"kecamatan"`
	Kelurahan  string   `parquet:"kelurahan"`
	Kabupaten  string   `parquet:"kabupaten"`
	Kota       string   `parquet:"kota"`
	Provinsi   string   `parquet:"provinsi"`
	Jalan      string   `parquet:"jalan"`
	Metadata   string   `parquet:"metadata"` // JSON
}

// ExportMobileDay writes all mobile telemetry for the UTC day of `day` to
// baseDir/mobile/date=YYYY-MM-DD/part.parquet and returns the path + row count.
// Mirrors ExportDay; the separate `mobile/` prefix keeps the two cold datasets
// in the same bucket but under distinct Athena tables.
func ExportMobileDay(ctx context.Context, pool *pgxpool.Pool, baseDir string, day time.Time) (string, int, error) {
	start := day.UTC().Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)

	const q = `
SELECT (extract(epoch from time) * 1000)::bigint,
       (extract(epoch from received_at) * 1000)::bigint,
       driver,
       ST_Y(geom::geometry), ST_X(geom::geometry),
       bearing, speed,
       coalesce(kecamatan, ''), coalesce(kelurahan, ''), coalesce(kabupaten, ''),
       coalesce(kota, ''), coalesce(provinsi, ''), coalesce(jalan, ''),
       coalesce(metadata::text, '{}')
FROM mobile_telemetry
WHERE time >= $1 AND time < $2
ORDER BY time`

	dbRows, err := pool.Query(ctx, q, start, end)
	if err != nil {
		return "", 0, err
	}
	defer dbRows.Close()

	var rows []MobileRow
	for dbRows.Next() {
		var r MobileRow
		if err := dbRows.Scan(&r.TimeMs, &r.ReceivedMs, &r.Driver, &r.Lat, &r.Lon,
			&r.Bearing, &r.Speed,
			&r.Kecamatan, &r.Kelurahan, &r.Kabupaten, &r.Kota, &r.Provinsi, &r.Jalan,
			&r.Metadata); err != nil {
			return "", 0, err
		}
		rows = append(rows, r)
	}
	if err := dbRows.Err(); err != nil {
		return "", 0, err
	}

	dir := filepath.Join(baseDir, "mobile", "date="+start.Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, err
	}
	path := filepath.Join(dir, "part.parquet")

	f, err := os.Create(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	w := parquet.NewGenericWriter[MobileRow](f)
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			return "", 0, err
		}
	}
	if err := w.Close(); err != nil {
		return "", 0, err
	}
	return path, len(rows), nil
}
