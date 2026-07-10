// Package archive exports historical telemetry to Parquet for cold storage.
// One UTC day per run; the S3 upload is intentionally left as a stub for now.
package archive

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

// Row is one telemetry record in the Parquet file: geom is flattened to
// lat/lon and metadata is kept as a JSON string. Pointer fields are nullable.
type Row struct {
	TimeMs     int64    `parquet:"time_ms"`     // dateGps, unix millis
	ReceivedMs *int64   `parquet:"received_ms"` // dateCreated, unix millis
	IMEI       string   `parquet:"imei"`
	Lat        *float64 `parquet:"lat"`
	Lon        *float64 `parquet:"lon"`
	Bearing    *float64 `parquet:"bearing"`
	Speed      *float64 `parquet:"speed"`
	Ignition   *bool    `parquet:"ignition"`
	Kecamatan  string   `parquet:"kecamatan"`
	Kabupaten  string   `parquet:"kabupaten"`
	Provinsi   string   `parquet:"provinsi"`
	Metadata   string   `parquet:"metadata"` // JSON
}

// ExportDay writes all telemetry for the UTC day of `day` to
// baseDir/telemetry/date=YYYY-MM-DD/part.parquet and returns the path + row count.
func ExportDay(ctx context.Context, pool *pgxpool.Pool, baseDir string, day time.Time) (string, int, error) {
	start := day.UTC().Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)

	const q = `
SELECT (extract(epoch from time) * 1000)::bigint,
       (extract(epoch from received_at) * 1000)::bigint,
       imei,
       ST_Y(geom::geometry), ST_X(geom::geometry),
       bearing, speed, ignition,
       coalesce(kecamatan, ''), coalesce(kabupaten, ''), coalesce(provinsi, ''),
       coalesce(metadata::text, '{}')
FROM telemetry
WHERE time >= $1 AND time < $2
ORDER BY time`

	dbRows, err := pool.Query(ctx, q, start, end)
	if err != nil {
		return "", 0, err
	}
	defer dbRows.Close()

	var rows []Row
	for dbRows.Next() {
		var r Row
		if err := dbRows.Scan(&r.TimeMs, &r.ReceivedMs, &r.IMEI, &r.Lat, &r.Lon,
			&r.Bearing, &r.Speed, &r.Ignition,
			&r.Kecamatan, &r.Kabupaten, &r.Provinsi, &r.Metadata); err != nil {
			return "", 0, err
		}
		rows = append(rows, r)
	}
	if err := dbRows.Err(); err != nil {
		return "", 0, err
	}

	dir := filepath.Join(baseDir, "telemetry", "date="+start.Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, err
	}
	path := filepath.Join(dir, "part.parquet")

	f, err := os.Create(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	w := parquet.NewGenericWriter[Row](f)
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			return "", 0, err
		}
	}
	if err := w.Close(); err != nil {
		return "", 0, err
	}

	// TODO(archival): upload `path` to s3://<bucket>/telemetry/date=.../part.parquet
	// (aws-sdk-go PutObject), then enable Timescale retention to drop the chunk.
	return path, len(rows), nil
}
