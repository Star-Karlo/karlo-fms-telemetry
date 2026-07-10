// Command archiver exports one UTC day of historical telemetry to Parquet for
// cold storage. Run daily (EventBridge → ECS Scheduled Task, later): each run
// archives the day that just crossed the retention window. S3 upload is stubbed.
//
//	DATABASE_URL   postgres connection
//	ARCHIVE_DIR    output root (default ./archive)
//	RETENTION_DAYS window before a day is archived (default 30)
//	ARCHIVE_DATE   optional YYYY-MM-DD override (backfill / testing)
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/karlo/fms/internal/archive"
)

func main() {
	ctx := context.Background()

	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fms")
	baseDir := env("ARCHIVE_DIR", "./archive")
	retentionDays := envInt("RETENTION_DAYS", 30)

	// Which day to archive: an explicit ARCHIVE_DATE, else the day that just
	// crossed the retention window (now - RETENTION_DAYS).
	day := time.Now().UTC().AddDate(0, 0, -retentionDays)
	if d := os.Getenv("ARCHIVE_DATE"); d != "" {
		t, err := time.Parse("2006-01-02", d)
		if err != nil {
			log.Fatalf("bad ARCHIVE_DATE %q: %v", d, err)
		}
		day = t
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	path, n, err := archive.ExportDay(ctx, pool, baseDir, day)
	if err != nil {
		log.Fatalf("export %s: %v", day.Format("2006-01-02"), err)
	}
	log.Printf("archived %d rows for %s -> %s  (S3 upload: TODO)", n, day.Format("2006-01-02"), path)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
