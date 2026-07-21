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
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/karlo/fms/internal/archive"
)

func main() {
	ctx := context.Background()

	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fms")
	baseDir := env("ARCHIVE_DIR", "./archive")
	retentionDays := envInt("RETENTION_DAYS", 14) // 2 weeks hot; older -> cold

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
	log.Printf("archived %d rows for %s -> %s", n, day.Format("2006-01-02"), path)

	// Upload to S3 Glacier Instant Retrieval (cheap cold storage, ms retrieval)
	// when a bucket is configured. Otherwise the Parquet stays local.
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		log.Printf("S3_BUCKET not set — skipping upload (local file only)")
		return
	}
	key := fmt.Sprintf("telemetry/date=%s/part.parquet", day.Format("2006-01-02"))
	if err := archive.UploadFile(ctx, bucket, key, path, types.StorageClassGlacierIr); err != nil {
		log.Fatalf("s3 upload: %v", err)
	}
	log.Printf("uploaded -> s3://%s/%s  (GLACIER_IR)", bucket, key)

	// Mobile (driver-keyed) telemetry: same day, same bucket, its own folder so
	// it maps to a distinct Athena table. A failure here must not fail the
	// telemetry archive that already succeeded above.
	mPath, mn, err := archive.ExportMobileDay(ctx, pool, baseDir, day)
	if err != nil {
		log.Printf("WARN mobile export %s: %v", day.Format("2006-01-02"), err)
		return
	}
	log.Printf("archived %d mobile rows for %s -> %s", mn, day.Format("2006-01-02"), mPath)

	mKey := fmt.Sprintf("mobile/date=%s/part.parquet", day.Format("2006-01-02"))
	if err := archive.UploadFile(ctx, bucket, mKey, mPath, types.StorageClassGlacierIr); err != nil {
		log.Printf("WARN mobile s3 upload: %v", err)
		return
	}
	log.Printf("uploaded -> s3://%s/%s  (GLACIER_IR)", bucket, mKey)
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
