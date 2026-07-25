// Command ingest is the multi-vendor telemetry ingestion service.
//
//	@title			Karlo FMS Ingest Service
//	@version		0.1
//	@description	Multi-vendor device telemetry intake. Devices POST readings; each is reverse-geocoded (kecamatan/kota/provinsi) and stored to the telemetry hypertable keyed by IMEI.
//	@BasePath		/
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	_ "github.com/karlo/fms/cmd/ingest/docs"
	"github.com/karlo/fms/internal/dashcam"
	"github.com/karlo/fms/internal/geocode"
	"github.com/karlo/fms/internal/ingest"
	"github.com/karlo/fms/internal/vss"
)

func main() {
	ctx := context.Background()

	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fms")
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	addr := env("INGEST_ADDR", ":8082")
	precision := uint(envInt("GEOHASH_PRECISION", 7))
	ttl := time.Duration(envInt("CACHE_TTL_HOURS", 720)) * time.Hour

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	// Cold reader (Athena over S3 Parquet) for history older than the retention
	// window. Enabled only when ATHENA_OUTPUT is set; otherwise history is hot-only.
	retentionDays := envInt("RETENTION_DAYS", 14)
	var cold *ingest.ColdReader
	if out := env("ATHENA_OUTPUT", ""); out != "" {
		cold, err = ingest.NewColdReader(ctx, env("ATHENA_DATABASE", "fms"), env("ATHENA_TABLE", "telemetry_cold"), out)
		if err != nil {
			log.Fatalf("athena init: %v", err)
		}
		log.Printf("cold reader enabled: %s.%s (boundary %dd)", env("ATHENA_DATABASE", "fms"), env("ATHENA_TABLE", "telemetry_cold"), retentionDays)
	}

	// Reverse-geocode in-process (shares the geocode PostGIS lookup + Redis cache).
	geoSvc := geocode.NewService(geocode.NewStore(pool), geocode.NewCache(rdb), precision, ttl)
	// Auth accepts the legacy shared secret and/or revocable keys from the
	// ingest_key table. Keys are enabled unless explicitly turned off, so a new
	// key issued with cmd/ingestkey works without redeploying.
	apiKey := env("INGEST_API_KEY", "")
	var keys *ingest.KeyStore
	if env("INGEST_KEYS_ENABLED", "1") != "0" {
		keys = ingest.NewKeyStore(pool)
	}
	switch {
	case apiKey == "" && keys == nil:
		log.Printf("WARNING: no INGEST_API_KEY and ingest_key table disabled — /v1/ingest is OPEN (no auth)")
	case apiKey == "":
		log.Printf("ingest auth enabled (X-Ingest-Key: ingest_key table only)")
	default:
		log.Printf("ingest auth enabled (X-Ingest-Key: shared secret + ingest_key table)")
	}
	// Mobile (driver-keyed) telemetry from the phone app. Its own tables and its
	// own cold table, so the IMEI identity space and the tracker analytics stay clean.
	var mobileCold *ingest.ColdReader
	if out := env("ATHENA_OUTPUT", ""); out != "" {
		mobileCold, err = ingest.NewColdReaderFor(ctx, env("ATHENA_DATABASE", "fms"),
			env("ATHENA_MOBILE_TABLE", "mobile_cold"), out, "driver")
		if err != nil {
			log.Fatalf("athena (mobile) init: %v", err)
		}
	}
	mob := ingest.NewMobileStore(pool, mobileCold, retentionDays)

	h := ingest.NewHandler(geoSvc, ingest.NewWriter(pool), ingest.NewReader(pool, cold, retentionDays), mob, apiKey, keys)

	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Get("/swagger/*", httpSwagger.WrapHandler)
	h.Routes(r)

	// Dashcam (Gtrack VSS) front. Empty VSS_BASE_URL leaves the endpoints
	// answering 503 and touches nothing else. Video clips land in the same
	// cold-storage bucket as the telemetry parquet, under video/.
	vssClient := vss.New(vss.Config{
		BaseURL:  env("VSS_BASE_URL", ""),
		Username: env("VSS_USERNAME", ""),
		Password: env("VSS_PASSWORD", ""),
	})
	videoBucket := env("VIDEO_S3_BUCKET", env("S3_BUCKET", "karlo-fms-cold-storage"))
	dashcam.NewHandler(vssClient, h.Authorized, videoBucket, pool).Routes(r)

	// Coverage/terrain spatial grid: nightly rollup of signal + ruggedness, plus
	// the read endpoint. Seeds the hot window on startup, then rolls up daily.
	cov := ingest.NewCoverage(pool)
	cov.Route(r)
	go cov.RunRollup(ctx, retentionDays)

	// Per-device daily time-in-state + distance. Same shape as coverage: roll
	// the hot window up in the background, serve the small result. Computing it
	// per request is not an option — the aggregate over raw telemetry is ~15s
	// for a single day.
	vd := ingest.NewVehicleDay(pool)
	vd.Route(r)
	go vd.RunRollup(ctx, retentionDays)

	// Live VSS alarm feed: one WebSocket to the camera platform, persisting
	// events for FMS to poll. No-op when VSS is not configured.
	go dashcam.NewAlarmRunner(vssClient, pool).Run(ctx)

	log.Printf("ingest service on %s  (swagger: http://localhost%s/swagger/index.html)", addr, addr)
	log.Fatal(http.ListenAndServe(addr, r))
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
