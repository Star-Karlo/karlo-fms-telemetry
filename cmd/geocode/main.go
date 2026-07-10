// Command geocode is the standalone reverse-geocoding service.
//
//	@title			Karlo FMS Geocode Service
//	@version		0.1
//	@description	Reverse geocoding: lat/lon -> kecamatan / kota / provinsi (PostGIS + Redis geohash cache).
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

	_ "github.com/karlo/fms/cmd/geocode/docs"
	"github.com/karlo/fms/internal/geocode"
)

func main() {
	ctx := context.Background()

	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fms")
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	addr := env("GEOCODE_ADDR", ":8081")
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

	svc := geocode.NewService(geocode.NewStore(pool), geocode.NewCache(rdb), precision, ttl)

	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Get("/swagger/*", httpSwagger.WrapHandler)
	svc.Routes(r)

	log.Printf("geocode service on %s  (swagger: http://localhost%s/swagger/index.html)", addr, addr)
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
