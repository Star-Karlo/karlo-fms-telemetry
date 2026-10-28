// Command geocode is the standalone reverse-geocoding service.
//
//	@title			Karlo FMS Geocode Service
//	@version		0.1
//	@description	Two jobs behind one process: geocoding (lat/lon -> kecamatan / kota / provinsi, PostGIS + Redis geohash cache; /search for forward lookups) and the platform's routing cache (/v1/route, /v1/route-toll, /v1/route-ids — answers kept 90 days in Postgres so the same lane is never bought twice). Routing has two possible engines and every answer names the one that produced it in `source`: MAPID, which is metered, and Karlo's own self-hosted Valhalla, which is free per call but answers coordinate routing only — id-routed journeys and toll fares stay on MAPID. ROUTE_ENGINE picks it at deploy time; callers see the same endpoints and shapes either way. With TOLL_SHADOW_URL set, every toll fare is also priced against Karlo's own tariff tables and both answers are recorded in toll_shadow; MAPID's is still the one returned, so callers see no difference.
//	@BasePath		/
//
//	@securityDefinitions.apikey	ServiceToken
//	@in							header
//	@name						Authorization
//	@description				Platform service token as "Bearer <token>" — required for /search and for every routing endpoint (they front a PAID upstream). /reverse stays open.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	_ "github.com/karlo/fms/cmd/geocode/docs"
	"github.com/karlo/fms/internal/geocode"
	"github.com/karlo/fms/internal/routecache"
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

	tokens := strings.Split(env("ACCEPTED_SERVICE_TOKENS", ""), ",")
	svc := geocode.NewService(geocode.NewStore(pool), geocode.NewCache(rdb), precision, ttl).
		WithServiceTokens(tokens)

	// The routing cache rides along: same pool, same deploy, its own table
	// and its own auth. Without a MAPID key it still serves whatever is
	// already cached and fails only on a miss — geocoding is unaffected
	// either way, which is why a missing key is a log line and not a fatal.
	routeUp := routecache.NewMAPID(env("MAPID_BASE_URL", ""), os.Getenv("MAPID_API_KEY"))
	routeTTL := time.Duration(envInt("ROUTE_CACHE_TTL_DAYS", 90)) * 24 * time.Hour
	routeSvc := routecache.NewService(routecache.NewStore(pool, routeTTL), routeUp)
	if !routeUp.Configured() {
		log.Printf("routing: MAPID_API_KEY is not set — cached answers only, every miss will fail")
	}

	// ROUTE_ENGINE=valhalla moves plain coordinate routing onto Karlo's own
	// engine. Everything else — id-routed journeys and toll — stays on MAPID,
	// which is why this is one variable and not a migration: callers see the
	// same endpoints, the same shapes and the same host, and `source` on each
	// answer says who produced it.
	//
	// DEFAULT IS MAPID, deliberately. The switch is a deploy-time decision
	// someone makes and can undo in one revision, not something that happens
	// because a URL was filled in.
	if strings.EqualFold(env("ROUTE_ENGINE", "mapid"), "valhalla") {
		router := routecache.NewValhalla(env("ROUTING_BASE_URL", ""), os.Getenv("ROUTING_API_TOKEN"))
		if router.Configured() {
			routeSvc = routeSvc.WithRouter(router)
			log.Printf("routing: coordinate routing on %s (%s); ids and toll stay on MAPID",
				env("ROUTING_BASE_URL", ""), router.Fingerprint())
		} else {
			log.Printf("routing: ROUTE_ENGINE=valhalla but ROUTING_BASE_URL is empty — staying on MAPID")
		}
	}
	// TOLL_SHADOW_URL prices every toll answer a second time against Karlo's
	// own tariff tables and records both, without changing what is served.
	// Empty — the default — leaves it off entirely.
	//
	// This is how the in-house pricer earns its way to being the primary: it
	// is not accurate enough today, and six hand-built lanes are too few to
	// fix it against. Shadowing turns every ordinary order into a measured
	// comparison at no extra cost, because MAPID is called either way.
	if u := env("TOLL_SHADOW_URL", ""); u != "" {
		routeSvc = routeSvc.WithShadow(routecache.NewShadow(pool, u, os.Getenv("TOLL_SHADOW_TOKEN")))
		log.Printf("routing: toll answers shadowed against %s — MAPID is still served", u)
	}

	go routeSvc.Sweep(ctx, time.Hour)

	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Get("/swagger/*", httpSwagger.WrapHandler)
	svc.Routes(r)
	routecache.NewHandler(routeSvc, tokens).Routes(r)

	log.Printf("geocode service on %s  (routing cache ttl %s, swagger: http://localhost%s/swagger/index.html)", addr, routeTTL, addr)
	// Bounded server: a client that opens a connection and never finishes its
	// headers, or trickles a body, must not hold a worker forever (slowloris).
	// WriteTimeout is generous because history and report responses are big;
	// long-running work (report render, clip download) is asynchronous anyway.
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		// Longer than MAPID's 60s upstream timeout: a routing miss waits on
		// it, and cutting the response after paying for the answer is the
		// one outcome worse than being slow.
		WriteTimeout:   120 * time.Second,
		IdleTimeout:    90 * time.Second,
		MaxHeaderBytes: 64 << 10,
	}
	log.Fatal(srv.ListenAndServe())
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
