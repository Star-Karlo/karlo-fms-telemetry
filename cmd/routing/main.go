// Command routing is the platform's cache in front of MAPID.
//
// Several Karlo services ask MAPID for the same routes and the same toll
// fares over and over, each call paid and none of it kept. This service
// holds the answers: content-addressed, 90 days, in Postgres, shared by
// every product. On a miss it asks MAPID once — even when ten callers miss
// the same lane at the same moment — stores the reply verbatim and serves
// it from then on.
//
// It is not a router: it adds no routing logic and rewrites nothing. What
// MAPID said is what a caller gets, plus whether it was cached.
//
//	@title			Karlo Routing Cache
//	@version		0.1
//	@description	Cached MAPID routing and toll pricing, shared by the Karlo platform's services. Every endpoint requires a platform service token — a paid upstream is never behind an open door.
//	@BasePath		/
//
//	@securityDefinitions.apikey	ServiceToken
//	@in							header
//	@name						Authorization
//	@description				Platform service token as "Bearer <token>".
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
	httpSwagger "github.com/swaggo/http-swagger/v2"

	_ "github.com/karlo/fms/cmd/routing/docs"
	"github.com/karlo/fms/internal/routecache"
)

func main() {
	ctx := context.Background()

	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fms")
	addr := env("ROUTING_ADDR", ":8083")
	ttl := time.Duration(envInt("ROUTE_CACHE_TTL_DAYS", 90)) * 24 * time.Hour

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	up := routecache.NewMAPID(env("MAPID_BASE_URL", ""), os.Getenv("MAPID_API_KEY"))
	if !up.Configured() {
		// Deliberately not fatal: the service still answers from cache, and
		// a missing key must be visible in the log rather than as a crash
		// loop that takes the cached answers down with it.
		log.Printf("routing: MAPID_API_KEY is not set — cached answers only, every miss will fail")
	}
	tokens := strings.Split(env("ACCEPTED_SERVICE_TOKENS", ""), ",")
	if len(tokens) == 0 || strings.TrimSpace(tokens[0]) == "" {
		log.Printf("routing: ACCEPTED_SERVICE_TOKENS is empty — every request will be refused")
	}

	store := routecache.NewStore(pool, ttl)
	svc := routecache.NewService(store, up)
	go svc.Sweep(ctx, time.Hour)

	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Get("/swagger/*", httpSwagger.WrapHandler)
	routecache.NewHandler(svc, tokens).Routes(r)

	log.Printf("routing cache on %s (ttl %s, swagger: http://localhost%s/swagger/index.html)", addr, ttl, addr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Longer than the upstream's 60s: a miss waits on MAPID, and cutting
		// the response after paying for it is the one thing worse than slow.
		WriteTimeout:   90 * time.Second,
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
