// Package api wires the HTTP router, middleware, Swagger UI, and handlers.
package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	// Generated OpenAPI docs (run `make swagger` to regenerate). Blank import
	// registers the spec that Swagger UI serves at /swagger/doc.json.
	_ "github.com/karlo/fms/docs"
)

// NewRouter builds the API handler tree.
func NewRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer)

	r.Get("/healthz", health)

	// Swagger UI: http://localhost:8080/swagger/index.html
	r.Get("/swagger/*", httpSwagger.WrapHandler)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/vehicles", listVehicles)
	})

	return r
}
