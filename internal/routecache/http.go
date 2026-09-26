package routecache

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// Handler exposes the cache to other Karlo services.
//
// Every route is behind a platform service token — no exceptions and no
// public path. The callers are machines, so requiring a token from the
// first day costs nobody a migration, and it means a PAID upstream is never
// reachable by anyone who finds the hostname. (The lesson is next door:
// geocode's /reverse was left open and now serves 36k calls a day from
// environments nobody can name.)
type Handler struct {
	svc    *Service
	tokens map[string]struct{}
}

func NewHandler(svc *Service, tokens []string) *Handler {
	set := map[string]struct{}{}
	for _, t := range tokens {
		if t = strings.TrimSpace(t); t != "" {
			set[t] = struct{}{}
		}
	}
	return &Handler{svc: svc, tokens: set}
}

func (h *Handler) Routes(r chi.Router) {
	r.Group(func(pr chi.Router) {
		pr.Use(h.requireToken)
		pr.Post("/v1/route", h.route)
		pr.Post("/v1/route-toll", h.routeToll)
	})
}

func (h *Handler) requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			writeErr(w, http.StatusUnauthorized, "missing service token")
			return
		}
		if _, ok := h.tokens[strings.TrimSpace(tok)]; !ok {
			writeErr(w, http.StatusUnauthorized, "invalid service token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RouteRequest is the body both endpoints take.
type RouteRequest struct {
	// Points are [lon, lat] in TRAVEL ORDER — MAPID's order, and the order
	// the cache keys on.
	Points  [][2]float64 `json:"points"`
	Profile string       `json:"profile,omitempty"` // truck (default) | car | motorcycle | foot
}

// route godoc
//
//	@Summary		Route between points (cached)
//	@Description	MAPID's routing answer for these points, served from the shared cache when it has been asked before and bought upstream when it has not. The response carries MAPID's payload untouched under `payload`, plus whether it was cached. Points are [lon, lat] in travel order; order is part of the identity, so A→B and B→A are different journeys.
//	@Tags			routing
//	@Accept			json
//	@Produce		json
//	@Param			body	body		RouteRequest	true	"points + profile"
//	@Success		200		{object}	Answer
//	@Failure		400		{object}	map[string]string
//	@Failure		401		{object}	map[string]string
//	@Failure		502		{object}	map[string]string	"MAPID refused or was unreachable; its own message is passed through"
//	@Security		ServiceToken
//	@Router			/v1/route [post]
func (h *Handler) route(w http.ResponseWriter, r *http.Request) { h.answer(w, r, "route") }

// routeToll godoc
//
//	@Summary		Route with toll fares (cached)
//	@Description	As /v1/route, with MAPID's toll pricing. The payload carries `prices` (the total per golongan — the figure to use) and `details` (the gates, as evidence: the per-gate prices are a partition of the total, not charges to add up). All five golongan come back in one answer, so the caller picks the class it needs and the cache holds one entry per journey rather than five.
//	@Tags			routing
//	@Accept			json
//	@Produce		json
//	@Param			body	body		RouteRequest	true	"points + profile"
//	@Success		200		{object}	Answer
//	@Failure		400		{object}	map[string]string
//	@Failure		401		{object}	map[string]string
//	@Failure		502		{object}	map[string]string
//	@Security		ServiceToken
//	@Router			/v1/route-toll [post]
func (h *Handler) routeToll(w http.ResponseWriter, r *http.Request) { h.answer(w, r, "route_toll") }

func (h *Handler) answer(w http.ResponseWriter, r *http.Request, endpoint string) {
	var req RouteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	pts := make([]Point, 0, len(req.Points))
	for _, p := range req.Points {
		pts = append(pts, Point{p[0], p[1]})
	}
	ans, err := h.svc.Get(r.Context(), Request{Endpoint: endpoint, Profile: req.Profile, Points: pts})
	switch {
	case err == nil:
	case err == ErrNoPoints:
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	default:
		// MAPID's wording is specific and useful; pass it through rather
		// than replacing it with "upstream error".
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ans)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
