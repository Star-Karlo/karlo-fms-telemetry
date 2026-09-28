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
		pr.Post("/v1/route-ids", h.routeIDs)
		pr.Post("/v1/route-cache/purge", h.purge)
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
// RouteIDsRequest names a journey by Karlo's location ids instead of
// coordinates (MAPID's /v4/route).
type RouteIDsRequest struct {
	// LocationIDs: exactly two, loading point first.
	LocationIDs []string `json:"location_ids"`
}

// PurgeRequest drops cached journeys after a place has moved.
type PurgeRequest struct {
	// LocationID drops every journey naming this id.
	LocationID string `json:"location_id,omitempty"`
	// Lon/Lat/RadiusM drops every journey with a point within the radius
	// (default 200 m) — the coordinate equivalent.
	Lon     float64 `json:"lon,omitempty"`
	Lat     float64 `json:"lat,omitempty"`
	RadiusM float64 `json:"radius_m,omitempty"`
}

type RouteRequest struct {
	// Points are [lon, lat] in TRAVEL ORDER — MAPID's order, and the order
	// the cache keys on.
	Points  [][2]float64 `json:"points"`
	Profile string       `json:"profile,omitempty"` // truck (default) | car | motorcycle | foot
}

// route godoc
//
//	@Summary		Route between points (cached)
//	@Description	MAPID's routing answer for these points, served from the shared cache when it has been asked before and bought upstream when it has not. For a journey whose FARE you also need, call /v1/route-toll instead — its answer already contains the route. The response carries MAPID's payload untouched under `payload`, plus whether it was cached. Points are [lon, lat] in travel order; order is part of the identity, so A→B and B→A are different journeys.
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
//	@Description	As /v1/route, with MAPID's toll pricing. IF YOU NEED BOTH THE ROUTE AND THE FARE, CALL ONLY THIS: the payload carries `route`, `distance` and `eta` alongside `prices`, so asking /v1/route as well buys a second answer for something you already have — and one computation means the fare and the geometry cannot describe slightly different roads. `prices` is the total per golongan, the figure to use; `details` are the gates as evidence, naming both ends of each segment, and their prices are a partition of the total rather than charges to add up. All five golongan come back in one answer, so the caller picks its class and the cache holds one entry per journey rather than five.
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

// routeIDs godoc
//
//	@Summary		Route between two Karlo location ids (cached)
//	@Description	MAPID's /v4/route, which takes Karlo's own location ids registered with MAPID rather than coordinates — it may know a yard or gate centroid that a dropped pin does not hit, so its answer can differ from the coordinate route for the same pair. Cached on the id pair, which is a steadier key than coordinates. A pair MAPID does not recognise comes back 502 and is not cached, so a caller can fall back to /v1/route exactly as it does today. If a location MOVES, call /v1/route-cache/purge with its id — the id does not change when the place does.
//	@Tags			routing
//	@Accept			json
//	@Produce		json
//	@Param			body	body		RouteIDsRequest	true	"two location ids, loading point first"
//	@Success		200		{object}	Answer
//	@Failure		400		{object}	map[string]string
//	@Failure		401		{object}	map[string]string
//	@Failure		502		{object}	map[string]string
//	@Security		ServiceToken
//	@Router			/v1/route-ids [post]
func (h *Handler) routeIDs(w http.ResponseWriter, r *http.Request) {
	var req RouteIDsRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	ans, err := h.svc.Get(r.Context(), Request{Endpoint: "route_ids", LocationIDs: req.LocationIDs})
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, ans)
	case err == ErrNoIDs:
		writeErr(w, http.StatusBadRequest, err.Error())
	default:
		writeErr(w, http.StatusBadGateway, err.Error())
	}
}

// purge godoc
//
//	@Summary		Drop cached journeys for a place that has moved
//	@Description	A cached answer is only as true as the place it was computed for. Coordinates change when a warehouse moves, so those entries simply go unused — but a LOCATION ID does not change, and would keep serving the route to where the warehouse used to be until it expires. Call this with the id when a location moves, or with lon/lat (+radius, default 200 m) to drop every journey touching a point.
//	@Tags			routing
//	@Accept			json
//	@Produce		json
//	@Param			body	body		PurgeRequest	true	"location_id, or lon+lat"
//	@Success		200		{object}	map[string]int64	"{\"purged\": n}"
//	@Failure		400		{object}	map[string]string
//	@Failure		401		{object}	map[string]string
//	@Security		ServiceToken
//	@Router			/v1/route-cache/purge [post]
func (h *Handler) purge(w http.ResponseWriter, r *http.Request) {
	var req PurgeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	var n int64
	var err error
	switch {
	case strings.TrimSpace(req.LocationID) != "":
		n, err = h.svc.PurgeLocation(r.Context(), strings.TrimSpace(req.LocationID))
	case req.Lon != 0 || req.Lat != 0:
		radius := req.RadiusM
		if radius <= 0 {
			radius = 200
		}
		n, err = h.svc.PurgeNear(r.Context(), req.Lon, req.Lat, radius)
	default:
		writeErr(w, http.StatusBadRequest, "location_id, or lon and lat, are required")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"purged": n})
}

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
