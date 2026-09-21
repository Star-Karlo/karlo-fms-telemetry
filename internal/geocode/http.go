package geocode

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
)

// Routes registers the geocode endpoints on r.
func (s *Service) Routes(r chi.Router) {
	r.Get("/reverse", s.handleReverse)
	r.Get("/search", s.handleSearch)
}

// handleSearch godoc
//
//	@Summary		Forward geocode: an address or place name to candidate pins (Indonesia)
//	@Description	Searches the service's own road and administrative-area data (no external provider). "Jl. Raya Bekasi, Cikarang Utara": the part before the first comma is the road or area, the rest narrows the area. Roads first, then kelurahan / kecamatan / kabupaten. Not house-number precise. Requires a platform service token as Bearer.
//	@Tags			geocode
//	@Produce		json
//	@Security		ServiceToken
//	@Param			q		query		string	true	"query, at least 3 characters"	example(Jl. Raya Bekasi, Cikarang)
//	@Param			limit	query		int		false	"max candidates (default 10, max 25)"
//	@Success		200		{array}		Candidate
//	@Failure		401		{object}	map[string]string
//	@Router			/search [get]
func (s *Service) handleSearch(w http.ResponseWriter, r *http.Request) {
	if len(s.serviceTokens) > 0 {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if _, known := s.serviceTokens[strings.TrimSpace(tok)]; !ok || !known {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "a platform service token is required (Authorization: Bearer …)"})
			return
		}
	}
	q := r.URL.Query().Get("q")
	if len(strings.TrimSpace(q)) < 3 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "q must be at least 3 characters"})
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	out, err := s.store.Search(r.Context(), q, limit)
	if err != nil {
		log.Printf("geocode search %q: %v", q, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "search failed"})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleReverse godoc
//
//	@Summary	Reverse geocode a coordinate to kecamatan / kota / provinsi
//	@Tags		geocode
//	@Produce	json
//	@Param		lat	query		number	true	"Latitude"	example(-6.1751)
//	@Param		lon	query		number	true	"Longitude"	example(106.8272)
//	@Success	200	{object}	Result
//	@Failure	400	{object}	map[string]string
//	@Router		/reverse [get]
func (s *Service) handleReverse(w http.ResponseWriter, r *http.Request) {
	lat, err1 := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, err2 := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "lat and lon are required numeric query params",
		})
		return
	}
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "lat/lon out of range"})
		return
	}

	res, err := s.Reverse(r.Context(), lat, lon)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "lookup failed"})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
