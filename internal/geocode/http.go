package geocode

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// Routes registers the geocode endpoints on r.
func (s *Service) Routes(r chi.Router) {
	r.Get("/reverse", s.handleReverse)
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
