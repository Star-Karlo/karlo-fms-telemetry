package ingest

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"
)

const (
	earthRadiusM      = 6371000.0 // mean Earth radius (metres)
	maxGeofenceChecks = 5000
)

// haversineMeters returns the great-circle distance between two lat/lon points
// in metres. Accurate to well within a metre at city/warehouse scale — plenty
// for radius geofencing (2–8 km rings).
func haversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	rlat1 := lat1 * math.Pi / 180
	rlat2 := lat2 * math.Pi / 180
	dlat := (lat2 - lat1) * math.Pi / 180
	dlon := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dlat/2)*math.Sin(dlat/2) +
		math.Cos(rlat1)*math.Cos(rlat2)*math.Sin(dlon/2)*math.Sin(dlon/2)
	return 2 * earthRadiusM * math.Asin(math.Sqrt(a))
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}

// GeofenceCheck is one item in a batch geofence request.
type GeofenceCheck struct {
	IMEI      string   `json:"imei" example:"350612071419004"`   // device to test (its latest live position)
	CenterLat float64  `json:"center_lat" example:"-6.2"`         // warehouse center latitude
	CenterLon float64  `json:"center_lon" example:"106.8"`        // warehouse center longitude
	RadiusM   *float64 `json:"radius_m,omitempty" example:"8000"` // optional; if set, `inside` = distance_m <= radius_m
}

// GeofenceRequest is the batch body for POST /v1/geofence/check.
type GeofenceRequest struct {
	Checks []GeofenceCheck `json:"checks"`
}

// GeofenceResult is one check's answer. distance_m is the real output — the
// caller applies its own distance bands (arrive/out/enter) on it.
type GeofenceResult struct {
	IMEI      string     `json:"imei"`
	Found     bool       `json:"found"`                 // false if the truck has no live position
	DistanceM *float64   `json:"distance_m,omitempty"`  // metres from the point to the center
	Inside    *bool      `json:"inside,omitempty"`      // present only when radius_m was given
	Lat       *float64   `json:"lat,omitempty"`         // the position used
	Lon       *float64   `json:"lon,omitempty"`
	Time      *time.Time `json:"time,omitempty"`        // fix time of that position
}

// GeofenceResponse wraps the batch results (order matches the request).
type GeofenceResponse struct {
	Results []GeofenceResult `json:"results"`
}

// handleGeofenceCheckBatch godoc
//
//	@Summary		Batch radius check: are these trucks within their circles?
//	@Description	For each {imei, center, radius}, returns the truck's current distance to the center (and `inside` if radius given). One request, one indexed lookup. The caller (platform) applies its own distance bands (arrive/out/enter) on distance_m. Trucks with no live position return found:false.
//	@Tags			geofence
//	@Accept			json
//	@Produce		json
//	@Param			payload	body		GeofenceRequest	true	"Batch of checks (max 5000)"
//	@Success		200		{object}	GeofenceResponse
//	@Failure		400		{object}	map[string]string
//	@Router			/v1/geofence/check [post]
func (h *Handler) handleGeofenceCheckBatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body"})
		return
	}
	var req GeofenceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if len(req.Checks) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "checks is empty"})
		return
	}
	if len(req.Checks) > maxGeofenceChecks {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("too many checks (max %d)", maxGeofenceChecks)})
		return
	}

	seen := map[string]bool{}
	imeis := make([]string, 0, len(req.Checks))
	for i, c := range req.Checks {
		if c.IMEI == "" || !validIMEI(c.IMEI) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("check %d: missing/invalid imei", i)})
			return
		}
		if !validLatLon(c.CenterLat, c.CenterLon) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("check %d: center out of range", i)})
			return
		}
		if !seen[c.IMEI] {
			seen[c.IMEI] = true
			imeis = append(imeis, c.IMEI)
		}
	}

	positions, err := h.r.LivePositions(r.Context(), imeis)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}

	results := make([]GeofenceResult, 0, len(req.Checks))
	for _, c := range req.Checks {
		results = append(results, evalCheck(c.IMEI, c.CenterLat, c.CenterLon, c.RadiusM, positions[c.IMEI], positionExists(positions, c.IMEI)))
	}
	writeJSON(w, http.StatusOK, GeofenceResponse{Results: results})
}

// handleGeofenceCheckOne godoc
//
//	@Summary		Single radius check (convenience/testing)
//	@Description	Distance of one truck (imei) or an arbitrary point (lat/lon) to a center. Returns the same fields as one batch result.
//	@Tags			geofence
//	@Produce		json
//	@Param			center_lat	query		number	true	"Center latitude"
//	@Param			center_lon	query		number	true	"Center longitude"
//	@Param			radius_m	query		number	false	"Optional radius; sets `inside`"
//	@Param			imei		query		string	false	"Test this truck's latest position (or use lat+lon)"
//	@Param			lat			query		number	false	"Test an arbitrary point (with lon)"
//	@Param			lon			query		number	false	"Test an arbitrary point (with lat)"
//	@Success		200			{object}	GeofenceResult
//	@Failure		400			{object}	map[string]string
//	@Router			/v1/geofence/check [get]
func (h *Handler) handleGeofenceCheckOne(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	centerLat, err1 := strconv.ParseFloat(q.Get("center_lat"), 64)
	centerLon, err2 := strconv.ParseFloat(q.Get("center_lon"), 64)
	if err1 != nil || err2 != nil || !validLatLon(centerLat, centerLon) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid center_lat and center_lon required"})
		return
	}
	var radius *float64
	if rs := q.Get("radius_m"); rs != "" {
		if v, err := strconv.ParseFloat(rs, 64); err == nil {
			radius = &v
		}
	}

	if imei := q.Get("imei"); imei != "" {
		if !validIMEI(imei) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid imei"})
			return
		}
		positions, err := h.r.LivePositions(r.Context(), []string{imei})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
		p, ok := positions[imei]
		writeJSON(w, http.StatusOK, evalCheck(imei, centerLat, centerLon, radius, p, ok))
		return
	}

	// arbitrary point mode
	lat, e1 := strconv.ParseFloat(q.Get("lat"), 64)
	lon, e2 := strconv.ParseFloat(q.Get("lon"), 64)
	if e1 != nil || e2 != nil || !validLatLon(lat, lon) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide imei, or a valid lat and lon"})
		return
	}
	writeJSON(w, http.StatusOK, evalCheck("", centerLat, centerLon, radius, GeoPos{Lat: lat, Lon: lon}, true))
}

func positionExists(m map[string]GeoPos, imei string) bool {
	_, ok := m[imei]
	return ok
}

// evalCheck builds a result from a resolved position (found=ok).
func evalCheck(imei string, centerLat, centerLon float64, radius *float64, pos GeoPos, found bool) GeofenceResult {
	res := GeofenceResult{IMEI: imei, Found: found}
	if !found {
		no := false
		res.Inside = &no
		return res
	}
	d := haversineMeters(pos.Lat, pos.Lon, centerLat, centerLon)
	lat, lon, t := pos.Lat, pos.Lon, pos.Time
	res.DistanceM = &d
	res.Lat, res.Lon = &lat, &lon
	if !t.IsZero() {
		res.Time = &t
	}
	if radius != nil {
		in := d <= *radius
		res.Inside = &in
	}
	return res
}
