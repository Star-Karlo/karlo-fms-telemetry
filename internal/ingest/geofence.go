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

// GeofenceCheck is one item in a batch geofence request. Identity is `imei`
// (GPS tracker), `driver` (mobile app), or BOTH — send both when you don't know
// which one is currently reporting and the freshest live fix wins.
type GeofenceCheck struct {
	IMEI      string   `json:"imei,omitempty" example:"350612071419004"`            // GPS tracker; its latest live position
	Driver    string   `json:"driver,omitempty" example:"649019ee6c7c280f4e46d0a2"` // mobile-app driver id; its latest live position
	CenterLat float64  `json:"center_lat" example:"-6.2"`                           // warehouse center latitude
	CenterLon float64  `json:"center_lon" example:"106.8"`                          // warehouse center longitude
	RadiusM   *float64 `json:"radius_m,omitempty" example:"8000"`                   // optional; if set, `inside` = distance_m <= radius_m
}

// GeofenceRequest is the batch body for POST /v1/geofence/check.
type GeofenceRequest struct {
	Checks []GeofenceCheck `json:"checks"`
}

// GeofenceResult is one check's answer. distance_m is the real output — the
// caller applies its own distance bands (arrive/out/enter) on it.
type GeofenceResult struct {
	IMEI      string     `json:"imei,omitempty"`       // echoed from the request
	Driver    string     `json:"driver,omitempty"`     // echoed from the request
	Source    string     `json:"source,omitempty"`     // which identity supplied the position: "gps" | "mobile"
	Found     bool       `json:"found"`                // false if neither identity has a live position
	DistanceM *float64   `json:"distance_m,omitempty"` // metres from the point to the center
	Inside    *bool      `json:"inside,omitempty"`     // present only when radius_m was given
	Lat       *float64   `json:"lat,omitempty"`        // the position used
	Lon       *float64   `json:"lon,omitempty"`
	Time      *time.Time `json:"time,omitempty"` // fix time of that position
}

// GeofenceResponse wraps the batch results (order matches the request).
type GeofenceResponse struct {
	Results []GeofenceResult `json:"results"`
}

// handleGeofenceCheckBatch godoc
//
//	@Summary		Batch radius check: are these trucks within their circles?
//	@Description	For each {imei and/or driver, center, radius}, returns the current distance to the center (and `inside` if radius given). Identity may be an `imei` (GPS tracker), a `driver` id (mobile app), or BOTH — send both when you don't know which is reporting and the freshest live fix wins; `source` tells you which was used. One request, one indexed lookup per identity type. The caller (platform) applies its own distance bands (arrive/out/enter) on distance_m. No live position → found:false.
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

	// Collect the two identity sets separately: each is one indexed lookup
	// against its own table (latest / mobile_latest).
	seenIMEI, seenDriver := map[string]bool{}, map[string]bool{}
	imeis := make([]string, 0, len(req.Checks))
	drivers := make([]string, 0, len(req.Checks))
	for i, c := range req.Checks {
		if c.IMEI == "" && c.Driver == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("check %d: imei or driver required", i)})
			return
		}
		if c.IMEI != "" && !validIMEI(c.IMEI) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("check %d: invalid imei", i)})
			return
		}
		if c.Driver != "" && !validIMEI(c.Driver) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("check %d: invalid driver", i)})
			return
		}
		if !validLatLon(c.CenterLat, c.CenterLon) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("check %d: center out of range", i)})
			return
		}
		if c.IMEI != "" && !seenIMEI[c.IMEI] {
			seenIMEI[c.IMEI] = true
			imeis = append(imeis, c.IMEI)
		}
		if c.Driver != "" && !seenDriver[c.Driver] {
			seenDriver[c.Driver] = true
			drivers = append(drivers, c.Driver)
		}
	}

	var gps, mob map[string]GeoPos
	if len(imeis) > 0 {
		var err error
		if gps, err = h.r.LivePositions(r.Context(), imeis); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
	}
	if len(drivers) > 0 {
		if h.mob == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "mobile store unavailable"})
			return
		}
		var err error
		if mob, err = h.mob.LivePositions(r.Context(), drivers); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
	}

	results := make([]GeofenceResult, 0, len(req.Checks))
	for _, c := range req.Checks {
		pos, source, found := resolveGeoPos(c.IMEI, c.Driver, gps, mob)
		results = append(results, evalCheck(c.IMEI, c.Driver, source, c.CenterLat, c.CenterLon, c.RadiusM, pos, found))
	}
	writeJSON(w, http.StatusOK, GeofenceResponse{Results: results})
}

// resolveGeoPos picks the position to test: whichever identity has a live fix,
// and when both do, the fresher one. Returns the source that supplied it.
func resolveGeoPos(imei, driver string, gps, mob map[string]GeoPos) (GeoPos, string, bool) {
	g, gok := GeoPos{}, false
	if imei != "" {
		g, gok = gps[imei]
	}
	m, mok := GeoPos{}, false
	if driver != "" {
		m, mok = mob[driver]
	}
	switch {
	case gok && mok:
		if m.Time.After(g.Time) {
			return m, "mobile", true
		}
		return g, "gps", true
	case gok:
		return g, "gps", true
	case mok:
		return m, "mobile", true
	}
	return GeoPos{}, "", false
}

// handleGeofenceCheckOne godoc
//
//	@Summary		Single radius check (convenience/testing)
//	@Description	Distance of one truck (imei), one mobile driver (driver), or an arbitrary point (lat/lon) to a center. Pass both imei and driver to use whichever has the fresher live fix. Returns the same fields as one batch result.
//	@Tags			geofence
//	@Produce		json
//	@Param			center_lat	query		number	true	"Center latitude"
//	@Param			center_lon	query		number	true	"Center longitude"
//	@Param			radius_m	query		number	false	"Optional radius; sets `inside`"
//	@Param			imei		query		string	false	"Test this truck's latest position (or use driver, or lat+lon)"
//	@Param			driver		query		string	false	"Test this mobile driver's latest position"
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

	imei, driver := q.Get("imei"), q.Get("driver")
	if imei != "" || driver != "" {
		if imei != "" && !validIMEI(imei) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid imei"})
			return
		}
		if driver != "" && !validIMEI(driver) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid driver"})
			return
		}
		var gps, mob map[string]GeoPos
		if imei != "" {
			var err error
			if gps, err = h.r.LivePositions(r.Context(), []string{imei}); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
				return
			}
		}
		if driver != "" {
			if h.mob == nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "mobile store unavailable"})
				return
			}
			var err error
			if mob, err = h.mob.LivePositions(r.Context(), []string{driver}); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
				return
			}
		}
		pos, source, found := resolveGeoPos(imei, driver, gps, mob)
		writeJSON(w, http.StatusOK, evalCheck(imei, driver, source, centerLat, centerLon, radius, pos, found))
		return
	}

	// arbitrary point mode
	lat, e1 := strconv.ParseFloat(q.Get("lat"), 64)
	lon, e2 := strconv.ParseFloat(q.Get("lon"), 64)
	if e1 != nil || e2 != nil || !validLatLon(lat, lon) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide imei or driver, or a valid lat and lon"})
		return
	}
	writeJSON(w, http.StatusOK, evalCheck("", "", "point", centerLat, centerLon, radius, GeoPos{Lat: lat, Lon: lon}, true))
}

// evalCheck builds a result from a resolved position (found=ok).
func evalCheck(imei, driver, source string, centerLat, centerLon float64, radius *float64, pos GeoPos, found bool) GeofenceResult {
	res := GeofenceResult{IMEI: imei, Driver: driver, Source: source, Found: found}
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
