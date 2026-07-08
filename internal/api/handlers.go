package api

import (
	"encoding/json"
	"net/http"
)

// health godoc
//
//	@Summary	Health check
//	@Tags		system
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Router		/healthz [get]
func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Vehicle is the API representation of a fleet vehicle.
type Vehicle struct {
	ID     int64  `json:"id"`
	Plate  string `json:"plate_no"`
	Make   string `json:"make,omitempty"`
	Model  string `json:"model,omitempty"`
	Status string `json:"status"`
}

// listVehicles godoc
//
//	@Summary	List vehicles
//	@Tags		vehicles
//	@Produce	json
//	@Success	200	{array}	Vehicle
//	@Router		/vehicles [get]
func listVehicles(w http.ResponseWriter, _ *http.Request) {
	// TODO(phase-4): query Postgres scoped to the caller's tenant (RLS).
	writeJSON(w, http.StatusOK, []Vehicle{})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
