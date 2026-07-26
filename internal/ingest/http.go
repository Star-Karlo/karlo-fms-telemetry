package ingest

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/karlo/fms/internal/geocode"
)

const maxBody = 4 << 20 // 4 MiB (allows batches)

// Handler serves the telemetry endpoints: write (ingest) and read (live +
// historical). Ingest enriches each reading with its kecamatan/kota/provinsi
// in-process via the geocode service, then stores it.
type Handler struct {
	geo    *geocode.Service
	w      *Writer
	r      *Reader
	mob    *MobileStore // driver-keyed telemetry (phone app); see mobile.go
	green  *GreenStore  // tracker Green Driving events; see green.go
	apiKey string       // legacy shared secret; still accepted when set
	keys   *KeyStore    // revocable per-sender keys (ingest_key table)
}

func NewHandler(geo *geocode.Service, w *Writer, reader *Reader, mob *MobileStore, green *GreenStore, apiKey string, keys *KeyStore) *Handler {
	return &Handler{geo: geo, w: w, r: reader, mob: mob, green: green, apiKey: apiKey, keys: keys}
}

// authorized reports whether the request may write telemetry. A request passes
// with either the legacy shared secret or any active ingest_key row, so issuing
// and revoking individual keys never disturbs existing senders.
//
// With neither configured the endpoint stays open, preserving the historical
// behaviour for local/dev runs (cmd/ingest logs a warning in that state).
// Authorized exposes the same key check to other handlers in the service (e.g.
// dashcam) so there is one auth path, not two that can drift.
func (h *Handler) Authorized(ctx context.Context, presented string) bool {
	return h.authorized(ctx, presented)
}

func (h *Handler) authorized(ctx context.Context, presented string) bool {
	if h.apiKey == "" && h.keys == nil {
		return true
	}
	if presented == "" {
		return false
	}
	// Constant-time: the shared secret is compared byte-for-byte on every call.
	if h.apiKey != "" && subtle.ConstantTimeCompare([]byte(presented), []byte(h.apiKey)) == 1 {
		return true
	}
	if h.keys != nil {
		return h.keys.Valid(ctx, presented)
	}
	return false
}

func (h *Handler) Routes(r chi.Router) {
	// write
	r.Post("/v1/ingest", h.handleIngest)
	// driving-behaviour events (Teltonika Green Driving), for FMS to poll
	if h.green != nil {
		r.Get("/v1/driving-events", h.green.handleEvents)
	}
	// read
	r.Get("/v1/live", h.handleLiveAll)
	r.Get("/v1/live/{imei}", h.handleLiveOne)
	r.Get("/v1/history/{imei}", h.handleHistory)
	// geofence (radius check)
	r.Post("/v1/geofence/check", h.handleGeofenceCheckBatch)
	r.Get("/v1/geofence/check", h.handleGeofenceCheckOne)
	// mobile (driver-keyed telemetry from the phone app)
	r.Post("/v1/ingest/mobile", h.handleMobileIngest)
	r.Get("/v1/mobile/live", h.handleMobileLiveAll)
	r.Get("/v1/mobile/live/{driver}", h.handleMobileLiveOne)
	r.Get("/v1/mobile/history/{driver}", h.handleMobileHistory)
}

// IngestResult is the response for a successful ingest.
type IngestResult struct {
	Stored int `json:"stored" example:"1"` // number of readings written
}

// handleIngest godoc
//
//	@Summary		Ingest device telemetry (single reading or batch array)
//	@Description	Accepts all readings, reverse-geocodes each to kecamatan/kota/provinsi, and stores them to the telemetry hypertable keyed by IMEI. Send one GenericReading object or a JSON array of them. Fields map to telemetry columns except those noted "-> metadata" (stored in the metadata jsonb).
//	@Tags			ingest
//	@Accept			json
//	@Produce		json
//	@Param			X-Ingest-Key	header		string			false	"Shared ingest API key (required when the service is configured with one)"
//	@Param			payload	body		GenericReading	true	"A reading (or a JSON array of readings)"
//	@Success		202		{object}	IngestResult
//	@Failure		400		{object}	map[string]string
//	@Failure		401		{object}	map[string]string
//	@Router			/v1/ingest [post]
func (h *Handler) handleIngest(w http.ResponseWriter, r *http.Request) {
	// API-key gate: accepts the shared secret or any active ingest_key.
	if !h.authorized(r.Context(), r.Header.Get("X-Ingest-Key")) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing X-Ingest-Key"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body"})
		return
	}

	readings, err := defaultAdapter.Parse(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	ctx := r.Context()
	rows := make([]Row, 0, len(readings))
	for _, rd := range readings {
		row := Row{Reading: rd}
		// Best-effort city enrichment (in-process, cached). A geocode failure
		// must never fail an ingest — the row just stores NULL location.
		if res, gerr := h.geo.Reverse(ctx, rd.Lat, rd.Lon); gerr == nil && res.Found {
			row.Kecamatan = res.Kecamatan
			row.Kelurahan = res.Kelurahan // village (ADM4)
			row.Kabupaten = res.Kabupaten // regency (empty for cities)
			row.Kota = res.Kota           // city (empty for regencies)
			row.Provinsi = res.Provinsi
			row.Jalan = res.Jalan // nearest street
		}
		rows = append(rows, row)
	}

	if err := h.w.WriteBatch(ctx, rows); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write failed"})
		return
	}

	// Harvest any Teltonika Green Driving events riding in the same batch. Runs
	// after the telemetry is safely written and never affects the ingest result.
	if h.green != nil {
		h.green.Extract(ctx, readings)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{"stored": len(rows)})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
