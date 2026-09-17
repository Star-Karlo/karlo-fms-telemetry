package ingest

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"

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
	// serviceTokens: the platform's service-to-service tokens (secret
	// karlo-prod/service-tokens, key "all"). A peer service inside the VPC —
	// TMS business, fms-api — reads telemetry with `Authorization: Bearer
	// <its token>` instead of a shared ingest key, the same credential it
	// uses on every other internal call. Reads only: devices still write
	// with an ingest key.
	serviceTokens map[string]struct{}
}

func NewHandler(geo *geocode.Service, w *Writer, reader *Reader, mob *MobileStore, green *GreenStore, apiKey string, keys *KeyStore) *Handler {
	return &Handler{geo: geo, w: w, r: reader, mob: mob, green: green, apiKey: apiKey, keys: keys, serviceTokens: map[string]struct{}{}}
}

// WithServiceTokens accepts these bearer tokens on the read endpoints.
func (h *Handler) WithServiceTokens(tokens []string) *Handler {
	for _, t := range tokens {
		if t = strings.TrimSpace(t); t != "" {
			h.serviceTokens[t] = struct{}{}
		}
	}
	return h
}

// AuthorizedRead reports whether a request may READ telemetry: an ingest key
// (devices, legacy callers) or a platform service token as a bearer.
func (h *Handler) AuthorizedRead(r *http.Request) bool {
	if h.authorized(r.Context(), r.Header.Get("X-Ingest-Key")) {
		return true
	}
	if tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		_, ok := h.serviceTokens[strings.TrimSpace(tok)]
		return ok
	}
	return false
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
	// write — each handler checks the key itself, because a device that
	// presents a per-device key is authorised differently from a service
	// presenting the shared one.
	// Bodies are bounded: the largest legitimate batch (a tracker flushing a
	// day's buffer) is a few hundred KB. Anything larger is a mistake or an
	// attack, and MaxBytesReader turns it into a 413 instead of a memory spike.
	r.With(bodyLimit(4<<20)).Post("/v1/ingest", h.handleIngest)
	r.With(bodyLimit(4<<20)).Post("/v1/ingest/mobile", h.handleMobileIngest)

	// READS REQUIRE THE KEY TOO.
	//
	// They did not, until 2026-09-15. The key guarded writes only, and every
	// read below answered anyone: GET /v1/live with no header returned 2,180
	// live positions across every customer's fleet, with street-level
	// addresses. Found while smoke-testing the Jakarta stack, present in
	// Singapore all along.
	//
	// Every legitimate reader already sends X-Ingest-Key — fms-backend's
	// telemetry client, business-service's — so this locks out only the
	// public. The browser never calls this service; it goes through fms-api.
	r.Group(func(pr chi.Router) {
		pr.Use(h.requireKey)
		if h.green != nil {
			pr.Get("/v1/driving-events", h.green.handleEvents)
		}
		pr.Get("/v1/live", h.handleLiveAll)
		pr.Get("/v1/live/{imei}", h.handleLiveOne)
		pr.Get("/v1/history/{imei}", h.handleHistory)
		pr.Post("/v1/geofence/check", h.handleGeofenceCheckBatch)
		pr.Get("/v1/geofence/check", h.handleGeofenceCheckOne)
		pr.Get("/v1/mobile/live", h.handleMobileLiveAll)
		pr.Get("/v1/mobile/live/{driver}", h.handleMobileLiveOne)
		pr.Get("/v1/mobile/history/{driver}", h.handleMobileHistory)
	})
}

// requireKey refuses a request that does not present a valid X-Ingest-Key,
// using the same check the write path applies.
func (h *Handler) requireKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.AuthorizedRead(r) {
			http.Error(w, `{"error":"missing or invalid X-Ingest-Key or service token"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
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
	stored, err := WriteReadings(ctx, h.geo, h.w, readings)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write failed"})
		return
	}

	// Harvest any Teltonika Green Driving events riding in the same batch. Runs
	// after the telemetry is safely written and never affects the ingest result.
	if h.green != nil {
		h.green.Extract(ctx, readings)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{"stored": stored})
}

// WriteReadings reverse-geocodes each reading (best-effort, cached) and persists
// the batch. Shared by the push ingest handler and pull pollers (e.g. the GMT
// vehicle-status poller) so every source stores an identically-shaped row. A
// geocode failure never fails the write — the row just stores NULL location.
func WriteReadings(ctx context.Context, geo *geocode.Service, w *Writer, readings []Reading) (int, error) {
	rows := make([]Row, 0, len(readings))
	for _, rd := range readings {
		row := Row{Reading: rd}
		if res, gerr := geo.Reverse(ctx, rd.Lat, rd.Lon); gerr == nil && res.Found {
			row.Kecamatan = res.Kecamatan
			row.Kelurahan = res.Kelurahan
			row.Kabupaten = res.Kabupaten
			row.Kota = res.Kota
			row.Provinsi = res.Provinsi
			row.Jalan = res.Jalan
		}
		rows = append(rows, row)
	}
	if err := w.WriteBatch(ctx, rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// bodyLimit caps a request body; the JSON decoder then fails cleanly past it.
func bodyLimit(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}
