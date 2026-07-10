package ingest

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
)

const maxBody = 1 << 20 // 1 MiB

// Handler serves the ingest endpoints.
type Handler struct {
	reg *Registry
	w   *Writer
}

func NewHandler(reg *Registry, w *Writer) *Handler {
	return &Handler{reg: reg, w: w}
}

func (h *Handler) Routes(r chi.Router) {
	r.Post("/v1/ingest/{vendor}", h.handleIngest)
}

// handleIngest godoc
//
//	@Summary	Ingest device telemetry (single reading or batch array)
//	@Description	Registered IMEIs are stored; unregistered ones are rejected. Partial batches store the registered readings and report the rest.
//	@Tags		ingest
//	@Accept		json
//	@Produce	json
//	@Param		vendor	path		string	true	"Vendor/adapter key (e.g. generic)"
//	@Param		payload	body		object	true	"A reading, or an array of readings"
//	@Success	202		{object}	map[string]any
//	@Failure	400		{object}	map[string]string
//	@Failure	401		{object}	map[string]any
//	@Failure	404		{object}	map[string]string
//	@Router		/v1/ingest/{vendor} [post]
func (h *Handler) handleIngest(w http.ResponseWriter, r *http.Request) {
	vendor := chi.URLParam(r, "vendor")
	adapter, ok := Adapters[vendor]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown vendor: " + vendor})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body"})
		return
	}

	events, err := adapter.Parse(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	// Registration gate: resolve each reading's IMEI against the in-memory
	// registry (no DB query). Unknown devices are dropped and reported.
	rows := make([]Row, 0, len(events))
	rejected := make([]string, 0)
	for _, ev := range events {
		entry, ok := h.reg.Lookup(ev.DeviceIdent)
		if !ok {
			rejected = append(rejected, ev.DeviceIdent)
			continue
		}
		rows = append(rows, Row{
			Event:     ev,
			TenantID:  entry.TenantID,
			DeviceID:  entry.DeviceID,
			VehicleID: entry.VehicleID,
		})
	}

	if len(rows) == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error":    "no registered devices in payload",
			"rejected": rejected,
		})
		return
	}

	if err := h.w.WriteBatch(r.Context(), rows); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write failed"})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted": len(rows),
		"rejected": rejected,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
