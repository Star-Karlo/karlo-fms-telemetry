package ingest

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/karlo/fms/internal/geocode"
)

const maxBody = 4 << 20 // 4 MiB (allows batches)

// Handler serves the ingest endpoints. It enriches each reading with its
// kecamatan/kota/provinsi in-process via the geocode service, then stores it.
type Handler struct {
	geo *geocode.Service
	w   *Writer
}

func NewHandler(geo *geocode.Service, w *Writer) *Handler {
	return &Handler{geo: geo, w: w}
}

func (h *Handler) Routes(r chi.Router) {
	r.Post("/v1/ingest/{vendor}", h.handleIngest)
}

// handleIngest godoc
//
//	@Summary	Ingest device telemetry (single reading or batch array)
//	@Description	Accepts all readings, reverse-geocodes each (kecamatan/kota/provinsi), and stores to the telemetry hypertable keyed by IMEI.
//	@Tags		ingest
//	@Accept		json
//	@Produce	json
//	@Param		vendor	path		string	true	"Vendor/adapter key (e.g. generic)"
//	@Param		payload	body		object	true	"A reading, or an array of readings"
//	@Success	202		{object}	map[string]any
//	@Failure	400		{object}	map[string]string
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

	readings, err := adapter.Parse(body)
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
		if res, gerr := h.geo.Reverse(ctx, rd.Lat, rd.Lon); gerr == nil && res.Found && res.Place != nil {
			row.Kecamatan = res.Place.Kecamatan
			row.Kabupaten = res.Place.Kabupaten
			row.Provinsi = res.Place.Provinsi
		}
		rows = append(rows, row)
	}

	if err := h.w.WriteBatch(ctx, rows); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write failed"})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{"stored": len(rows)})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
