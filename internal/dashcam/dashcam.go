// Package dashcam is the telemetry service's VSS front. FMS calls these
// endpoints; this package calls VSS. FMS never touches VSS directly.
//
// It does the three things VSS is needed for on demand:
//   - list a device's clips for a date (annotated with the coordinate the
//     dashcam's own GPS reported at that moment)
//   - pull a chosen clip onto S3 for durable keeping (accident evidence)
//   - mint a short-lived live/playback URL for the browser to iframe
//
// Continuous event ingestion (DMS/ADAS/collision over the VSS WebSocket) is a
// separate concern handled alongside GPS ingest; see events.go.
//
// Endpoints are API-key gated with the SAME key as /v1/ingest and are
// tenant-agnostic — this is the raw layer. FMS holds the device<->vehicle link
// and the tenant scoping.
package dashcam

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/karlo/fms/internal/archive"
	"github.com/karlo/fms/internal/vss"
)

// AuthFunc reports whether a presented X-Ingest-Key is valid. Passed in rather
// than reimplemented so there is one auth path in the service.
type AuthFunc func(ctx context.Context, presentedKey string) bool

// Handler serves the dashcam endpoints.
type Handler struct {
	vss    *vss.Client
	auth   AuthFunc
	bucket string
	jobs   *jobStore
	client *http.Client
	pool   *pgxpool.Pool
	tracks *trackCache
}

// NewHandler builds the handler. bucket is where downloaded clips are kept
// (karlo-fms-cold-storage); videos land under the "video/" prefix, beside the
// telemetry parquet.
func NewHandler(client *vss.Client, auth AuthFunc, bucket string, pool *pgxpool.Pool) *Handler {
	return &Handler{
		vss: client, auth: auth, bucket: bucket, pool: pool,
		jobs:   newJobStore(),
		client: &http.Client{Timeout: 10 * time.Minute}, // clip pulls are large + slow
		tracks: newTrackCache(client),
	}
}

// Routes mounts the dashcam endpoints.
func (h *Handler) Routes(r chi.Router) {
	r.Get("/v1/dashcams", h.gated(h.list))
	r.Get("/v1/dashcams/{deviceId}/videos", h.gated(h.videos))
	r.Get("/v1/dashcams/{deviceId}/track", h.gated(h.track))
	r.Get("/v1/dashcams/{deviceId}/live", h.gated(h.live))
	r.Post("/v1/dashcams/{deviceId}/download", h.gated(h.startDownload))
	r.Get("/v1/dashcams/downloads/{jobId}", h.gated(h.downloadStatus))
	r.Get("/v1/dashcams/alarms", h.gated(h.alarms))
}

func (h *Handler) gated(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.auth(r.Context(), r.Header.Get("X-Ingest-Key")) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing X-Ingest-Key"})
			return
		}
		next(w, r)
	}
}

// fail maps a VSS failure to the right HTTP status: not-configured -> 503,
// device-offline -> 200 with an offline flag (expected), everything else -> 502.
func (h *Handler) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, vss.ErrNotConfigured) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "camera platform not configured (VSS_BASE_URL/USERNAME/PASSWORD)"})
		return
	}
	var apiErr *vss.APIError
	if errors.As(err, &apiErr) && apiErr.Offline() {
		writeJSON(w, http.StatusOK, map[string]any{"offline": true})
		return
	}
	log.Printf("dashcam: %v", err)
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": "camera platform unreachable"})
}

// list godoc
//
//	@Summary		List dashcam devices and their online state
//	@Description	Fronts the VSS device list. Tenant-agnostic; FMS maps devices to vehicles and scopes by tenant.
//	@Tags			dashcams
//	@Produce		json
//	@Param			X-Ingest-Key	header	string	true	"ingest API key"
//	@Param			q				query	string	false	"filter by device id/name"
//	@Success		200	{array}		map[string]interface{}
//	@Failure		503	{object}	map[string]string
//	@Router			/v1/dashcams [get]
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	devices, err := h.vss.ListDevices(r.Context(), strings.TrimSpace(r.URL.Query().Get("q")))
	if err != nil {
		h.fail(w, err)
		return
	}
	type dev struct {
		DeviceID     string   `json:"device_id"`
		DeviceName   string   `json:"device_name"`
		DeviceType   string   `json:"device_type"`
		Channels     int      `json:"channels"`
		ChannelNames string   `json:"channel_names"`
		Online       bool     `json:"online"`
		AccessMode   int      `json:"access_mode"`
		Lat          *float64 `json:"lat"`
		Lon          *float64 `json:"lon"`
	}
	out := make([]dev, 0, len(devices))
	for _, d := range devices {
		out = append(out, dev{
			DeviceID: d.DeviceNo, DeviceName: d.DeviceName, DeviceType: d.DeviceType,
			Channels: d.VideoChannels, ChannelNames: d.ChannelName,
			Online: d.Online(), AccessMode: d.AccessMode,
			Lat: coordToFloat(d.Latitude), Lon: coordToFloat(d.Longitude),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// Clip is one SD-card clip, annotated with the coordinate derived from the
// device's GPS track at the clip's start.
type Clip struct {
	Channel  int      `json:"channel"`
	Start    string   `json:"start"`
	Stop     string   `json:"stop"`
	Duration int      `json:"duration"`
	Size     int64    `json:"size"`
	Path     string   `json:"path"`
	Lat      *float64 `json:"lat"`
	Lon      *float64 `json:"lon"`
}

// videos godoc
//
//	@Summary		List a device's clips for a date, with coordinates
//	@Description	Clips on the device SD card for the given day. Each clip is annotated with the position the dashcam's own GPS reported at the clip's start (derived — the vendor's clip list carries no coordinate).
//	@Tags			dashcams
//	@Produce		json
//	@Param			X-Ingest-Key	header	string	true	"ingest API key"
//	@Param			deviceId		path	string	true	"VSS device id"
//	@Param			date			query	string	true	"YYYY-MM-DD"
//	@Param			channel			query	string	false	"channel list e.g. 1_2 (default all)"
//	@Success		200	{array}		dashcam.Clip
//	@Failure		400	{object}	map[string]string
//	@Router			/v1/dashcams/{deviceId}/videos [get]
func (h *Handler) videos(w http.ResponseWriter, r *http.Request) {
	deviceID := chi.URLParam(r, "deviceId")
	day, err := time.Parse("2006-01-02", r.URL.Query().Get("date"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "date must be YYYY-MM-DD"})
		return
	}
	from, to := day, day.Add(24*time.Hour)
	channels := parseChannels(r.URL.Query().Get("channel"))

	files, err := h.vss.SearchVideos(r.Context(), deviceID, channels, from, to)
	if err != nil {
		h.fail(w, err)
		return
	}
	// The dashcam's own GPS for the day, once — then each clip's coordinate is
	// the nearest fix to its start. A failed track lookup is not fatal: clips
	// list with null coordinates rather than not at all.
	track, terr := h.vss.Track(r.Context(), deviceID, from, to)
	if terr != nil {
		log.Printf("dashcam: track for %s: %v (coordinates omitted)", deviceID, terr)
	}

	out := make([]Clip, 0, len(files))
	for _, f := range files {
		c := Clip{Channel: f.Channel, Start: f.Start, Stop: f.Stop, Duration: f.Duration, Size: f.Size, Path: f.Path}
		if st, e := time.Parse("2006-01-02 15:04:05", f.Start); e == nil {
			if fix, ok := vss.NearestFix(track, st); ok {
				lat, lon := fix.Lat, fix.Lon
				c.Lat, c.Lon = &lat, &lon
			}
		}
		out = append(out, c)
	}
	writeJSON(w, http.StatusOK, out)
}

// live godoc
//
//	@Summary		Embeddable live-view URL
//	@Tags			dashcams
//	@Produce		json
//	@Param			X-Ingest-Key	header	string	true	"ingest API key"
//	@Param			deviceId		path	string	true	"VSS device id"
//	@Param			channel			query	string	false	"channel list e.g. 1_2"
//	@Param			stream			query	string	false	"main|sub (default sub)"
//	@Success		200	{object}	map[string]string
//	@Router			/v1/dashcams/{deviceId}/live [get]
func (h *Handler) live(w http.ResponseWriter, r *http.Request) {
	deviceID := chi.URLParam(r, "deviceId")
	u, err := h.vss.LiveURL(r.Context(), deviceID, parseChannels(r.URL.Query().Get("channel")),
		r.URL.Query().Get("stream") == "main")
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": u})
}

// DownloadRequest asks for one clip to be pulled to S3.
type DownloadRequest struct {
	Channel int    `json:"channel"`
	Path    string `json:"path"`  // the clip's device path, from a prior /videos call
	Start   string `json:"start"` // "2006-01-02 15:04:05"
	Stop    string `json:"stop"`
}

// startDownload godoc
//
//	@Summary		Pull a clip to S3 (async)
//	@Description	Starts a VSS download, fetches the file, and stores it under karlo-fms-cold-storage/video/. Returns a job id to poll; the finished job carries the S3 key FMS records.
//	@Tags			dashcams
//	@Accept			json
//	@Produce		json
//	@Param			X-Ingest-Key	header	string					true	"ingest API key"
//	@Param			deviceId		path	string					true	"VSS device id"
//	@Param			body			body	dashcam.DownloadRequest	true	"clip to pull"
//	@Success		202	{object}	map[string]string
//	@Router			/v1/dashcams/{deviceId}/download [post]
func (h *Handler) startDownload(w http.ResponseWriter, r *http.Request) {
	deviceID := chi.URLParam(r, "deviceId")
	var req DownloadRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || req.Start == "" || req.Stop == "" {
		// Downloads are by TIME RANGE now (req.Path is unused), so the window is
		// what's required — a clip is defined by its start/stop, whether it came
		// from the SD-card file list or an alarm's timestamp.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "start and stop are required"})
		return
	}
	const layout = "2006-01-02 15:04:05"
	from, err1 := time.Parse(layout, req.Start)
	to, err2 := time.Parse(layout, req.Stop)
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "start and stop must be 'YYYY-MM-DD HH:MM:SS'"})
		return
	}

	job := h.jobs.create()
	// Detached from the request: the pull can take minutes, longer than any
	// client will hold a connection. FMS polls the job instead.
	go h.runDownload(context.Background(), job, deviceID, req, from, to)
	writeJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}

func (h *Handler) runDownload(ctx context.Context, job *job, deviceID string, req DownloadRequest, from, to time.Time) {
	h.jobs.set(job.ID, "downloading", "", "")

	// FAST PATH — an alarm's evidence clip is already on the VSS server: when a
	// DMS/ADAS alarm fires the dashcam auto-uploads a short clip, so for an
	// alarm-based save we just copy that (a few MB, already uploaded) instead of
	// pulling the whole ~156 MB SD-card segment over the truck's 4G, which stalls
	// on a weak link. Falls through to the SD-card path if there's no such clip.
	if url, ok := h.evidenceClipURL(ctx, deviceID, from, to, req.Channel); ok {
		h.jobs.set(job.ID, "uploading", "", "")
		key := videoKey(deviceID, req.Channel, from)
		if err := h.pullToS3(ctx, url, key); err != nil {
			log.Printf("dashcam: evidence clip fetch for %s: %v (falling back to SD card)", deviceID, err)
		} else {
			h.jobs.set(job.ID, "done", key, "")
			return
		}
	}

	// Download by TIME RANGE, not by whole file. A single SD-card recording file
	// is ~156 MB regardless of how short the chosen clip is, and pulling all of it
	// over 4G is what stalled saves at ~90%. Passing an empty resDeviceFileName
	// makes VSS extract only the selected window (a few MB for a short clip), so
	// there is far less to transfer and far less to drop. req.Path is left unused.
	task, err := h.vss.StartDownload(ctx, deviceID, req.Channel, from, to, "")
	if err != nil {
		h.jobs.set(job.ID, "failed", "", err.Error())
		return
	}
	// Poll VSS until the file is ready on its server, RESUMING the transfer when
	// the device drops it part-way. Over cellular an abnormal stop at ~90% is
	// common; the vendor's guidance (§3.15.2) is to continue the task, not start
	// over — restarting just hits the same blip and is why a plain "Retry" kept
	// failing. A bounded resume count stops a hopeless link from looping forever.
	deadline := time.Now().Add(20 * time.Minute)
	const maxResumes = 15
	resumes := 0
	for {
		if time.Now().After(deadline) {
			h.jobs.set(job.ID, "failed", "", "timed out preparing the clip — retry when the dashcam has stronger signal")
			return
		}
		time.Sleep(4 * time.Second)
		p, perr := h.vss.DownloadStatus(ctx, task.TaskID, task.ResWebPath)
		if perr != nil {
			h.jobs.set(job.ID, "failed", "", perr.Error())
			return
		}
		if p.Done() {
			break
		}
		if p.Terminal() { // -7 no data / -8 offline: resuming can't help
			h.jobs.set(job.ID, "failed", "", p.Reason())
			return
		}
		if p.Resumable() { // -1 paused / -9 abnormal stop: continue from where it stopped
			if resumes >= maxResumes {
				h.jobs.set(job.ID, "failed", "", fmt.Sprintf("the dashcam kept dropping the transfer (stopped at %s%%) — retry when it has stronger signal", p.Progress))
				return
			}
			resumes++
			if rerr := h.vss.ResumeDownload(ctx, task.TaskID, task.ResWebPath); rerr != nil {
				log.Printf("dashcam: resume job %s: %v", job.ID, rerr)
			}
			// Give a fluctuating cellular link a moment to recover before the next
			// poll, rather than hammering resume every 4s.
			time.Sleep(6 * time.Second)
		}
		// otherwise 0/1 (not started / downloading) — keep waiting
	}

	// Fetch the finished file from the VSS file server, then push to S3. A temp
	// file rather than buffering in memory — a clip is tens of MB.
	h.jobs.set(job.ID, "uploading", "", "")
	key := videoKey(deviceID, req.Channel, from)
	// The insert's resWebPath is not directly servable; ask VSS for the real,
	// token-bearing download URL for this finished task.
	fetchURL, err := h.vss.DownloadURL(ctx, deviceID, task.TaskID, task.ResWebPath)
	if err != nil {
		h.jobs.set(job.ID, "failed", "", err.Error())
		return
	}
	if err := h.pullToS3(ctx, fetchURL, key); err != nil {
		h.jobs.set(job.ID, "failed", "", err.Error())
		return
	}
	h.jobs.set(job.ID, "done", key, "")
}

// TrackPoint is one GPS fix from the dashcam's own GPS, in the history shape.
type TrackPoint struct {
	Lat  float64   `json:"lat"`
	Lon  float64   `json:"lon"`
	Time time.Time `json:"time"`
}

// track godoc
//
//	@Summary		A dashcam's own GPS track over a range
//	@Description	The device's GPS fixes, for a vehicle that has a camera but no
//	@Description	tracker — so its trip history comes from the dashcam.
//	@Tags			dashcams
//	@Produce		json
//	@Param			deviceId	path	string	true	"vss device id"
//	@Param			from		query	string	true	"RFC3339"
//	@Param			to			query	string	true	"RFC3339"
//	@Success		200	{array}	dashcam.TrackPoint
//	@Router			/v1/dashcams/{deviceId}/track [get]
func (h *Handler) track(w http.ResponseWriter, r *http.Request) {
	deviceID := chi.URLParam(r, "deviceId")
	from, err1 := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, err2 := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from and to must be RFC3339"})
		return
	}
	// Served from a per-day cache so repeatedly changing the date range doesn't
	// re-hit VSS's throttled track API (the intermittent 502 the operators saw).
	fixes, err := h.tracks.rangeFixes(r.Context(), deviceID, from.In(wib), to.In(wib))
	if err != nil {
		h.fail(w, err)
		return
	}
	out := make([]TrackPoint, 0, len(fixes))
	for _, f := range fixes {
		out = append(out, TrackPoint{Lat: f.Lat, Lon: f.Lon, Time: f.T})
	}
	writeJSON(w, http.StatusOK, out)
}

// evidenceClipURL returns the ready-to-fetch URL of an alarm's evidence VIDEO in
// [from,to] — the short clip the dashcam auto-uploaded to the VSS server when the
// alarm fired — preferring the requested channel. false when the window has no
// such clip (so the caller pulls from the SD card instead).
func (h *Handler) evidenceClipURL(ctx context.Context, deviceID string, from, to time.Time, channel int) (string, bool) {
	evs, err := h.vss.SearchAlarmEvidence(ctx, deviceID, from, to, "")
	if err != nil {
		return "", false
	}
	const layout = "2006-01-02 15:04:05"
	lo, hi := from.Add(-30*time.Second), to.Add(30*time.Second) // small slack
	var fallback string
	for _, e := range evs {
		for _, f := range e.Files {
			if f.DownURL == "" || (f.FileType != 1 && f.FileType != 2) {
				continue // videos only (1 ordinary, 2 alarm); skip pictures
			}
			if t, perr := time.Parse(layout, f.StartTime); perr == nil {
				if t.Before(lo) || t.After(hi) {
					continue
				}
			}
			if channel == 0 || f.Channel == 0 || f.Channel == channel {
				return f.DownURL, true
			}
			if fallback == "" {
				fallback = f.DownURL
			}
		}
	}
	return fallback, fallback != ""
}

// pullToS3 streams the VSS file to a temp file and uploads it. Video kept as
// S3 Standard, not Glacier: accident evidence is retrieved interactively and
// must be instantly available, unlike the archival parquet.
func (h *Handler) pullToS3(ctx context.Context, fileURL, key string) error {
	// VSS reports the task "done" a beat before the extracted clip is actually
	// readable on its file server, so a 404/5xx right after completion is usually
	// "not written yet" — retry a few times before giving up.
	resp, err := h.fetchClip(ctx, fileURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	tmp, err := os.CreateTemp("", "dashcam-*.mp4")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return fmt.Errorf("buffer clip: %w", err)
	}
	tmp.Close()
	return archive.UploadFileAs(ctx, h.bucket, key, tmp.Name(), types.StorageClassStandard, "video/mp4")
}

// fetchClip GETs the finished clip, retrying while the VSS file server reports it
// missing (404/5xx) — the extracted file lands a moment after the task is "done".
func (h *Handler) fetchClip(ctx context.Context, fileURL string) (*http.Response, error) {
	const attempts = 6
	var lastCode int
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(3 * time.Second):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := h.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch clip: %w", err)
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		resp.Body.Close()
		lastCode = resp.StatusCode
		// 404 (not written yet) and 5xx (server busy finalising) are worth waiting
		// on; anything else is a hard error.
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode < 500 {
			break
		}
	}
	log.Printf("dashcam: clip not fetchable after %d tries (last HTTP %d): %s", attempts, lastCode, fileURL)
	return nil, fmt.Errorf("fetch clip: HTTP %d", lastCode)
}

// downloadStatus godoc
//
//	@Summary	Poll a download job
//	@Tags		dashcams
//	@Produce	json
//	@Param		X-Ingest-Key	header	string	true	"ingest API key"
//	@Param		jobId			path	string	true	"job id"
//	@Success	200	{object}	map[string]string
//	@Failure	404	{object}	map[string]string
//	@Router		/v1/dashcams/downloads/{jobId} [get]
func (h *Handler) downloadStatus(w http.ResponseWriter, r *http.Request) {
	j, ok := h.jobs.get(chi.URLParam(r, "jobId"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	resp := map[string]string{"status": j.Status}
	if j.S3Key != "" {
		resp["s3_key"] = j.S3Key
		resp["bucket"] = h.bucket
	}
	if j.Err != "" {
		resp["error"] = j.Err
	}
	writeJSON(w, http.StatusOK, resp)
}

// videoKey is the S3 object key for a clip, partitioned by device and day so
// the bucket stays browsable and lifecycle rules can target old video.
func videoKey(deviceID string, channel int, at time.Time) string {
	return fmt.Sprintf("video/%s/%s/ch%d_%s.mp4",
		deviceID, at.Format("2006-01-02"), channel, at.Format("150405"))
}

func parseChannels(q string) []int {
	if q == "" {
		return nil
	}
	out := []int{}
	for _, p := range strings.Split(q, "_") {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// coordToFloat coerces a VSS device coordinate (sent as a string or number) to a
// float pointer. VSS reports "0"/0 when it has no fix; those and unparseable
// values become nil so a device with no GPS is not plotted at null island.
func coordToFloat(v any) *float64 {
	var f float64
	switch t := v.(type) {
	case float64:
		f = t
	case string:
		p, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil {
			return nil
		}
		f = p
	default:
		return nil
	}
	if f == 0 {
		return nil
	}
	return &f
}
