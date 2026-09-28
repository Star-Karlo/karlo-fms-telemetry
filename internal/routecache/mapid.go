package routecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// MAPID is the upstream. Every call through it costs money, which is the
// whole reason this service exists: nothing here is called unless the cache
// missed.
//
// THE KEY IS PUBLIC BY CONSTRUCTION. Nathanael decided (2026-09-28) that the
// service uses the same MAPID key the console ships to browsers as its
// basemap key, rather than a separate server-side one. It is held in SSM and
// never logged here, but it is readable in any page's source, so it is not a
// secret and must not be treated as one. The practical consequence is for
// whoever investigates a bill: a jump in MAPID usage is somebody else
// spending the quota at least as plausibly as a caller of this service, so
// check the page before you go hunting for a cache miss. Splitting it into a
// referrer-restricted public key and a private routing key remains possible
// later and changes nothing here but the value.
type MAPID struct {
	http    *http.Client
	baseURL string
	key     string
}

const (
	// A 350 km route with full geometry takes seconds, not milliseconds.
	// TMS ran 10s first and saw spurious failures on real lanes.
	mapidTimeout = 60 * time.Second
	// The body cap: a long route's geometry is large but never this large.
	mapidMaxBody = 8 << 20
)

func NewMAPID(baseURL, key string) *MAPID {
	if baseURL == "" {
		baseURL = "https://routing.mapid.io"
	}
	return &MAPID{
		http:    &http.Client{Timeout: mapidTimeout},
		baseURL: strings.TrimRight(baseURL, "/"),
		key:     key,
	}
}

func (m *MAPID) Configured() bool { return m != nil && m.key != "" }

// Route asks for a path between the points.
//
// THE BARE ENDPOINT IS DELIBERATE — do not "upgrade" it to /v4/route.
// Measured against MAPID on 2026-09-27 with one identical body:
//
//	POST /          200, paths[0] distance=148347.263 time=7629920
//	POST /v3/route  200, identical figures (plus a `hints` block)
//	POST /v4/route  400
//
// So the bare path already IS v3's contract, and v4 is not a newer version
// of it: v4 takes Karlo's own location IDs registered with MAPID
// ({IDLokasiMuat, IDLokasiBongkar}) rather than coordinates. That makes it
// uncacheable here — its inputs are opaque ids, not points, so two callers
// asking for the same journey would not share an entry — and unusable by
// anything that does not hold those ids. A product that wants v4 keeps its
// own attempt and falls back to this service.
//
// points_encoded stays FALSE: nothing in the estate decodes a polyline
// (there is no decoder in any repo), and GeoJSON is the richer form. It is
// therefore not part of the cache key either — one entry per lane.
func (m *MAPID) Route(ctx context.Context, r Request) ([]byte, error) {
	body := map[string]any{
		"profile":        r.Profile,
		"points":         r.Points,
		"points_encoded": false,
		// Measured 2026-09-28: turn-by-turn is ~10% of the answer and
		// nothing in the estate reads it, so it is off unless asked for.
		// NOTE the spelling — MAPID's own documentation writes
		// "insctructions", which the API ignores, so a body copied from it
		// silently keeps the instructions it meant to drop.
		"instructions": r.Instructions,
		"elevation":    r.Elevation,
	}
	if len(r.SnapPreventions) > 0 {
		body["snap_preventions"] = r.SnapPreventions
	}
	// Alternatives are honoured only for a two-point journey; with a
	// waypoint MAPID quietly returns one path (the caller is refused
	// upstream of here rather than being given a silent single route).
	if r.Alternatives > 1 {
		body["algorithm"] = "alternative_route"
		body["alternative_route"] = true
		body["alternative_route.max_paths"] = r.Alternatives
	}
	return m.post(ctx, "/", body)
}

// RouteByIDs asks /v4/route, which takes Karlo's own location ids rather
// than coordinates. MAPID may know a yard or gate centroid for an id that a
// dropped pin does not hit, so the answer can legitimately differ from the
// coordinate route for the same pair — that is the reason a caller would
// use it, and the reason it is a separate cache entry rather than an alias.
//
// A pair MAPID does not recognise is a 400, and a 400 is never cached: the
// caller falls back to coordinates exactly as it does today.
func (m *MAPID) RouteByIDs(ctx context.Context, ids []string, pointsEncoded bool) ([]byte, error) {
	if len(ids) != 2 {
		return nil, fmt.Errorf("v4 routing takes exactly two location ids, got %d", len(ids))
	}
	body := map[string]any{
		"IDLokasiMuat":    ids[0],
		"IDLokasiBongkar": ids[1],
		"points_encoded":  pointsEncoded,
	}
	return m.post(ctx, "/v4/route", body)
}

// RouteToll asks for a path priced by toll gate.
//
// /v2/route_n_toll, which is what the old backend calls. The /rute variant
// TMS uses rejects this body — measured against MAPID on 2026-09-28:
//
//	POST /v2/route_n_toll/rute  400 "Cannot read properties of undefined (reading 'coordinates')"
//	POST /v2/route_n_toll       200 {route, prices, details, distance, eta, bbox}
//
// so /rute prices an ALREADY COMPUTED route (it wants coordinates handed to
// it) while this one routes and prices in a single call, which is what a
// caller asking "what does this journey cost" wants. The answer carries all
// five golongan and the gates as {from, to, price}, so one cached entry
// serves every truck class.
func (m *MAPID) RouteToll(ctx context.Context, profile string, points []Point) ([]byte, error) {
	// {profile, points} is the whole body: tollType is not needed (measured
	// 2026-09-28 — the answer prices all five golongan either way), so it
	// is not sent and cannot be mistaken for something that narrows the
	// answer.
	body := map[string]any{"profile": profile, "points": points}
	return m.post(ctx, "/v2/route_n_toll", body)
}

// post sends one request, retrying a 5xx once. A 4xx is never retried: its
// message is MAPID's own and specific ("out of bounds", "unknown profile"),
// and repeating a rejected request only spends time.
func (m *MAPID) post(ctx context.Context, path string, body map[string]any) ([]byte, error) {
	if !m.Configured() {
		return nil, fmt.Errorf("MAPID_API_KEY is not set on this deployment")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	u := m.baseURL + path + "?key=" + url.QueryEscape(m.key)
	for attempt := 0; ; attempt++ {
		out, status, err := m.once(ctx, u, raw)
		switch {
		case err == nil:
			// Every miss is logged with what it cost us to answer: this is
			// the only record of what was actually bought, and "the fare
			// looked wrong six weeks ago" is otherwise unanswerable.
			n := 0
			if pts, ok := body["points"].([]Point); ok {
				n = len(pts)
			}
			log.Printf("mapid: %s points=%d bytes=%d (paid call)", path, n, len(out))
			return out, nil
		case status >= 500 && attempt == 0:
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		default:
			return nil, err
		}
	}
}

func (m *MAPID) once(ctx context.Context, u string, raw []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(raw))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("mapid unreachable: %w", err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, mapidMaxBody))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("mapid: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, fmt.Errorf("mapid: HTTP %d: %s", resp.StatusCode, trim(out))
	}
	// A 200 IS NOT SUCCESS. MAPID answers "no route found" with 200 and a
	// {"message": …} body carrying no path — cached as-is, that becomes a
	// permanent non-answer for the lane.
	if err := checkBody(out); err != nil {
		return nil, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

// checkBody rejects a 200 that carries no usable answer.
func checkBody(out []byte) error {
	var probe struct {
		Message string            `json:"message"`
		Error   string            `json:"error"`
		Paths   []json.RawMessage `json:"paths"`
		Prices  json.RawMessage   `json:"prices"`
		Route   json.RawMessage   `json:"route"`
	}
	if err := json.Unmarshal(out, &probe); err != nil {
		return fmt.Errorf("mapid: unreadable response: %w", err)
	}
	if len(probe.Paths) > 0 || len(probe.Prices) > 0 || len(probe.Route) > 0 {
		return nil
	}
	msg := probe.Message
	if msg == "" {
		msg = probe.Error
	}
	if msg == "" {
		msg = trim(out)
	}
	return fmt.Errorf("mapid: %s", msg)
}

func trim(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		return s[:300] + "…"
	}
	return s
}
