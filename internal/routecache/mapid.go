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
	// tollTypeUpstream is the class we always ASK for. The response prices
	// every golongan, so this affects nothing in the answer and keeps one
	// cache entry per journey (see Request).
	tollTypeUpstream = 1
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
func (m *MAPID) Route(ctx context.Context, profile string, points []Point) ([]byte, error) {
	body := map[string]any{"profile": profile, "points": points, "points_encoded": false}
	return m.post(ctx, "/", body)
}

// RouteToll asks for a path priced by toll gate.
func (m *MAPID) RouteToll(ctx context.Context, profile string, points []Point) ([]byte, error) {
	body := map[string]any{"profile": profile, "points": points, "tollType": tollTypeUpstream}
	return m.post(ctx, "/v2/route_n_toll/rute", body)
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
			log.Printf("mapid: %s points=%d bytes=%d (paid call)", path, len(body["points"].([]Point)), len(out))
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
