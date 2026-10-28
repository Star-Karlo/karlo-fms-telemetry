package routecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Upstream is where an answer comes from when the cache misses.
//
// Two implementations, and they are NOT interchangeable. MAPID is metered and
// answers all three endpoints. Valhalla is Karlo's own engine, free per call,
// and answers coordinate routing only — it has never heard of the location
// ids MAPID registers, and it does not price a toll. Service therefore holds
// both and sends each endpoint to whichever can serve it, rather than
// pretending a single provider exists.
type Upstream interface {
	// Name is what the answer reports as its source, for cost accounting.
	Name() string

	// Fingerprint identifies the engine AND THE DATA IT ROUTES ON, and goes
	// into the cache key.
	//
	// Both halves matter. Without the engine, flipping the provider serves
	// MAPID's stored geometry as ours — a 90-day TTL means a quarter of
	// mislabelled answers. Without the data version, a tile rebuild changes
	// every road in the country and the cache keeps serving the old ones
	// until each entry expires.
	Fingerprint() string

	Route(ctx context.Context, r Request) ([]byte, error)
	RouteByIDs(ctx context.Context, ids []string, pointsEncoded bool) ([]byte, error)
	RouteToll(ctx context.Context, profile string, points []Point) ([]byte, error)
}

// ErrUnsupported is an endpoint this engine cannot answer. It is not a
// failure of the request: the caller asked something reasonable of the wrong
// provider, and Service turns it into a fallback rather than an error.
var ErrUnsupported = errors.New("this routing engine does not answer that endpoint")

// Valhalla is Karlo's self-hosted routing engine (FMS/routing-service), which
// speaks the cache's own request and response shapes on POST /v1/route
// precisely so it can stand in for MAPID here.
//
// IT COSTS NOTHING PER CALL, which changes what the cache is for: not saving
// money, but saving the half-second the engine takes on a long lane.
type Valhalla struct {
	http    *http.Client
	baseURL string
	token   string

	mu      sync.Mutex
	tileset string // from /status, memoised; "" until a probe succeeds
}

const (
	valhallaTimeout = 60 * time.Second
	valhallaMaxBody = 16 << 20 // its geometry is denser than MAPID's
)

func NewValhalla(baseURL, token string) *Valhalla {
	return &Valhalla{
		http:    &http.Client{Timeout: valhallaTimeout},
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
	}
}

// Configured reports whether there is anywhere to send a request.
func (v *Valhalla) Configured() bool { return v != nil && v.baseURL != "" }

func (v *Valhalla) Name() string { return "valhalla" }

// Fingerprint is "valhalla@<tileset>", the tileset being the engine's own
// tile build timestamp from /status.
//
// Probed lazily and memoised: a startup probe would tie this service's boot
// to another service being up, and the answer never changes while the task
// lives. Until a probe succeeds the fingerprint says so, which keys those
// answers separately — a cold cache for a few requests, rather than answers
// filed under a version we were guessing at.
func (v *Valhalla) Fingerprint() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.tileset == "" {
		v.tileset = v.probeTileset()
	}
	return "valhalla@" + v.tileset
}

func (v *Valhalla) probeTileset() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.baseURL+"/status", nil)
	if err != nil {
		return "unknown"
	}
	if v.token != "" {
		req.Header.Set("Authorization", "Bearer "+v.token)
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return "unknown"
	}
	defer resp.Body.Close()
	var out struct {
		TilesetLastModified int64 `json:"tileset_last_modified"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out) != nil || out.TilesetLastModified == 0 {
		return "unknown"
	}
	return fmt.Sprint(out.TilesetLastModified)
}

// Route asks the engine for a path, in the cache's own request shape.
//
// Alternatives is NOT passed on and callers asking for it never reach here —
// Service keeps those on MAPID. The engine answers one route, and returning
// one where three were asked for would look like a thin day for alternatives
// rather than a provider that cannot do it.
func (v *Valhalla) Route(ctx context.Context, r Request) ([]byte, error) {
	body := map[string]any{"points": r.Points}
	if r.Profile != "" {
		body["profile"] = r.Profile
	}
	if len(r.SnapPreventions) > 0 {
		body["snap_preventions"] = r.SnapPreventions
	}
	raw, err := v.post(ctx, "/v1/route", body)
	if err != nil {
		return nil, err
	}
	// The engine wraps its answer the way this service does — {payload,
	// cached, provider} — but what gets STORED here is the payload alone,
	// the same shape a MAPID answer is stored in. Unwrapping it at the edge
	// is what keeps one cache table holding two providers' answers.
	var env struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || len(env.Payload) == 0 {
		return nil, fmt.Errorf("valhalla: unexpected response shape")
	}
	return env.Payload, nil
}

// RouteByIDs is MAPID's: Karlo's location ids are registered with MAPID, and
// mean nothing to a Valhalla tile.
func (v *Valhalla) RouteByIDs(context.Context, []string, bool) ([]byte, error) {
	return nil, ErrUnsupported
}

// RouteToll is MAPID's for now. The engine measures toll KILOMETRES, but the
// fare comes from Karlo's own tariff tables behind fms-api /v1/toll/quote,
// which is a different service and a different change.
func (v *Valhalla) RouteToll(context.Context, string, []Point) ([]byte, error) {
	return nil, ErrUnsupported
}

func (v *Valhalla) post(ctx context.Context, path string, body map[string]any) ([]byte, error) {
	if !v.Configured() {
		return nil, errors.New("ROUTING_BASE_URL is not set on this deployment")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if v.token != "" {
		req.Header.Set("Authorization", "Bearer "+v.token)
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("valhalla unreachable: %w", err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, valhallaMaxBody))
	if err != nil {
		return nil, fmt.Errorf("valhalla: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("valhalla: HTTP %d: %s", resp.StatusCode, trim(out))
	}
	return out, nil
}
