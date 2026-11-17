package dashcam

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/karlo/fms/internal/vss"
)

// fakeVSS is a minimal Gtrack VSS: MD5 login, HTTP-200 envelopes, and enough of
// the video/track endpoints to exercise the handler.
func fakeVSS(t *testing.T) *httptest.Server {
	t.Helper()
	env := func(w http.ResponseWriter, data any) {
		raw, _ := json.Marshal(data)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 10000, "msg": "Success", "data": json.RawMessage(raw)})
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/vss/user/apiLogin.action", func(w http.ResponseWriter, r *http.Request) {
		env(w, map[string]string{"token": "tok", "pid": "pid"})
	})
	mux.HandleFunc("/vss/record/videoFileSearch.action", func(w http.ResponseWriter, r *http.Request) {
		env(w, map[string]any{"files": []map[string]any{
			{"deviceID": "D1", "channel": 1, "duration": 296, "size": 979915, "type": 1,
				"start": "2026-07-24 09:05:00", "stop": "2026-07-24 09:09:56", "path": "/clip1.mp4"},
		}})
	})
	mux.HandleFunc("/vss/track/getTrackList.action", func(w http.ResponseWriter, r *http.Request) {
		// A fix at 09:05:02 — two seconds off the clip start, so NearestFix must
		// pick it and the clip gets a coordinate.
		env(w, []map[string]any{
			{"location": map[string]any{"latitude": "-6.20", "longitude": "106.82", "dtu": "2026-07-24 09:05:02"}},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newHandler(t *testing.T, baseURL string) *Handler {
	t.Helper()
	client := vss.New(vss.Config{BaseURL: baseURL, Username: "u", Password: "p"})
	// Auth always passes in tests; the gate itself is the ingest package's job.
	return NewHandler(client, func(*http.Request) bool { return true }, "test-bucket", nil, nil)
}

func do(t *testing.T, h *Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	h.Routes(r)
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-Ingest-Key", "k")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestVideosAnnotatesEachClipWithCoordinate(t *testing.T) {
	srv := fakeVSS(t)
	h := newHandler(t, srv.URL)
	w := do(t, h, http.MethodGet, "/v1/dashcams/D1/videos?date=2026-07-24")
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var clips []Clip
	if err := json.Unmarshal(w.Body.Bytes(), &clips); err != nil {
		t.Fatal(err)
	}
	if len(clips) != 1 {
		t.Fatalf("want 1 clip, got %d", len(clips))
	}
	c := clips[0]
	if c.Path != "/clip1.mp4" || c.Channel != 1 {
		t.Fatalf("clip fields wrong: %+v", c)
	}
	// The coordinate is DERIVED from the GPS track, not present on the clip —
	// this is the whole point of the endpoint.
	if c.Lat == nil || c.Lon == nil {
		t.Fatal("clip coordinate was not derived from the track")
	}
	if *c.Lat != -6.20 || *c.Lon != 106.82 {
		t.Fatalf("wrong coordinate: %v,%v", *c.Lat, *c.Lon)
	}
}

func TestVideosBadDate(t *testing.T) {
	srv := fakeVSS(t)
	h := newHandler(t, srv.URL)
	w := do(t, h, http.MethodGet, "/v1/dashcams/D1/videos?date=nonsense")
	if w.Code != 400 {
		t.Fatalf("want 400 for bad date, got %d", w.Code)
	}
}

func TestUnconfiguredVSSReturns503(t *testing.T) {
	h := newHandler(t, "") // empty base URL = not configured
	w := do(t, h, http.MethodGet, "/v1/dashcams")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 when VSS unconfigured, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAuthGateRejectsMissingKey(t *testing.T) {
	srv := fakeVSS(t)
	client := vss.New(vss.Config{BaseURL: srv.URL, Username: "u", Password: "p"})
	h := NewHandler(client, func(*http.Request) bool { return false }, "b", nil, nil)
	r := chi.NewRouter()
	h.Routes(r)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashcams", nil) // no key
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 without key, got %d", w.Code)
	}
}

func TestVideoKeyLayout(t *testing.T) {
	at := time.Date(2026, 7, 24, 9, 5, 3, 0, time.UTC)
	got := videoKey("MDVR-1", 2, at)
	want := "video/MDVR-1/2026-07-24/ch2_090503.mp4"
	if got != want {
		t.Fatalf("video key = %q, want %q", got, want)
	}
}

func TestNearestFixPicksClosestInTime(t *testing.T) {
	base := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	track := []vss.TrackPoint{
		{Lat: 1, Lon: 1, Time: base},
		{Lat: 2, Lon: 2, Time: base.Add(5 * time.Minute)},
		{Lat: 3, Lon: 3, Time: base.Add(10 * time.Minute)},
	}
	fix, ok := vss.NearestFix(track, base.Add(6*time.Minute))
	if !ok || fix.Lat != 2 {
		t.Fatalf("expected the 5-min fix, got %+v ok=%v", fix, ok)
	}
	if _, ok := vss.NearestFix(nil, base); ok {
		t.Fatal("empty track must return ok=false")
	}
}
