package routecache

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The provider is part of a request's identity: the same journey answered by
// two engines must not share one cache entry.
func TestProviderSplitsTheKey(t *testing.T) {
	r := Request{Endpoint: "route", Profile: "truck", Points: []Point{{106.8272, -6.1751}, {107.6191, -6.9175}}}
	a := r
	a.Provider = "mapid"
	b := r
	b.Provider = "valhalla@1792847555"

	if string(a.Key()) == string(b.Key()) {
		t.Fatal("mapid and valhalla share a cache key: one engine's geometry would be served as the other's")
	}
	// And a tile rebuild must not keep serving the old roads.
	c := r
	c.Provider = "valhalla@1800000000"
	if string(b.Key()) == string(c.Key()) {
		t.Fatal("two tilesets share a cache key")
	}
}

func TestPickKeepsWhatValhallaCannotAnswerOnMapid(t *testing.T) {
	s := &Service{up: stubUp{name: "mapid"}, router: stubUp{name: "valhalla"}}
	cases := []struct {
		name string
		req  Request
		want string
	}{
		{"plain coordinate routing goes to the router", Request{Endpoint: "route"}, "valhalla"},
		{"id-routed journeys stay upstream", Request{Endpoint: "route_ids"}, "mapid"},
		{"toll stays upstream", Request{Endpoint: "route_toll"}, "mapid"},
		{"alternatives stay upstream", Request{Endpoint: "route", Alternatives: 3}, "mapid"},
	}
	for _, c := range cases {
		if got := s.pick(c.req).Name(); got != c.want {
			t.Errorf("%s: picked %s, want %s", c.name, got, c.want)
		}
	}

	// No router configured leaves everything where it was.
	plain := &Service{up: stubUp{name: "mapid"}}
	if got := plain.pick(Request{Endpoint: "route"}).Name(); got != "mapid" {
		t.Errorf("without a router, picked %s", got)
	}
}

// What gets stored is the payload alone, in the same shape a MAPID answer is
// stored in — the engine's own {payload, cached, provider} wrapper is the
// cache's wrapper, and storing it would nest one inside the other.
func TestValhallaUnwrapsTheEnvelope(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			_, _ = w.Write([]byte(`{"tileset_last_modified":1792847555}`))
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		if got := r.Header.Get("Authorization"); got != "Bearer t0ken" {
			t.Errorf("Authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"payload":{"paths":[{"distance":155909.0}]},"cached":false,"provider":"valhalla (self-hosted)"}`))
	}))
	defer srv.Close()

	v := NewValhalla(srv.URL, "t0ken")
	out, err := v.Route(context.Background(), Request{
		Profile:         "truck",
		Points:          []Point{{106.8272, -6.1751}, {107.6191, -6.9175}},
		SnapPreventions: []string{"ferry", "tunnel"},
	})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	var payload struct {
		Paths []struct {
			Distance float64 `json:"distance"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("stored payload is not the bare MAPID shape: %v", err)
	}
	if len(payload.Paths) != 1 || payload.Paths[0].Distance != 155909.0 {
		t.Fatalf("payload = %s", out)
	}
	// A constraint the caller set must reach the engine, not be dropped on
	// the way: that is the whole bug this guards.
	if _, ok := gotBody["snap_preventions"]; !ok {
		t.Error("snap_preventions was not forwarded to the engine")
	}
	if v.Fingerprint() != "valhalla@1792847555" {
		t.Errorf("Fingerprint = %s", v.Fingerprint())
	}
}

func TestValhallaRefusesWhatItCannotAnswer(t *testing.T) {
	v := NewValhalla("http://example.invalid", "")
	if _, err := v.RouteByIDs(context.Background(), []string{"A", "B"}, false); !errors.Is(err, ErrUnsupported) {
		t.Errorf("RouteByIDs err = %v, want ErrUnsupported", err)
	}
	if _, err := v.RouteToll(context.Background(), "truck", nil); !errors.Is(err, ErrUnsupported) {
		t.Errorf("RouteToll err = %v, want ErrUnsupported", err)
	}
}

type stubUp struct{ name string }

func (s stubUp) Name() string        { return s.name }
func (s stubUp) Fingerprint() string { return s.name }
func (s stubUp) Route(context.Context, Request) ([]byte, error) {
	return []byte(`{}`), nil
}
func (s stubUp) RouteByIDs(context.Context, []string, bool) ([]byte, error) {
	return nil, ErrUnsupported
}
func (s stubUp) RouteToll(context.Context, string, []Point) ([]byte, error) {
	return nil, ErrUnsupported
}
