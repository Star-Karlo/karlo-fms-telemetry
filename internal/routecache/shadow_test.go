package routecache

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// testdata/mapid_route_n_toll.json is a real /v2/route_n_toll answer for
// Jakarta-Merak, truncated to 40 route vertices. Everything the shadow reads
// — prices, details, distance — is untouched production data, because the
// whole value of this table is that it stores what MAPID really said.
func fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/mapid_route_n_toll.json")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return raw
}

func TestParsesUpstreamTollAnswer(t *testing.T) {
	var m mapidToll
	if err := json.Unmarshal(fixture(t), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(m.Route.Coordinates) != 40 {
		t.Fatalf("geometry: got %d vertices, want 40", len(m.Route.Coordinates))
	}
	// [lon, lat], which is the order /v1/toll/quote wants. A swap here would
	// price a journey somewhere off the coast of Somalia and still return 200,
	// so it is asserted rather than assumed.
	if lon, lat := m.Route.Coordinates[0][0], m.Route.Coordinates[0][1]; lon < 100 || lon > 115 || lat > 0 || lat < -10 {
		t.Errorf("first vertex %v is not [lon, lat] in Indonesia", m.Route.Coordinates[0])
	}
	if m.Distance <= 0 {
		t.Errorf("distance: got %v, want positive", m.Distance)
	}

	got := byGolongan(m.Prices)
	want := map[string]int64{"1": 49000, "2": 64500, "3": 64500, "4": 83500, "5": 83500}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("golongan %s: got %d, want %d", k, got[k], v)
		}
	}
	if _, stillPrefixed := got["golongan_1"]; stillPrefixed {
		t.Error("golongan keys kept MAPID's prefix; the two providers' columns will not compare")
	}

	// details is the reason the table exists: the gates MAPID charged at.
	// Kept as raw JSON so a field MAPID adds later is still recorded.
	var details []struct {
		From  *struct{ Name string } `json:"from"`
		To    *struct{ Name string } `json:"to"`
		Price map[string]int64       `json:"price"`
	}
	if err := json.Unmarshal(m.Details, &details); err != nil {
		t.Fatalf("details: %v", err)
	}
	if len(details) != 4 {
		t.Fatalf("details: got %d entries, want 4", len(details))
	}
	// Two of the four carry no price at all — MAPID's own gaps. They must
	// survive into the row rather than being filtered out: a gate the paid
	// provider cannot price either is evidence, not noise.
	var priced int
	for _, d := range details {
		if len(d.Price) > 0 {
			priced++
		}
	}
	if priced != 2 {
		t.Errorf("priced details: got %d, want 2", priced)
	}
}

func TestUnconfiguredShadowDoesNothing(t *testing.T) {
	// No pool and no URL. Compare must be safe to call, because the service
	// calls it on the hot path for every toll answer.
	var s *Shadow
	if s.Configured() {
		t.Fatal("a nil shadow reports itself configured")
	}
	s.Compare(Request{Endpoint: "route_toll"}, fixture(t))

	s = NewShadow(nil, "", "")
	if s.Configured() {
		t.Fatal("a shadow with no base URL reports itself configured")
	}
	s.Compare(Request{Endpoint: "route_toll"}, fixture(t))
}

func TestQuoteSendsGeometryAndReadsTheAnswer(t *testing.T) {
	var gotBody map[string]json.RawMessage
	var gotAuth, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"by_golongan":{"1":40000,"5":70000},
			"segments":[{"gate":"Cilegon Barat"}],
			"unpriced_gates":["Kunciran 8"],"complete":false}`))
	}))
	defer srv.Close()

	s := NewShadow(nil, srv.URL, "tok")
	q, err := s.quote(context.Background(), [][2]float64{{106.0, -6.1}, {105.9, -5.9}})
	if err != nil {
		t.Fatalf("quote: %v", err)
	}

	if gotPath != "/v1/toll/quote" {
		t.Errorf("path: got %q", gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("authorization: got %q, want %q", gotAuth, "Bearer tok")
	}
	if _, ok := gotBody["geometry"]; !ok {
		t.Errorf("body has no geometry: %v", gotBody)
	}
	if q.ByGolongan["5"] != 70000 {
		t.Errorf("golongan 5: got %d, want 70000", q.ByGolongan["5"])
	}
	if q.Complete {
		t.Error("complete: got true, want false — one gate was unpriced")
	}
	if len(q.UnpricedGates) != 1 || q.UnpricedGates[0] != "Kunciran 8" {
		t.Errorf("unpriced: got %v", q.UnpricedGates)
	}
}

func TestQuoteSurfacesAnUpstreamRefusal(t *testing.T) {
	// 503 is what /v1/toll/quote returns when the tariff tables are empty.
	// It must come back as an error so the row records WHY ours was absent
	// rather than storing a silent NULL.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"the tariff tables are empty or unreadable"}`))
	}))
	defer srv.Close()

	s := NewShadow(nil, srv.URL, "")
	if _, err := s.quote(context.Background(), [][2]float64{{106.0, -6.1}, {105.9, -5.9}}); err == nil {
		t.Fatal("a 503 from the pricer was reported as success")
	}
}

func TestCompareSkipsAnAnswerWithNoRoute(t *testing.T) {
	// A journey touching no toll road is a legitimate answer, not a fault.
	// It must not reach the pricer and must not be logged as an error.
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewShadow(nil, srv.URL, "")
	err := s.compare(context.Background(), []byte("key"), "truck", nil,
		[]byte(`{"route":{"type":"LineString","coordinates":[]},"prices":{},"details":[]}`))
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if called {
		t.Error("a toll-free journey was sent to the pricer")
	}
}
