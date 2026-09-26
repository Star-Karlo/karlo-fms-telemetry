package routecache

import "testing"

// The same journey asked in different words is one cache entry; a different
// journey is never the same entry.
func TestKeyIdentity(t *testing.T) {
	base := Request{Endpoint: "route_toll", Profile: "truck",
		Points: []Point{{106.8272, -6.1754}, {110.4381, -7.0051}}}

	same := []Request{
		{Endpoint: "ROUTE_TOLL", Profile: "Truck", Points: base.Points},
		{Endpoint: "route_toll", Profile: "", Points: base.Points}, // truck is the default
		// A coordinate that went through JSON and a NUMERIC column and back.
		{Endpoint: "route_toll", Profile: "truck",
			Points: []Point{{106.82720000000001, -6.17540000000003}, {110.438100000004, -7.00509999999998}}},
	}
	for i, r := range same {
		if string(r.Key()) != string(base.Key()) {
			t.Errorf("same[%d] produced a different key — the cache would miss", i)
		}
	}

	diff := map[string]Request{
		"reversed":       {Endpoint: "route_toll", Profile: "truck", Points: []Point{base.Points[1], base.Points[0]}},
		"via a third":    {Endpoint: "route_toll", Profile: "truck", Points: []Point{base.Points[0], {107.6191, -6.9175}, base.Points[1]}},
		"other profile":  {Endpoint: "route_toll", Profile: "car", Points: base.Points},
		"other endpoint": {Endpoint: "route", Profile: "truck", Points: base.Points},
		"moved 100m":     {Endpoint: "route_toll", Profile: "truck", Points: []Point{{106.8282, -6.1754}, base.Points[1]}},
	}
	for name, r := range diff {
		if string(r.Key()) == string(base.Key()) {
			t.Errorf("%s produced the SAME key — one journey's answer would be served for another", name)
		}
	}
}

// One paid answer serves every truck class: MAPID prices all five golongan
// in one response, so the class a caller asks about must not change the key.
func TestGolonganIsNotPartOfTheKey(t *testing.T) {
	pts := []Point{{106.8272, -6.1754}, {110.4381, -7.0051}}
	a := Request{Endpoint: "route_toll", Profile: "truck", Points: pts}
	b := Request{Endpoint: "route_toll", Profile: "truck", Points: pts}
	if string(a.Key()) != string(b.Key()) {
		t.Error("two identical toll requests keyed differently")
	}
}
