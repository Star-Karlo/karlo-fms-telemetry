// Package routecache stores MAPID's paid answers, keyed by the content of
// the request that produced them.
package routecache

import (
	"crypto/sha256"
	"math"
	"strconv"
	"strings"
)

// keyVersion is part of every key. Bump it whenever the STORED SHAPE changes
// — not when MAPID's data changes. Without it a deploy that changes what we
// keep keeps serving the old shape until the last entry expires, which on a
// 90-day TTL is a quarter of silently wrong answers. (The geocode cache has
// the same field and it was needed within a week of shipping.)
const keyVersion = "v1"

// coordDecimals is where a coordinate is cut before it becomes part of a key.
//
// Five decimals is about a metre — far finer than any routing decision — and
// the cut is what makes the cache work at all: the same warehouse pin makes
// a round trip through JSON, Postgres NUMERIC and a JavaScript float and
// comes back as 110.37017880000001. Without rounding, the cache appears to
// work, hits in testing, and misses in production.
const coordDecimals = 5

// Point is [lon, lat], MAPID's order.
type Point [2]float64

// Request is everything that makes one MAPID answer different from another.
//
// tollType is NOT here, deliberately. MAPID prices every golongan in one
// response, so one paid call answers all five truck classes; keying on the
// class would buy the same lane five times and cut the hit rate to a fifth
// (TMS learned this first — their cache omits it too). The service always
// asks upstream with the same class and lets the caller read the golongan
// it wants out of `prices`.
type Request struct {
	Endpoint string // route | route_toll
	Profile  string // truck (default) | car | motorcycle | foot
	Points   []Point
}

// Normalise fills in the defaults and rounds the coordinates, so two callers
// asking the same question in different words produce the same key.
func (r Request) Normalise() Request {
	out := Request{
		Endpoint: strings.ToLower(strings.TrimSpace(r.Endpoint)),
		Profile:  strings.ToLower(strings.TrimSpace(r.Profile)),
		Points:   make([]Point, 0, len(r.Points)),
	}
	if out.Profile == "" {
		out.Profile = "truck"
	}
	for _, p := range r.Points {
		out.Points = append(out.Points, Point{round(p[0]), round(p[1])})
	}
	return out
}

func round(f float64) float64 {
	p := math.Pow10(coordDecimals)
	return math.Round(f*p) / p
}

// Key is the cache key: sha256 over the version, endpoint, profile and the
// ORDERED points. Order is identity — A→B, B→A and A→C→B are three
// different journeys and three different entries.
func (r Request) Key() []byte {
	n := r.Normalise()
	var b strings.Builder
	b.WriteString(keyVersion)
	b.WriteByte('|')
	b.WriteString(n.Endpoint)
	b.WriteByte('|')
	b.WriteString(n.Profile)
	for _, p := range n.Points {
		b.WriteByte('|')
		b.WriteString(fmtCoord(p[0]))
		b.WriteByte(',')
		b.WriteString(fmtCoord(p[1]))
	}
	sum := sha256.Sum256([]byte(b.String()))
	return sum[:]
}

// fmtCoord prints a rounded coordinate the same way every time: fixed
// decimals, never scientific notation. %v switches to exponent form past a
// certain magnitude and the key silently changes shape.
func fmtCoord(f float64) string {
	return strconv.FormatFloat(f, 'f', coordDecimals, 64)
}
