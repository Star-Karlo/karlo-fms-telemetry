package routecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Shadow records what Karlo's own toll pricing WOULD have answered, beside
// what MAPID actually answered, for every toll fare the platform serves.
//
// WHY THIS EXISTS. Karlo's own pricer is measurably not ready: against six
// hand-built lanes on 2026-10-28 it was between 88% under and 66% over
// MAPID, and complete on one lane in six. Six lanes is also far too small a
// sample to improve it against — the defects found so far (several booths of
// one plaza charged separately, tariff names that do not join to gate names,
// open systems charged per gate instead of per tap) each showed up on some
// lanes and not others, and fixing one made another worse.
//
// This closes that gap without risking a single quoted fare. MAPID's answer
// is still the one served; ours is computed beside it and both are written
// down. A week of ordinary traffic yields a comparison per distinct journey
// instead of six, and it costs nothing: the MAPID call happens anyway.
//
// WHAT MAKES IT WORTH A TABLE RATHER THAN A LOG LINE. MAPID's reply carries
// `details` — the gates it charged at, each with a name, a Point and a price.
// That is the one thing the pricer cannot currently derive and the thing
// every open defect needs: which booths are transactions, where they are,
// and what each costs. Stored as rows it can be queried and joined against
// the gate table; in CloudWatch it cannot.
//
// NOTHING HERE MAY AFFECT THE ANSWER. Compare returns before any work
// starts, runs on its own context so writing the response cannot cancel it,
// and every failure is a log line. A fare must never be slower, or wrong,
// because the shadow was watching.
type Shadow struct {
	pool    *pgxpool.Pool
	baseURL string
	token   string
	hc      *http.Client
}

// NewShadow builds the comparator. An empty baseURL leaves it unconfigured
// and Compare becomes a no-op, which is the default state.
func NewShadow(pool *pgxpool.Pool, baseURL, token string) *Shadow {
	return &Shadow{
		pool:    pool,
		baseURL: baseURL,
		token:   token,
		// Generous: this is off the request path, and a quote that takes ten
		// seconds is still a comparison worth having.
		hc: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *Shadow) Configured() bool {
	return s != nil && s.pool != nil && s.baseURL != ""
}

// mapidToll is the part of /v2/route_n_toll's reply this needs.
//
// Shape measured against production on 2026-10-28:
//
//	{route: {type, coordinates}, prices: {golongan_1..5}, details: [...],
//	 distance, eta, bbox}
//
// details holds one entry per charge as {from, to, price}, where from and to
// are each {gate, prepaid, name, geometry} or null — an open-system tap has
// one side only. It is kept as raw JSON rather than parsed into a struct: it
// is evidence, and a struct would silently drop whatever MAPID adds to it.
type mapidToll struct {
	Route struct {
		Coordinates [][2]float64 `json:"coordinates"`
	} `json:"route"`
	Prices   map[string]int64 `json:"prices"`
	Details  json.RawMessage  `json:"details"`
	Distance float64          `json:"distance"`
}

// ourQuote is fms-backend's /v1/toll/quote reply.
type ourQuote struct {
	ByGolongan    map[string]int64 `json:"by_golongan"`
	Segments      json.RawMessage  `json:"segments"`
	UnpricedGates []string         `json:"unpriced_gates"`
	Complete      bool             `json:"complete"`
}

// Compare records a comparison for one toll answer. It returns immediately.
//
// n must already carry the provider that answered, because the key it
// produces is the route_cache key for that answer — the two tables are meant
// to be joined on it.
func (s *Shadow) Compare(n Request, payload json.RawMessage) {
	if !s.Configured() || len(payload) == 0 {
		return
	}
	key := n.Key()
	// A copy, because the caller's slices outlive this call only by accident.
	points := append([]Point(nil), n.Points...)
	profile := n.Profile
	buf := append([]byte(nil), payload...)

	go func() {
		// Deliberately NOT the request context: that one is cancelled the
		// moment the response is written, which is before this finishes.
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if err := s.compare(ctx, key, profile, points, buf); err != nil {
			log.Printf("routecache: toll shadow %x: %v", key[:6], err)
		}
	}()
}

func (s *Shadow) compare(ctx context.Context, key []byte, profile string, points []Point, payload []byte) error {
	var m mapidToll
	if err := json.Unmarshal(payload, &m); err != nil {
		return fmt.Errorf("parse upstream toll reply: %w", err)
	}
	if len(m.Route.Coordinates) < 2 {
		// Not an error worth a line: a toll reply can legitimately hold no
		// route when the journey touches no toll road.
		return nil
	}

	// Ours. A failure here is the thing being measured, so it is recorded
	// rather than returned: "the pricer refused" is a result.
	var (
		ours    *ourQuote
		oursErr string
	)
	q, err := s.quote(ctx, m.Route.Coordinates)
	if err != nil {
		oursErr = err.Error()
	} else {
		ours = q
	}

	return s.record(ctx, key, profile, points, m, ours, oursErr)
}

// quote asks fms-backend what the journey costs against Karlo's own tables.
func (s *Shadow) quote(ctx context.Context, geometry [][2]float64) (*ourQuote, error) {
	body, err := json.Marshal(map[string]any{"geometry": geometry})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/v1/toll/quote", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	out := make([]byte, 0, 64<<10)
	chunk := make([]byte, 32<<10)
	for len(out) < 1<<20 {
		nr, rerr := resp.Body.Read(chunk)
		out = append(out, chunk[:nr]...)
		if rerr != nil {
			break
		}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("toll quote: %s: %s", resp.Status, trimForLog(out))
	}
	var q ourQuote
	if err := json.Unmarshal(out, &q); err != nil {
		return nil, fmt.Errorf("parse toll quote: %w", err)
	}
	return &q, nil
}

// record upserts the comparison. A journey asked for twice updates its row
// and bumps seen rather than writing a second one: the value is one row per
// distinct journey, and the repeat count says which lanes matter.
func (s *Shadow) record(ctx context.Context, key []byte, profile string, points []Point, m mapidToll, ours *ourQuote, oursErr string) error {
	mapidRp, err := json.Marshal(byGolongan(m.Prices))
	if err != nil {
		return err
	}
	pts, err := json.Marshal(points)
	if err != nil {
		return err
	}
	details := m.Details
	if len(details) == 0 {
		details = json.RawMessage("[]")
	}

	var (
		oursRp       []byte
		oursSegments []byte
		oursUnpriced []byte
		oursComplete *bool
	)
	if ours != nil {
		if oursRp, err = json.Marshal(byGolongan(ours.ByGolongan)); err != nil {
			return err
		}
		oursSegments = ours.Segments
		if len(oursSegments) == 0 {
			oursSegments = json.RawMessage("[]")
		}
		if oursUnpriced, err = json.Marshal(ours.UnpricedGates); err != nil {
			return err
		}
		c := ours.Complete
		oursComplete = &c
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO toll_shadow (
			key, profile, points, distance_m,
			mapid_rp, mapid_gates,
			ours_rp, ours_gates, ours_unpriced, ours_complete, ours_error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (key) DO UPDATE SET
			seen          = toll_shadow.seen + 1,
			observed_at   = now(),
			distance_m    = EXCLUDED.distance_m,
			mapid_rp      = EXCLUDED.mapid_rp,
			mapid_gates   = EXCLUDED.mapid_gates,
			ours_rp       = EXCLUDED.ours_rp,
			ours_gates    = EXCLUDED.ours_gates,
			ours_unpriced = EXCLUDED.ours_unpriced,
			ours_complete = EXCLUDED.ours_complete,
			ours_error    = EXCLUDED.ours_error`,
		key, profile, pts, m.Distance,
		mapidRp, []byte(details),
		oursRp, oursSegments, oursUnpriced, oursComplete, nullIfEmpty(oursErr))
	return err
}

// byGolongan keys a price map on the bare class number.
//
// MAPID answers {"golongan_1": n} and our own endpoint answers {"1": n}. Both
// go through here so the two JSON columns of a row can be compared key for
// key in SQL without either side's spelling leaking into the query.
func byGolongan(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[trimPrefix(k, "golongan_")] = v
	}
	return out
}

func trimPrefix(s, p string) string {
	if len(s) > len(p) && s[:len(p)] == p {
		return s[len(p):]
	}
	return s
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func trimForLog(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
