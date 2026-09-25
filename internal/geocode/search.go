package geocode

// Forward geocoding over our own data — "type an address → pin", with no
// external provider. Asked for by TMS's warehouse form (2026-09-18).
//
// The corpus is what the reverse geocoder already holds: 82k kelurahan
// polygons (each carrying kecamatan / kabupaten / provinsi) and 406k named
// OSM roads. So a query can resolve to a ROAD (best-matching segment inside
// the matched area, pin at its midpoint) or an ADMIN AREA (kelurahan,
// kecamatan or kabupaten, pin at the polygon's point-on-surface). It cannot
// resolve house numbers or business names; the caller drags the pin the last
// few metres.
//
// Matching is trigram similarity (pg_trgm) on names, case-insensitive, with
// "Kota X" / "Kabupaten X" / "Kab. X" folded to X. "Jl. Raya Bekasi, Cikarang
// Utara" is split at the first comma: the head is the road (or area) being
// looked for, the tail narrows the area. Results: roads before areas, then by
// similarity, then bigger areas last.

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Candidate is one search hit.
type Candidate struct {
	Kind    string  `json:"kind"` // road | kelurahan | kecamatan | kabupaten
	Name    string  `json:"name"`
	Label   string  `json:"label"` // display string, composed once for every caller
	Address Address `json:"address"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Score   float64 `json:"score"` // 0..1 match quality
}

// Address is the admin chain of a hit (kota/kabupaten folded into Kabupaten,
// the reverse endpoint's split is a presentation choice it keeps to itself).
type Address struct {
	Kelurahan string `json:"kelurahan,omitempty"`
	Kecamatan string `json:"kecamatan,omitempty"`
	Kabupaten string `json:"kabupaten,omitempty"`
	Provinsi  string `json:"provinsi,omitempty"`
}

var (
	adminPrefix = regexp.MustCompile(`(?i)^\s*(kota|kabupaten|kab\.?|kecamatan|kec\.?|kelurahan|kel\.?|desa)\s+`)
	// levelOf maps an explicit prefix to the admin level the user meant.
	levelOf = map[string]string{"kota": "kabupaten", "kabupaten": "kabupaten", "kab": "kabupaten", "kecamatan": "kecamatan", "kec": "kecamatan", "kelurahan": "kelurahan", "kel": "kelurahan", "desa": "kelurahan"}
	roadPrefix  = regexp.MustCompile(`(?i)^\s*(jalan|jl\.?|jln\.?)\s+`)
	spaces      = regexp.MustCompile(`\s+`)
)

// normalise strips admin prefixes and squeezes whitespace, for matching.
func normalise(s string) string {
	s = strings.TrimSpace(adminPrefix.ReplaceAllString(s, ""))
	return spaces.ReplaceAllString(s, " ")
}

// Search returns up to limit candidates for q.
func (s *Store) Search(ctx context.Context, q string, limit int) ([]Candidate, error) {
	q = strings.TrimSpace(q)
	if len(q) < 3 {
		return []Candidate{}, nil
	}
	if limit <= 0 || limit > 25 {
		limit = 10
	}
	// Accept the spellings other systems use for the two provinces whose
	// names are abbreviations ("DI YOGYAKARTA" is master data's), so a caller
	// pasting an address from there is not told the place does not exist.
	q = canonProvinceIn(q)
	head, tail := q, ""
	if i := strings.Index(q, ","); i >= 0 {
		head, tail = strings.TrimSpace(q[:i]), strings.TrimSpace(q[i+1:])
	}
	headN := normalise(head)
	roadHead := strings.TrimSpace(roadPrefix.ReplaceAllString(head, "")) // roads are stored "Jalan X"
	tailN := normalise(strings.ReplaceAll(tail, ",", " "))
	// What the user said they are looking for: "Jl. …" → a road; "Kota …" /
	// "Kecamatan …" → that level only; nothing → areas outrank roads on a
	// tie ("Pekanbaru" is the city before it is any "Jalan Pekanbaru").
	wantRoad := roadPrefix.MatchString(head)
	wantLevel := ""
	if m := adminPrefix.FindStringSubmatch(head); m != nil {
		wantLevel = levelOf[strings.ToLower(strings.TrimSuffix(m[1], "."))]
	}

	out := []Candidate{}

	// ROADS. Candidate segments by trigram similarity on the name; each is
	// placed in its kelurahan (spatial join at the segment midpoint) and, when
	// the query has a tail, kept only if the tail matches the area chain.
	// One hit per (road name, kecamatan): a long road is many OSM segments.
	const roadSQL = `
WITH cand AS (
  -- word_similarity(q, name): how well the query matches a PART of the road
  -- name, so "Sudirman" finds "Jalan Jenderal Sudirman" at full score.
  SELECT r.name, ST_LineInterpolatePoint(r.geom, 0.5) AS mid,
         GREATEST(similarity(r.name, 'Jalan ' || $1), word_similarity($1, r.name)) AS sim
  FROM road r
  WHERE $1 <% r.name
  ORDER BY sim DESC LIMIT 200
), placed AS (
  SELECT c.name, c.sim, c.mid, k.kel_name, k.kec_name, k.kab_name, k.prov_name
  FROM cand c
  LEFT JOIN LATERAL (SELECT kel_name, kec_name, kab_name, prov_name FROM kelurahan k WHERE ST_Contains(k.geom, c.mid) LIMIT 1) k ON true
), scored AS (
  SELECT *, CASE WHEN $2 = '' THEN 1.0
                 ELSE GREATEST(word_similarity($2, coalesce(kec_name,'')), word_similarity($2, coalesce(kab_name,'')),
                               word_similarity($2, coalesce(kel_name,'')), word_similarity($2, coalesce(prov_name,''))) END AS area_sim
  FROM placed
)
SELECT DISTINCT ON (name, kec_name) name, ST_Y(mid), ST_X(mid), kel_name, kec_name, kab_name, prov_name, sim, area_sim
FROM scored
WHERE $2 = '' OR area_sim >= 0.4
ORDER BY name, kec_name, sim DESC`
	rows, err := s.pool.Query(ctx, roadSQL, roadHead, tailN)
	if err != nil {
		return nil, fmt.Errorf("road search: %w", err)
	}
	if wantLevel != "" {
		rows.Close()
		rows, err = s.pool.Query(ctx, "SELECT NULL::text, 0::float8, 0::float8, NULL::text, NULL::text, NULL::text, NULL::text, 0::float8, 0::float8 WHERE false")
		if err != nil {
			return nil, err
		}
	}
	for rows.Next() {
		var name string
		var lat, lon, sim, areaSim float64
		var kel, kec, kab, prov *string
		if err := rows.Scan(&name, &lat, &lon, &kel, &kec, &kab, &prov, &sim, &areaSim); err != nil {
			rows.Close()
			return nil, err
		}
		a := Address{Kelurahan: deref(kel), Kecamatan: deref(kec), Kabupaten: deref(kab), Provinsi: canonProvince(deref(prov))}
		score := sim
		if tailN != "" {
			score = 0.6*sim + 0.4*areaSim
		}
		out = append(out, Candidate{Kind: "road", Name: name, Label: label(name, a), Address: a, Lat: lat, Lon: lon, Score: round2(score)})
	}
	rows.Close()

	// ADMIN AREAS: one row per distinct area at each level whose name matches
	// the head; the tail (if any) must match a parent. Point-on-surface of the
	// union keeps the pin inside the area even for crescent shapes.
	const areaSQL = `
WITH lv AS (
  -- one indexed predicate per level (the trigram GIN indexes only apply to
  -- a direct column or the exact bare-kabupaten expression)
  SELECT 'kelurahan' AS kind, kel_name AS name, kel_name, kec_name, kab_name, prov_name, geom, similarity(kel_name, $1) AS sim
  FROM kelurahan WHERE kel_name % $1
  UNION ALL
  SELECT 'kecamatan', kec_name, NULL, kec_name, kab_name, prov_name, geom, similarity(kec_name, $1)
  FROM kelurahan WHERE kec_name % $1
  UNION ALL
  SELECT 'kabupaten', kab_name, NULL, NULL, kab_name, prov_name, geom, similarity(regexp_replace(kab_name, '^(Kota|Kabupaten) ', ''), $1)
  FROM kelurahan WHERE regexp_replace(kab_name, '^(Kota|Kabupaten) ', '') % $1
), m AS (
  SELECT *, CASE WHEN $2 = '' THEN 1.0 ELSE GREATEST(word_similarity($2, coalesce(kec_name,'')), word_similarity($2, coalesce(kab_name,'')), word_similarity($2, coalesce(prov_name,''))) END AS area_sim
  FROM lv
)
SELECT kind, name, min(kel_name), min(kec_name), min(kab_name), min(prov_name), max(sim), max(area_sim),
       -- pin inside the LARGEST member polygon: always inside the area and
       -- far cheaper than a union of a whole regency
       ST_Y(ST_PointOnSurface((array_agg(geom ORDER BY ST_Area(geom) DESC))[1])),
       ST_X(ST_PointOnSurface((array_agg(geom ORDER BY ST_Area(geom) DESC))[1]))
FROM m
WHERE ($2 = '' OR area_sim >= 0.4) AND ($3 = '' OR kind = $3)
GROUP BY kind, name, kec_name, kab_name, prov_name
ORDER BY max(sim) DESC LIMIT 15`
	rows, err = s.pool.Query(ctx, areaSQL, headN, tailN, wantLevel)
	if err != nil {
		return nil, fmt.Errorf("area search: %w", err)
	}
	for rows.Next() {
		var kind, name string
		var kel, kec, kab, prov *string
		var sim, areaSim, lat, lon float64
		if err := rows.Scan(&kind, &name, &kel, &kec, &kab, &prov, &sim, &areaSim, &lat, &lon); err != nil {
			rows.Close()
			return nil, err
		}
		a := Address{Kecamatan: deref(kec), Kabupaten: deref(kab), Provinsi: canonProvince(deref(prov))}
		switch kind {
		case "kelurahan":
			a.Kelurahan = deref(kel)
		case "kecamatan":
			a.Kelurahan = ""
		case "kabupaten":
			a.Kelurahan, a.Kecamatan = "", ""
		}
		score := sim
		if tailN != "" {
			score = 0.6*sim + 0.4*areaSim
		}
		out = append(out, Candidate{Kind: kind, Name: name, Label: label(name, a), Address: a, Lat: lat, Lon: lon, Score: round2(score)})
	}
	rows.Close()

	rank := map[string]int{"kabupaten": 0, "kecamatan": 1, "kelurahan": 2, "road": 3}
	if wantRoad {
		rank = map[string]int{"road": 0, "kelurahan": 1, "kecamatan": 2, "kabupaten": 3}
	}
	sortCandidates(out, rank)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// label composes the display string every caller shows: the name, then the
// admin chain above it, deduplicated ("Cikarang Utara, Bekasi, Jawa Barat").
func label(name string, a Address) string {
	parts := []string{name}
	for _, p := range []string{a.Kelurahan, a.Kecamatan, a.Kabupaten, a.Provinsi} {
		if p != "" && !strings.EqualFold(p, name) && !strings.EqualFold(p, parts[len(parts)-1]) {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ", ")
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

// sortCandidates: match quality first, then kind order for ties (roads
// before areas, bigger areas last).
func sortCandidates(c []Candidate, rank map[string]int) {
	for i := 1; i < len(c); i++ {
		for j := i; j > 0; j-- {
			a, b := c[j-1], c[j]
			better := b.Score > a.Score+0.05 || (absDiff(a.Score, b.Score) <= 0.05 && rank[b.Kind] < rank[a.Kind])
			if !better {
				break
			}
			c[j-1], c[j] = b, a
		}
	}
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}

// SearchPool exposes the store's pool for the schema check at startup.
func (s *Store) SearchPool() *pgxpool.Pool { return s.pool }
