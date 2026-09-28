package routecache

// Documentation-only types.
//
// The service returns whatever MAPID said, verbatim, under `payload` — which
// is right for the code and useless in a browser: the reader is shown an
// opaque object and has to call the endpoint to learn what comes back. These
// structs are never constructed at runtime; they exist so the Swagger page
// can show the real shape, with values taken from actual production
// responses (Jakarta -> Semarang, truck, 2026-09-28).
//
// If MAPID adds a field, callers get it immediately and this only lags the
// documentation — the opposite trade to modelling the payload for real,
// where a new field would be silently dropped.

// GeoJSONLine is a route geometry: [[lon,lat], …].
type GeoJSONLine struct {
	Type string `json:"type" example:"LineString"`
	// Coordinates are [lon, lat] pairs along the line, e.g.
	// [[106.8272,-6.1754], [106.8291,-6.1802], … , [110.4381,-7.0051]].
	Coordinates [][]float64 `json:"coordinates" swaggertype:"array,array,number"`
}

// GeoJSONPoint is a single position, as a toll gate carries.
type GeoJSONPoint struct {
	Type        string    `json:"type" example:"Point"`
	Coordinates []float64 `json:"coordinates" swaggertype:"array,number" example:"106.815531,-6.2236949"`
}

// RoutePath is one route option. MAPID returns metres and MILLISECONDS.
type RoutePath struct {
	Distance      float64     `json:"distance" example:"445023.495"`
	Time          int64       `json:"time" example:"20009019"`
	Points        GeoJSONLine `json:"points"`
	PointsEncoded bool        `json:"points_encoded" example:"false"`
	Bbox          []float64   `json:"bbox" swaggertype:"array,number" example:"106.815,-7.005,110.438,-6.175"`
	Ascend        float64     `json:"ascend" example:"1204.5"`
	Descend       float64     `json:"descend" example:"1198.2"`
}

// RoutePayload is the MAPID body under `payload` for /v1/route.
type RoutePayload struct {
	Paths []RoutePath `json:"paths"`
}

// RouteAnswer is what /v1/route returns.
type RouteAnswer struct {
	Payload  RoutePayload `json:"payload"`
	Cached   bool         `json:"cached" example:"true"`
	CachedAt string       `json:"cached_at,omitempty" example:"2026-09-28T04:12:07Z"`
	Hits     int64        `json:"hits,omitempty" example:"37"`
	Source   string       `json:"source" example:"mapid"`
}

// TollPrices is a fare per vehicle class. Golongan I is a car; a rigid truck
// is II–III and an articulated one IV–V, which is why the classes come in
// pairs at the same price.
type TollPrices struct {
	Golongan1 int64 `json:"golongan_1" example:"432000"`
	Golongan2 int64 `json:"golongan_2" example:"665500"`
	Golongan3 int64 `json:"golongan_3" example:"665500"`
	Golongan4 int64 `json:"golongan_4" example:"874500"`
	Golongan5 int64 `json:"golongan_5" example:"874500"`
}

// TollGateEnd is one end of a tolled segment. `gate` is "in" or "out";
// `prepaid` says the barrier takes a prepaid card.
type TollGateEnd struct {
	Name     string       `json:"name" example:"Cikampek Utama 1"`
	Gate     string       `json:"gate" example:"in"`
	Prepaid  bool         `json:"prepaid" example:"true"`
	Geometry GeoJSONPoint `json:"geometry"`
}

// TollSegment is one priced stretch between two gates. On a closed-system
// ruas the fare is the entry-exit PAIR, charged once at the exit, so a long
// segment carries the whole ruas and short open-system gates sit beside it.
// The segment prices are a partition of the total, NOT charges to sum.
type TollSegment struct {
	From  TollGateEnd  `json:"from"`
	To    *TollGateEnd `json:"to"`
	Price TollPrices   `json:"price"`
}

// TollPayload is the MAPID body under `payload` for /v1/route-toll. It
// carries the route as well, so a caller needing both makes one request.
type TollPayload struct {
	Prices   TollPrices    `json:"prices"`
	Details  []TollSegment `json:"details"`
	Route    GeoJSONLine   `json:"route"`
	Distance float64       `json:"distance" example:"445023.495"`
	Eta      float64       `json:"eta" example:"2670.14"`
	Bbox     []float64     `json:"bbox" swaggertype:"array,number" example:"106.815,-7.005,110.438,-6.175"`
}

// TollAnswer is what /v1/route-toll returns.
type TollAnswer struct {
	Payload  TollPayload `json:"payload"`
	Cached   bool        `json:"cached" example:"false"`
	CachedAt string      `json:"cached_at,omitempty" example:"2026-09-28T04:12:07Z"`
	Hits     int64       `json:"hits,omitempty" example:"12"`
	Source   string      `json:"source" example:"mapid"`
}

// PurgeResult is what /v1/route-cache/purge returns.
type PurgeResult struct {
	Purged int64 `json:"purged" example:"3"`
}

// APIError is the shape of every failure. On a 502 the message is MAPID's
// own wording, passed through rather than replaced.
type APIError struct {
	Error string `json:"error" example:"mapid: HTTP 400: out of bounds"`
}
