package geocode

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache stores reverse-geocode results keyed by geohash cell.
type Cache struct {
	rdb    *redis.Client
	prefix string
}

// Bump this prefix version whenever the geocode result shape or data source
// changes, so a deploy ignores stale cache entries instead of serving them.
// v2: COD-AB kelurahan hierarchy + OSM streets (was kecamatan-only).
// v3: one spelling per province ("DKI Jakarta", not the data's "Dki Jakarta").
// v4: points outside every polygon (ports, jetties, reclaimed aprons) snap to
//
//	the nearest kelurahan and carry admin_approx, where v3 cached an empty
//	admin chain for them.
//
//	Entries live 30 days, so without this bump the corrected name would
//	have reached a busy cell only after its cached copy expired — and every
//	port already looked up would have kept answering with a blank region
//	long after the fix shipped.
func NewCache(rdb *redis.Client) *Cache {
	return &Cache{rdb: rdb, prefix: "geo:v4:"}
}

func (c *Cache) Get(ctx context.Context, hash string) (entry, bool) {
	b, err := c.rdb.Get(ctx, c.prefix+hash).Bytes()
	if err != nil {
		return entry{}, false // miss or redis error → treat as miss
	}
	var e entry
	if json.Unmarshal(b, &e) != nil {
		return entry{}, false
	}
	return e, true
}

func (c *Cache) Set(ctx context.Context, hash string, e entry, ttl time.Duration) {
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	_ = c.rdb.Set(ctx, c.prefix+hash, b, ttl).Err() // best-effort; miss on failure
}
