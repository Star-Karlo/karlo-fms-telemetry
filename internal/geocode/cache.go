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

func NewCache(rdb *redis.Client) *Cache {
	return &Cache{rdb: rdb, prefix: "geo:kec:"}
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
