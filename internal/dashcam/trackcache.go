package dashcam

import (
	"context"
	"sync"
	"time"

	"github.com/karlo/fms/internal/vss"
)

// fix is one GPS point with its TRUE instant. vss.Track returns the device-local
// (WIB) timestamp parsed as a naive time in the UTC zone, so we re-stamp its wall
// clock as WIB exactly once — here — and everything downstream works in real
// instants.
type fix struct {
	Lat, Lon float64
	T        time.Time
}

// trackCache memoises a device's GPS track a whole WIB day at a time.
//
// VSS throttles the track API hard (status 10129 "requests too frequent"), and a
// user exploring trip history changes the date range repeatedly — each change
// otherwise a fresh track call. Fetching the day once and slicing every window
// from memory turns N calls into 1, which is what stops the intermittent 502s.
// The TTL is short because today's track keeps growing.
type trackCache struct {
	vss *vss.Client
	mu  sync.Mutex
	m   map[string]cacheEntry
}

type cacheEntry struct {
	fixes  []fix
	expiry time.Time
}

const trackTTL = 5 * time.Minute

func newTrackCache(v *vss.Client) *trackCache {
	return &trackCache{vss: v, m: map[string]cacheEntry{}}
}

// rangeFixes returns the device's fixes within [from, to) (both WIB), fetching
// and caching whole days as needed. A window within one day is a single cached
// day; a window crossing midnight spans two.
func (c *trackCache) rangeFixes(ctx context.Context, deviceID string, from, to time.Time) ([]fix, error) {
	var out []fix
	for d := dayStart(from); d.Before(to); d = d.AddDate(0, 0, 1) {
		day, err := c.dayFixes(ctx, deviceID, d)
		if err != nil {
			return nil, err
		}
		for _, f := range day {
			if !f.T.Before(from) && f.T.Before(to) {
				out = append(out, f)
			}
		}
	}
	return out, nil
}

func (c *trackCache) dayFixes(ctx context.Context, deviceID string, day time.Time) ([]fix, error) {
	key := deviceID + "|" + day.Format("2006-01-02")
	c.mu.Lock()
	if e, ok := c.m[key]; ok && time.Now().Before(e.expiry) {
		c.mu.Unlock()
		return e.fixes, nil
	}
	c.mu.Unlock()

	// One VSS call for the whole WIB day.
	pts, err := c.vss.Track(ctx, deviceID, day, day.AddDate(0, 0, 1))
	if err != nil {
		return nil, err
	}
	fixes := make([]fix, 0, len(pts))
	for _, p := range pts {
		t := p.Time
		fixes = append(fixes, fix{Lat: p.Lat, Lon: p.Lon, T: time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, wib)})
	}
	c.mu.Lock()
	c.m[key] = cacheEntry{fixes: fixes, expiry: time.Now().Add(trackTTL)}
	for k, e := range c.m { // opportunistic prune so the map can't grow unbounded
		if time.Now().After(e.expiry) {
			delete(c.m, k)
		}
	}
	c.mu.Unlock()
	return fixes, nil
}

// dayStart is midnight (WIB) of the given time.
func dayStart(t time.Time) time.Time {
	t = t.In(wib)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, wib)
}
