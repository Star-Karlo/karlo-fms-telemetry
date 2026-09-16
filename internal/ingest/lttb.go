package ingest

import (
	"math"
	"time"
)

// downsampleLTTB reduces points to ~threshold using the Largest-Triangle-
// Three-Buckets algorithm, driven on latitude (adequate for map tracks).
// points MUST be sorted ascending by their "time" key. Returns points
// unchanged when len <= threshold. Falls back to uniform stride sampling when
// latitude isn't part of the projection.
//
// Used for both tiers: the cold segment from Athena and, since the move to
// plain PostgreSQL, the hot segment too (see hotLTTB in read.go).
func downsampleLTTB(points []map[string]any, threshold int) []map[string]any {
	n := len(points)
	if threshold < 3 || n <= threshold {
		return points
	}

	xs := make([]float64, n)
	ys := make([]float64, n)
	haveLat := true
	for i, p := range points {
		if t, ok := p["time"].(time.Time); ok {
			xs[i] = float64(t.UnixNano())
		}
		if y, ok := latOf(p); ok {
			ys[i] = y
		} else {
			haveLat = false
		}
	}
	if !haveLat {
		return uniformStride(points, threshold)
	}

	sampled := make([]map[string]any, 0, threshold)
	sampled = append(sampled, points[0]) // always keep the first point

	bucketSize := float64(n-2) / float64(threshold-2)
	a := 0 // index of the previously selected point
	for i := 0; i < threshold-2; i++ {
		// Average point of the next bucket.
		start := int(float64(i+1)*bucketSize) + 1
		end := int(float64(i+2)*bucketSize) + 1
		if end > n {
			end = n
		}
		var avgX, avgY float64
		cnt := float64(end - start)
		for j := start; j < end; j++ {
			avgX += xs[j]
			avgY += ys[j]
		}
		if cnt > 0 {
			avgX /= cnt
			avgY /= cnt
		}

		// Point in the current bucket forming the largest triangle with a and avg.
		rStart := int(float64(i)*bucketSize) + 1
		rEnd := int(float64(i+1)*bucketSize) + 1
		maxArea := -1.0
		next := rStart
		for j := rStart; j < rEnd && j < n; j++ {
			area := math.Abs((xs[a]-avgX)*(ys[j]-ys[a]) - (xs[a]-xs[j])*(avgY-ys[a]))
			if area > maxArea {
				maxArea = area
				next = j
			}
		}
		sampled = append(sampled, points[next])
		a = next
	}

	sampled = append(sampled, points[n-1]) // always keep the last point
	return sampled
}

// uniformStride keeps threshold evenly-spaced points (fallback when there's no
// latitude to drive LTTB with).
func uniformStride(points []map[string]any, threshold int) []map[string]any {
	n := len(points)
	if threshold < 2 || n <= threshold {
		return points
	}
	out := make([]map[string]any, 0, threshold)
	step := float64(n-1) / float64(threshold-1)
	for i := 0; i < threshold; i++ {
		out = append(out, points[int(float64(i)*step)])
	}
	return out
}

// latOf extracts a latitude value from a projected row, tolerating the various
// numeric shapes pgx / conversion can produce.
func latOf(p map[string]any) (float64, bool) {
	switch v := p["lat"].(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case *float64:
		if v != nil {
			return *v, true
		}
	}
	return 0, false
}
