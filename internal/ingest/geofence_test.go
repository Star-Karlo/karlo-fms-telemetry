package ingest

import (
	"math"
	"testing"
)

func TestHaversineMeters(t *testing.T) {
	// same point -> 0
	if d := haversineMeters(-6.2, 106.8, -6.2, 106.8); d > 0.001 {
		t.Errorf("same point = %v, want 0", d)
	}
	// ~1 deg latitude ~= 111.19 km
	if d := haversineMeters(0, 0, 1, 0); math.Abs(d-111195) > 500 {
		t.Errorf("1deg lat = %v, want ~111195", d)
	}
	// Jakarta warehouse vs truck ~7.3 km
	d := haversineMeters(-6.200, 106.800, -6.170, 106.860)
	if d < 6500 || d > 8000 {
		t.Errorf("jkt pair = %v, want ~7300", d)
	}
}
