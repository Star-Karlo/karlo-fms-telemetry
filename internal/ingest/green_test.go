package ingest

import "testing"

func TestIoFloat(t *testing.T) {
	cases := []struct {
		v    any
		want float64
		ok   bool
	}{
		{float64(2), 2, true},
		{int(3), 3, true},
		{"0.45", 0.45, true},
		{"45", 45, true},
		{"nope", 0, false},
		{nil, 0, false},
		{true, 0, false},
	}
	for _, c := range cases {
		got, ok := ioFloat(c.v)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("ioFloat(%v) = %v,%v want %v,%v", c.v, got, ok, c.want, c.ok)
		}
	}
}

func TestFirstNum_CandidateKeys(t *testing.T) {
	// Numeric AVL id key is found; a named alias also works; neither present -> false.
	m := map[string]any{"253": float64(2), "254": "0.6"}
	if v, ok := firstNum(m, greenTypeKeys); !ok || v != 2 {
		t.Errorf("type from '253' = %v,%v", v, ok)
	}
	if v, ok := firstNum(m, greenValueKeys); !ok || v != 0.6 {
		t.Errorf("value from '254' = %v,%v", v, ok)
	}
	named := map[string]any{"green_driving_type": 1}
	if v, ok := firstNum(named, greenTypeKeys); !ok || v != 1 {
		t.Errorf("type from named alias = %v,%v", v, ok)
	}
	if _, ok := firstNum(map[string]any{"speed": 40}, greenTypeKeys); ok {
		t.Error("no green key present should be false")
	}
}
