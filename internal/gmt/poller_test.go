package gmt

import (
	"testing"
	"time"
)

func TestNormalizeIMEI(t *testing.T) {
	cases := []struct{ in, want string }{
		{"353742378917173_1", "353742378917173"}, // the rajabesi form: strip _N
		{"353742378917173", "353742378917173"},   // already clean
		{"354002391634563_1", "354002391634563"},
		{"  353742378917173_1 ", "353742378917173"}, // leading digits only after trim of non-digits
		{"35374237891717", ""},                      // 14 digits → too short, rejected
		{"", ""},
		{"_1", ""},
	}
	for _, c := range cases {
		if got := normalizeIMEI(c.in); got != c.want {
			t.Errorf("normalizeIMEI(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseTime(t *testing.T) {
	def := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	// Unparseable / empty fall back to def; a good value parses.
	if got := parseTime("", def); !got.Equal(def) {
		t.Errorf("empty should fall back to def, got %v", got)
	}
	if got := parseTime("not-a-time", def); !got.Equal(def) {
		t.Errorf("garbage should fall back to def, got %v", got)
	}
	got := parseTime("2026-09-01 08:15:30", def)
	if got.Year() != 2026 || got.Month() != 9 || got.Day() != 1 || got.Hour() != 8 {
		t.Errorf("expected parsed 2026-09-01 08:15:30, got %v", got)
	}
}
