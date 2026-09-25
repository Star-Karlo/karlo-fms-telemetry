package geocode

import "testing"

func TestNormaliseAndLabel(t *testing.T) {
	for in, want := range map[string]string{"Kota Surabaya": "Surabaya", "Kab. Bekasi": "Bekasi", "  Kecamatan   Tanah Abang ": "Tanah Abang", "Cikarang Utara": "Cikarang Utara"} {
		if got := normalise(in); got != want {
			t.Errorf("normalise(%q) = %q, want %q", in, got, want)
		}
	}
	a := Address{Kelurahan: "Sukaresmi", Kecamatan: "Cikarang Selatan", Kabupaten: "Bekasi", Provinsi: "Jawa Barat"}
	if got := label("Jalan Bekasi Raya", a); got != "Jalan Bekasi Raya, Sukaresmi, Cikarang Selatan, Bekasi, Jawa Barat" {
		t.Errorf("label = %q", got)
	}
	// an area's own name is not repeated in its chain
	if got := label("Tanah Abang", Address{Kecamatan: "Tanah Abang", Kabupaten: "Kota Jakarta Pusat", Provinsi: "Dki Jakarta"}); got != "Tanah Abang, Kota Jakarta Pusat, Dki Jakarta" {
		t.Errorf("label = %q", got)
	}
}

func TestSortCandidates(t *testing.T) {
	c := []Candidate{{Kind: "road", Score: 1.0, Name: "Jalan Pekanbaru"}, {Kind: "kabupaten", Score: 1.0, Name: "Kota Pekanbaru"}, {Kind: "road", Score: 0.6}}
	sortCandidates(c, map[string]int{"kabupaten": 0, "kecamatan": 1, "kelurahan": 2, "road": 3})
	if c[0].Name != "Kota Pekanbaru" || c[2].Score != 0.6 {
		t.Errorf("order wrong: %+v", c)
	}
}

// The capital must not come back as "Dki Jakarta" (the boundary data's
// mechanical title case), and every spelling of Yogyakarta resolves to the
// one both consoles use — TMS matches these strings against master data, so
// a one-letter difference is a province that silently fails to match.
func TestProvinceSpellingIsCanonical(t *testing.T) {
	cases := map[string]string{
		"Dki Jakarta":                   "DKI Jakarta",
		"DKI JAKARTA":                   "DKI Jakarta",
		"Daerah Khusus Ibukota Jakarta": "DKI Jakarta",
		"DI YOGYAKARTA":                 "Daerah Istimewa Yogyakarta",
		"Di Yogyakarta":                 "Daerah Istimewa Yogyakarta",
		"Daerah Istimewa Yogyakarta":    "Daerah Istimewa Yogyakarta",
		"Jawa Tengah":                   "Jawa Tengah", // untouched
		"":                              "",
	}
	for in, want := range cases {
		if got := canonProvince(in); got != want {
			t.Errorf("canonProvince(%q) = %q, want %q", in, got, want)
		}
	}
}

// A caller pasting master data's spelling must find the same place: the query
// is folded to the canonical name before it is matched.
func TestSearchQueryAcceptsProvinceAliases(t *testing.T) {
	cases := map[string]string{
		"Jl. Malioboro, DI Yogyakarta":  "Jl. Malioboro, Daerah Istimewa Yogyakarta",
		"Sleman, DI YOGYAKARTA":         "Sleman, Daerah Istimewa Yogyakarta",
		"Gudang, Daerah Khusus Ibukota Jakarta": "Gudang, DKI Jakarta",
		"Jl. Sudirman, Jawa Tengah":     "Jl. Sudirman, Jawa Tengah", // untouched
	}
	for in, want := range cases {
		if got := canonProvinceIn(in); got != want {
			t.Errorf("canonProvinceIn(%q) = %q, want %q", in, got, want)
		}
	}
}
