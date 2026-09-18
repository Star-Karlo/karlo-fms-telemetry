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
