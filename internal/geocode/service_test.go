package geocode

import "testing"

// A point outside every polygon still has to come back with a usable region.
// Pelabuhan Tanjung Buton is the case this exists for: the quay sits a few
// hundred metres beyond the Mengkapan boundary, so containment misses, the
// chain is snapped from the nearest polygon, and the result must SAY it was
// snapped rather than pass a guess off as a containment hit.
func TestResultCarriesSnappedAdmin(t *testing.T) {
	e := entry{Found: true, Place: Place{
		Kelurahan: "Mengkapan", Kecamatan: "Sungai Apit",
		Kabupaten: "Siak", Provinsi: "Riau",
		Street:      "Jalan Akses Penyebrangan Pelabuhan Mengkapan",
		AdminApprox: true, AdminDistM: 412.5,
	}}
	r := result(0.9661, 102.2531, e, false)

	if !r.AdminApprox {
		t.Error("AdminApprox = false, want true: a snapped chain must not look like a containment hit")
	}
	if r.AdminDistM != 412.5 {
		t.Errorf("AdminDistM = %v, want 412.5", r.AdminDistM)
	}
	if r.Kecamatan != "Sungai Apit" || r.Provinsi != "Riau" {
		t.Errorf("admin chain lost: %+v", r)
	}
	// A regency, not a city, so it belongs in Kabupaten and Kota stays empty.
	if r.Kabupaten != "Siak" || r.Kota != "" {
		t.Errorf("Kabupaten = %q, Kota = %q, want \"Siak\" and \"\"", r.Kabupaten, r.Kota)
	}
	if r.Jalan != "Jalan Akses Penyebrangan Pelabuhan Mengkapan" {
		t.Errorf("Jalan = %q", r.Jalan)
	}
}

// The ordinary case must stay clean: a contained point reports no approximation
// and no distance, so "admin_approx" is a real signal rather than noise.
func TestResultExactMatchIsNotApprox(t *testing.T) {
	e := entry{Found: true, Place: Place{
		Kelurahan: "Tambak Sarioso", Kecamatan: "Asemrowo",
		Kabupaten: "Kota Surabaya", Provinsi: "Jawa Timur",
	}}
	r := result(-7.2522, 112.6866, e, true)

	if r.AdminApprox {
		t.Error("AdminApprox = true for a contained point")
	}
	if r.AdminDistM != 0 {
		t.Errorf("AdminDistM = %v, want 0", r.AdminDistM)
	}
	if !r.Cached {
		t.Error("Cached flag lost")
	}
	// "Kota X" is a city: it moves to Kota and Kabupaten is left empty.
	if r.Kota != "Surabaya" || r.Kabupaten != "" {
		t.Errorf("Kota = %q, Kabupaten = %q, want \"Surabaya\" and \"\"", r.Kota, r.Kabupaten)
	}
}

// Nothing found means nothing claimed — no region, and no approximation flag
// implying one was attempted and trusted.
func TestResultNotFoundClaimsNothing(t *testing.T) {
	r := result(-5.0, 110.0, entry{Found: false}, false)

	if r.Found {
		t.Error("Found = true for an empty entry")
	}
	if r.AdminApprox || r.AdminDistM != 0 {
		t.Errorf("not-found carried approximation: approx=%v dist=%v", r.AdminApprox, r.AdminDistM)
	}
	if r.Provinsi != "" || r.Kecamatan != "" || r.Jalan != "" {
		t.Errorf("not-found carried location data: %+v", r)
	}
}
