package geocode

// One spelling per province, shared with the rest of the platform.
//
// The boundary data is COD-AB, whose ADM1 names are title-cased mechanically:
// it calls the capital "Dki Jakarta", which is wrong as an abbreviation and
// reads as a typo in an address. Master data spells its provinces differently
// again ("DI YOGYAKARTA"), and TMS matches our strings against that list, so a
// difference of one letter silently fails to match a province.
//
// The names below are the agreed form and the one this service returns: the
// long form of an abbreviation, ordinary title case, no ALL CAPS. Anything the
// data already spells this way passes through untouched, so this map holds
// only the exceptions — currently the two provinces whose names are
// abbreviations.
//
// Kabupaten/kota names need no such table: all 519 are plain title case in the
// source. The four provinces created by the 2022 Papua split are absent from
// this vintage of COD-AB (34 provinces, not 38) — a point there resolves to
// Papua or Papua Barat, which is a data refresh, not a spelling fix.
var provinceCanon = map[string]string{
	"dki jakarta":                   "DKI Jakarta",
	"di yogyakarta":                 "Daerah Istimewa Yogyakarta",
	"diy":                           "Daerah Istimewa Yogyakarta",
	"daerah istimewa yogyakarta":    "Daerah Istimewa Yogyakarta",
	"d.i. yogyakarta":               "Daerah Istimewa Yogyakarta",
	"daerah khusus ibukota jakarta": "DKI Jakarta",
}

// canonProvince returns the agreed spelling for a province name, or the name
// unchanged when it is not one of the exceptions.
func canonProvince(name string) string {
	if name == "" {
		return name
	}
	if c, ok := provinceCanon[lowerASCII(name)]; ok {
		return c
	}
	return name
}

// lowerASCII lower-cases without pulling in unicode tables: these names are
// ASCII, and a locale-aware fold would be a surprise in a lookup key.
func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}
