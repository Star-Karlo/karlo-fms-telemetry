package migrate

import (
	"context"
	"encoding/json"
	"testing"
)

// Covers the three source shapes seen in the exports:
//   - plain with timeReceived (2021/2025/2026)
//   - plain with only createdAt (2023/2024)
//   - MongoDB Extended JSON ($date/$oid)
func TestRecToRowFormats(t *testing.T) {
	const raw = `[
	  {"imeiTracker":"111","latitude":-6.1,"longitude":106.8,"speed":5,"bearing":90,
	   "timeReceived":"2025-03-04T05:06:07.000Z","createdAt":"2025-01-01T00:00:00.000Z","truck":"abc"},
	  {"imeiTracker":"222","latitude":-6.2,"longitude":106.9,
	   "createdAt":"2024-01-01T23:59:58.000Z","truck":"def"},
	  {"imeiTracker":"333","latitude":-6.3,"longitude":107.0,
	   "timeReceived":{"$date":"2026-01-12T00:00:11.997Z"},
	   "createdAt":{"$date":"2026-01-12T00:00:00.000Z"},"truck":{"$oid":"6290f10a"}},
	  {"imeiTracker":"","latitude":0,"longitude":0}
	]`
	var recs []legacyRec
	if err := json.Unmarshal([]byte(raw), &recs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	m := &Migrator{} // Geo nil => skip geocoding
	want := []struct {
		date string
		ok   bool
	}{
		{"2025-03-04", true}, // uses timeReceived
		{"2024-01-01", true}, // falls back to createdAt
		{"2026-01-12", true}, // extended-json timeReceived
		{"", false},          // no imei => skipped
	}
	for i := range recs {
		row, date, ok := m.recToRow(context.Background(), &recs[i])
		if ok != want[i].ok || (ok && date != want[i].date) {
			t.Errorf("rec %d: got date=%q ok=%v, want date=%q ok=%v", i, date, ok, want[i].date, want[i].ok)
		}
		if ok && row.IMEI == "" {
			t.Errorf("rec %d: empty imei on ok row", i)
		}
	}
	// truck should land in metadata for the extended record.
	if _, d, _ := m.recToRow(context.Background(), &recs[2]); d == "2026-01-12" {
		row, _, _ := m.recToRow(context.Background(), &recs[2])
		if want := `"truck":"6290f10a"`; !contains(row.Metadata, want) {
			t.Errorf("extended truck oid not in metadata: %s", row.Metadata)
		}
	}
}

// errorDesc appears as both ["NORMAL"] and "NORMAL" across the exports; neither
// must fail the record.
func TestErrorDescStringOrArray(t *testing.T) {
	for _, raw := range []string{
		`{"imeiTracker":"1","latitude":-6,"longitude":106,"timeReceived":"2025-01-01T00:00:00Z","errorDesc":["NORMAL"]}`,
		`{"imeiTracker":"2","latitude":-6,"longitude":106,"timeReceived":"2025-01-01T00:00:00Z","errorDesc":"NORMAL"}`,
	} {
		var r legacyRec
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			t.Fatalf("unmarshal failed on %s: %v", raw, err)
		}
		if len(r.ErrorDesc) != 1 || r.ErrorDesc[0] != "NORMAL" {
			t.Errorf("errorDesc = %v, want [NORMAL] for %s", r.ErrorDesc, raw)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
