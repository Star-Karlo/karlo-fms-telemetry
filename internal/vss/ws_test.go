package vss

import "testing"

// The 80004 payload shape and string-typed action are taken verbatim from the
// vendor doc §4.6, including the quoted "action" and string lat/lon.
const alarmPush = `{
  "action": "80004",
  "payload": {
    "deviceID": "300342",
    "alarmID": "60D27AA2-E2E7495A-B77AB878-D58861F2",
    "payload": { "det": {"ch":"1"}, "dtu": "2018-01-12 09:34:20", "ec": "1", "et": "", "st": "2018-01-12 09:34:20" },
    "location": {
      "mode": "1", "dtu": "2021-01-25 17:31:59", "direct": "0", "speed": "19",
      "longitude": "114.111992", "latitude": "22.5380726"
    }
  }
}`

func TestParseAlarm(t *testing.T) {
	a, ok := parseAlarm([]byte(alarmPush))
	if !ok {
		t.Fatal("an 80004 push with a STRING action must parse; if this fails the wire format is int-only in the code")
	}
	if a.DeviceID != "300342" {
		t.Errorf("device %q", a.DeviceID)
	}
	if a.AlarmID != "60D27AA2-E2E7495A-B77AB878-D58861F2" {
		t.Errorf("alarm id %q", a.AlarmID)
	}
	if a.Code != 1 {
		t.Errorf("code %d, want 1", a.Code)
	}
	if a.OccurredAt.Year() != 2018 || a.OccurredAt.Hour() != 9 {
		t.Errorf("occurred_at %v (from the event dtu, not the location dtu)", a.OccurredAt)
	}
	if a.Lat == nil || a.Lon == nil || *a.Lat != 22.5380726 || *a.Lon != 114.111992 {
		t.Errorf("coordinate not parsed from string lat/lon: %+v %+v", a.Lat, a.Lon)
	}
	if a.Speed == nil || *a.Speed != 19 {
		t.Errorf("speed %+v", a.Speed)
	}
}

func TestParseAlarmIgnoresNonAlarmActions(t *testing.T) {
	// A GPS push (80003) must NOT be treated as an alarm.
	for _, push := range []string{
		`{"action":"80003","payload":{"deviceID":"x"}}`,
		`{"action":"80005","payload":{"deviceID":"x","accessMode":5}}`,
		`{"action":"80000","payload":{"result":"success"}}`,
		`not json`,
	} {
		if _, ok := parseAlarm([]byte(push)); ok {
			t.Errorf("non-alarm push parsed as an alarm: %s", push)
		}
	}
}

func TestWSURLDerivation(t *testing.T) {
	cases := map[string]string{
		"http://vss.gtrack.co.id:9966":  "ws://vss.gtrack.co.id:36300/ws",
		"https://vss.example.com":       "wss://vss.example.com:36301/ws",
		"http://10.0.0.5:9966/":         "ws://10.0.0.5:36300/ws",
	}
	for base, want := range cases {
		c := New(Config{BaseURL: base, Username: "u", Password: "p"})
		if got := c.wsURL(); got != want {
			t.Errorf("wsURL(%q) = %q, want %q", base, got, want)
		}
	}
}
