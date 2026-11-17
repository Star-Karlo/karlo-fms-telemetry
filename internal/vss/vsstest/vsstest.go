// Package vsstest is a fake Gtrack VSS camera platform for tests and local
// development (cmd/fakevss). It never touches the vendor's production account.
//
// Every device has two cameras and the same daily event schedule (see
// schedule), so event ids are stable whatever window a caller searches:
// road + driver clips, events 30 s apart, two events in the same second, an
// event with only a picture, and one whose clip "uploads" only on the second
// search that sees it. SD-card pulls always answer "no data" and are counted,
// so an accidental one shows up as a failure.
package vsstest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Clip is the body served for every clip file.
var Clip = []byte("fake-mp4-clip-bytes")

// Fake is the platform. Devices are device numbers; each has channels 1 and 2.
type Fake struct {
	Devices []string

	SDPulls        atomic.Int32 // /vss/webdownrecord/insert.action calls
	EvidenceSearch atomic.Int32 // /vss/record/evidenceToRetrieve.action calls

	mu      sync.Mutex
	seen    map[string]int // event id -> searches that returned it, for the late upload
	fetched []string       // clip file paths served, in order
}

// New returns a fake serving the given devices.
func New(devices ...string) *Fake {
	return &Fake{Devices: devices, seen: map[string]int{}}
}

// Fetched lists the clip file paths served so far.
func (f *Fake) Fetched() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.fetched...)
}

type event struct {
	offset   time.Duration // from local midnight
	code     int
	channels []int
	video    bool
	late     bool // clip appears only from the second search that returns it
}

// schedule is each device's day. 08:00:00 and 08:00:30 are 30 s apart on the
// driver camera; the two 09:15:10 events share a second.
var schedule = []event{
	{offset: 8 * time.Hour, code: 124, channels: []int{1, 2}, video: true},
	{offset: 8*time.Hour + 30*time.Second, code: 121, channels: []int{2}, video: true},
	{offset: 9*time.Hour + 15*time.Minute + 10*time.Second, code: 110, channels: []int{1}, video: true},
	{offset: 9*time.Hour + 15*time.Minute + 10*time.Second, code: 116, channels: []int{1}, video: true},
	{offset: 10 * time.Hour, code: 147, channels: []int{1}, video: false},
	{offset: 11 * time.Hour, code: 122, channels: []int{2}, video: true, late: true},
}

const layout = "2006-01-02 15:04:05"

// Handler serves the VSS endpoints the telemetry client uses.
func (f *Fake) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/vss/user/apiLogin.action", func(w http.ResponseWriter, _ *http.Request) {
		env(w, 10000, "Success", map[string]string{"token": "fake-token", "pid": "fake-pid"})
	})
	mux.HandleFunc("/vss/vehicle/findAll.action", func(w http.ResponseWriter, _ *http.Request) {
		list := []map[string]any{}
		for _, d := range f.Devices {
			list = append(list, map[string]any{"deviceno": d, "devicename": d, "accessmode": 5,
				"videoencodernumber": 2, "channelname": "Road;Driver"})
		}
		env(w, 10000, "Success", map[string]any{"dataList": list})
	})
	mux.HandleFunc("/vss/onoffline/queryMoreDeviceStatus.action", func(w http.ResponseWriter, _ *http.Request) {
		list := []map[string]any{}
		for _, d := range f.Devices {
			list = append(list, map[string]any{"deviceID": d, "accessMode": 5})
		}
		env(w, 10000, "Success", list)
	})
	mux.HandleFunc("/vss/record/evidenceToRetrieve.action", f.evidence)
	mux.HandleFunc("/vss/record/videoFileSearch.action", func(w http.ResponseWriter, _ *http.Request) {
		env(w, 10000, "Success", map[string]any{"files": []any{}})
	})
	mux.HandleFunc("/vss/track/getTrackList.action", func(w http.ResponseWriter, _ *http.Request) {
		env(w, 10000, "Success", []any{})
	})
	mux.HandleFunc("/vss/webdownrecord/insert.action", func(w http.ResponseWriter, _ *http.Request) {
		f.SDPulls.Add(1)
		env(w, 10007, "no data", nil)
	})
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/files/expired/") {
			http.Error(w, "token expired", http.StatusForbidden)
			return
		}
		f.mu.Lock()
		f.fetched = append(f.fetched, r.URL.Path)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(Clip)
	})
	return mux
}

func (f *Fake) evidence(w http.ResponseWriter, r *http.Request) {
	f.EvidenceSearch.Add(1)
	var body struct {
		Device string `json:"conditionName"`
		Start  string `json:"startTime"`
		End    string `json:"endTime"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	from, err1 := time.Parse(layout, body.Start)
	to, err2 := time.Parse(layout, body.End)
	if err1 != nil || err2 != nil || !slices.Contains(f.Devices, body.Device) {
		env(w, 10000, "Success", []any{})
		return
	}
	base := "http://" + r.Host
	rows := []map[string]any{}
	for day := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC); !day.After(to); day = day.AddDate(0, 0, 1) {
		for i, e := range schedule {
			at := day.Add(e.offset)
			if at.Before(from) || at.After(to) {
				continue
			}
			id := EventID(body.Device, at, i)
			rows = append(rows, map[string]any{
				"deviceID": body.Device, "alarmGuid": id, "alarmType": e.code,
				"alarmGps": "106.8227,-6.2088", "alarmTime": at.Format(layout),
				"alarmFile": f.files(base, id, at, e),
			})
		}
	}
	env(w, 10000, "Success", rows)
}

func (f *Fake) files(base, id string, at time.Time, e event) []map[string]any {
	out := []map[string]any{{"fileType": 4, "channel": e.channels[0], "downUrl": base + "/files/" + id + ".jpg"}}
	if !e.video {
		return out
	}
	if e.late {
		f.mu.Lock()
		f.seen[id]++
		first := f.seen[id] == 1
		f.mu.Unlock()
		if first {
			return out
		}
	}
	for _, ch := range e.channels {
		out = append(out, map[string]any{
			"fileType": 2, "channel": ch,
			"fileStartTime": at.Add(-5 * time.Second).Format(layout),
			"fileStopTime":  at.Add(5 * time.Second).Format(layout),
			"downUrl":       fmt.Sprintf("%s/files/%s_ch%d.mp4", base, id, ch),
		})
	}
	return out
}

// EventID is the stable id of a scheduled event.
func EventID(device string, at time.Time, index int) string {
	return fmt.Sprintf("%s-%s-%d", device, at.Format("20060102150405"), index)
}

// env writes the vendor envelope. Errors also arrive as HTTP 200, as on VSS.
func env(w http.ResponseWriter, status int, msg string, data any) {
	raw, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "msg": msg, "data": json.RawMessage(raw)})
}
