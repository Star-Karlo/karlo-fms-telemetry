package dashcam

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/karlo/fms/internal/vss"
	"github.com/karlo/fms/internal/vss/vsstest"
)

type uploaded struct{ bucket, key string }

// alarmHandler wires a handler to the fake platform with both seams replaced:
// no events are stored, uploads are recorded instead of sent.
func alarmHandler(t *testing.T) (*Handler, *vsstest.Fake, string, *[]uploaded) {
	t.Helper()
	fake := vsstest.New("D1", "D2")
	srv := httptest.NewServer(fake.Handler())
	t.Cleanup(srv.Close)
	h := NewHandler(vss.New(vss.Config{BaseURL: srv.URL, Username: "u", Password: "p"}),
		func(*http.Request) bool { return true }, "default-bucket", []string{"siba-bucket"}, nil)
	var ups []uploaded
	h.upload = func(_ context.Context, bucket, key, _ string) error {
		ups = append(ups, uploaded{bucket, key})
		return nil
	}
	h.alarmFiles = storedAlarms(nil)
	return h, fake, srv.URL, &ups
}

func storedAlarms(stored map[string]storedAlarm) func(context.Context, string) (string, []vss.EvidenceFile, error) {
	return func(_ context.Context, id string) (string, []vss.EvidenceFile, error) {
		a, ok := stored[id]
		if !ok {
			return "", nil, errors.New("no rows")
		}
		return a.device, a.files, nil
	}
}

type storedAlarm struct {
	device string
	files  []vss.EvidenceFile
}

func post(t *testing.T, h *Handler, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	h.Routes(r)
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(raw))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// waitJob polls the job until it is done or failed.
func waitJob(t *testing.T, h *Handler, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if w.Code != http.StatusAccepted {
		t.Fatalf("start: %d %s", w.Code, w.Body.String())
	}
	var started map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &started)
	for i := 0; i < 100; i++ {
		res := do(t, h, http.MethodGet, "/v1/dashcams/downloads/"+started["job_id"])
		var st map[string]any
		_ = json.Unmarshal(res.Body.Bytes(), &st)
		if st["status"] == "done" || st["status"] == "failed" {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("job did not finish")
	return nil
}

func TestAlarmDownloadCopiesStoredDriverClipWithoutSDCard(t *testing.T) {
	h, fake, base, ups := alarmHandler(t)
	h.alarmFiles = storedAlarms(map[string]storedAlarm{"E1": {device: "D1", files: []vss.EvidenceFile{
		{FileType: 4, Channel: 2, DownURL: base + "/files/E1.jpg"},
		{FileType: 2, Channel: 0, DownURL: base + "/files/E1_ch1.mp4"}, // channel 0 = road camera
		{FileType: 2, Channel: 2, DownURL: base + "/files/E1_ch2.mp4"},
	}}})
	st := waitJob(t, h, post(t, h, "/v1/dashcams/D1/download", DownloadRequest{
		Channel: 2, Start: "2026-11-10 08:00:00", Stop: "2026-11-10 08:00:10", AlarmID: "E1", Bucket: "siba-bucket"}))

	if st["status"] != "done" {
		t.Fatalf("job: %v", st)
	}
	if got := fake.Fetched(); len(got) != 1 || got[0] != "/files/E1_ch2.mp4" {
		t.Fatalf("fetched %v, want only the driver-camera clip", got)
	}
	if st["bucket"] != "siba-bucket" || st["s3_key"] != "video/D1/2026-11-10/ch2_080000_E1.mp4" {
		t.Fatalf("stored at %v/%v", st["bucket"], st["s3_key"])
	}
	if st["size_bytes"] != float64(len(vsstest.Clip)) {
		t.Fatalf("size %v, want %d", st["size_bytes"], len(vsstest.Clip))
	}
	if len(*ups) != 1 || (*ups)[0].bucket != "siba-bucket" {
		t.Fatalf("uploads %v", *ups)
	}
	if fake.SDPulls.Load() != 0 || fake.EvidenceSearch.Load() != 0 {
		t.Fatalf("SD pulls %d, searches %d; want none", fake.SDPulls.Load(), fake.EvidenceSearch.Load())
	}
}

func TestAlarmDownloadSearchesOnceWhenStoredLinkFails(t *testing.T) {
	at := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC) // first event of the fake's day
	id := vsstest.EventID("D1", at, 0)
	h, fake, base, _ := alarmHandler(t)
	h.alarmFiles = storedAlarms(map[string]storedAlarm{
		id: {device: "D1", files: []vss.EvidenceFile{{FileType: 2, Channel: 1, DownURL: base + "/files/expired/x.mp4"}}},
	})
	st := waitJob(t, h, post(t, h, "/v1/dashcams/D1/download", DownloadRequest{
		Channel: 1, Start: "2026-11-10 08:00:00", Stop: "2026-11-10 08:00:10", AlarmID: id}))

	if st["status"] != "done" || st["bucket"] != "default-bucket" {
		t.Fatalf("job: %v", st)
	}
	if got := fake.Fetched(); len(got) != 1 || got[0] != "/files/"+id+"_ch1.mp4" {
		t.Fatalf("fetched %v", got)
	}
	if fake.EvidenceSearch.Load() != 1 || fake.SDPulls.Load() != 0 {
		t.Fatalf("searches %d, SD pulls %d", fake.EvidenceSearch.Load(), fake.SDPulls.Load())
	}
}

func TestAlarmDownloadWithoutClipFailsWithoutSDCard(t *testing.T) {
	h, fake, _, ups := alarmHandler(t)
	// 10:00 is the fake's picture-only event.
	id := vsstest.EventID("D1", time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC), 4)
	st := waitJob(t, h, post(t, h, "/v1/dashcams/D1/download", DownloadRequest{
		Channel: 1, Start: "2026-11-10 10:00:00", Stop: "2026-11-10 10:00:10", AlarmID: id}))

	if st["status"] != "failed" || st["error"] != "no clip on server" {
		t.Fatalf("job: %v", st)
	}
	if fake.SDPulls.Load() != 0 || len(*ups) != 0 {
		t.Fatalf("SD pulls %d, uploads %d; want none", fake.SDPulls.Load(), len(*ups))
	}
}

func TestAlarmDownloadRejectsAnotherCamerasEvent(t *testing.T) {
	h, _, base, ups := alarmHandler(t)
	h.alarmFiles = storedAlarms(map[string]storedAlarm{
		"E2": {device: "D2", files: []vss.EvidenceFile{{FileType: 2, Channel: 1, DownURL: base + "/files/E2_ch1.mp4"}}},
	})
	st := waitJob(t, h, post(t, h, "/v1/dashcams/D1/download", DownloadRequest{
		Channel: 1, Start: "2026-11-10 08:00:00", Stop: "2026-11-10 08:00:10", AlarmID: "E2"}))
	if st["status"] != "failed" || len(*ups) != 0 {
		t.Fatalf("job: %v, uploads %v", st, *ups)
	}
}

func TestDownloadRejectsBucketNotOnTheList(t *testing.T) {
	h, _, _, _ := alarmHandler(t)
	w := post(t, h, "/v1/dashcams/D1/download", DownloadRequest{
		Channel: 1, Start: "2026-11-10 08:00:00", Stop: "2026-11-10 08:00:10", AlarmID: "E1", Bucket: "someone-elses"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %s", w.Code, w.Body.String())
	}
}

func TestVideoFilesFromRaw(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"alarm_type": 124, "files": []vss.EvidenceFile{
		{FileType: 4, Channel: 1, DownURL: "https://vss/pic.jpg"},
		{FileType: 2, Channel: 0, StartTime: "2026-11-10 07:59:55", StopTime: "2026-11-10 08:00:05", DownURL: "https://vss/a.mp4?token=t"},
		{FileType: 1, Channel: 2, DownURL: "https://vss/b.mp4?token=t"},
		{FileType: 2, Channel: 2}, // no link: not downloadable
	}})
	got := videoFiles(raw)
	want := []AlarmFile{
		{Channel: 1, FileType: 2, Start: "2026-11-10 07:59:55", Stop: "2026-11-10 08:00:05"},
		{Channel: 2, FileType: 1},
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("files %+v, want %+v", got, want)
	}
	out, _ := json.Marshal(got)
	if strings.Contains(string(out), "token") {
		t.Fatalf("download link leaked: %s", out)
	}
	if f := videoFiles([]byte(`{"files":[{"FileType":3,"DownURL":"x"}]}`)); f == nil || len(f) != 0 {
		t.Fatalf("picture-only event: %+v, want empty list", f)
	}
}
