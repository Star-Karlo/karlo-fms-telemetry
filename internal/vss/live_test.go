package vss

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestLiveServer runs the real client against a real VSS server when
// VSS_LIVE=1 and creds are in the env. Skipped in CI. Proves the shipping
// client — login, token cache, device list — works against gtrack, not a mock.
func TestLiveServer(t *testing.T) {
	if os.Getenv("VSS_LIVE") != "1" {
		t.Skip("set VSS_LIVE=1 with VSS_BASE_URL/USERNAME/PASSWORD to run")
	}
	c := New(Config{
		BaseURL:  os.Getenv("VSS_BASE_URL"),
		Username: os.Getenv("VSS_USERNAME"),
		Password: os.Getenv("VSS_PASSWORD"),
	})
	tok, err := c.Token(context.Background())
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	t.Logf("login OK, token len=%d", len(tok))

	devices, err := c.ListDevices(context.Background(), "")
	if err != nil {
		t.Fatalf("list devices: %v", err)
	}
	t.Logf("devices: %d", len(devices))
	for _, d := range devices {
		t.Logf("  %s %q type=%s ch=%d online=%v", d.DeviceNo, d.DeviceName, d.DeviceType, d.VideoChannels, d.Online())
	}
	if len(devices) == 0 {
		t.Fatal("expected at least one device")
	}
}

// TestLiveWS opens the real WebSocket and subscribes, proving the handshake
// (dial -> login -> subscribe -> heartbeat) works against gtrack. The device is
// likely offline so no alarms arrive; success is the socket staying up for the
// window without a handshake error.
func TestLiveWS(t *testing.T) {
	if os.Getenv("VSS_LIVE") != "1" {
		t.Skip("set VSS_LIVE=1 to run")
	}
	c := New(Config{
		BaseURL:  os.Getenv("VSS_BASE_URL"),
		Username: os.Getenv("VSS_USERNAME"),
		Password: os.Getenv("VSS_PASSWORD"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	got := 0
	err := c.SubscribeAlarms(ctx, func(a Alarm) {
		got++
		t.Logf("alarm: device=%s code=%d id=%s", a.DeviceID, a.Code, a.AlarmID)
	})
	// A clean deadline is success — the handshake held and the read loop ran
	// until ctx expired. Any other error means the handshake or socket failed.
	if err != nil && ctx.Err() == nil {
		t.Fatalf("ws subscription failed before the deadline: %v", err)
	}
	t.Logf("held the socket for the window; alarms received: %d", got)
}

// TestLiveRawDeviceResponses prints the full raw device-list and status
// responses for one camera, to see whether VSS reports SIM data usage. If any
// data-usage field appears, the optional SIM-data phase goes ahead.
//
//	VSS_LIVE=1 VSS_LIVE_DEVICE=<device no> go test -run TestLiveRawDeviceResponses -v ./internal/vss/
func TestLiveRawDeviceResponses(t *testing.T) {
	device := os.Getenv("VSS_LIVE_DEVICE")
	if os.Getenv("VSS_LIVE") != "1" || device == "" {
		t.Skip("set VSS_LIVE=1 and VSS_LIVE_DEVICE with VSS_BASE_URL/USERNAME/PASSWORD to run")
	}
	c := New(Config{
		BaseURL:  os.Getenv("VSS_BASE_URL"),
		Username: os.Getenv("VSS_USERNAME"),
		Password: os.Getenv("VSS_PASSWORD"),
	})
	ctx := context.Background()
	list, err := c.post(ctx, "/vss/vehicle/findAll.action", map[string]any{"pageNum": 1, "pageCount": -1, "keyword": device})
	if err != nil {
		t.Fatalf("device list: %v", err)
	}
	t.Logf("findAll.action raw:\n%s", list)
	status, err := c.post(ctx, "/vss/onoffline/queryMoreDeviceStatus.action", map[string]any{"deviceID": device})
	if err != nil {
		t.Fatalf("device status: %v", err)
	}
	t.Logf("queryMoreDeviceStatus.action raw:\n%s", status)
}

// TestLiveEvidenceCounts logs how many events the evidence search returns per
// day for one camera, and how many have a video clip.
//
//	VSS_LIVE=1 VSS_LIVE_DEVICE=<device no> go test -run TestLiveEvidenceCounts -v ./internal/vss/
func TestLiveEvidenceCounts(t *testing.T) {
	device := os.Getenv("VSS_LIVE_DEVICE")
	if os.Getenv("VSS_LIVE") != "1" || device == "" {
		t.Skip("set VSS_LIVE=1 and VSS_LIVE_DEVICE with VSS_BASE_URL/USERNAME/PASSWORD to run")
	}
	c := New(Config{
		BaseURL:  os.Getenv("VSS_BASE_URL"),
		Username: os.Getenv("VSS_USERNAME"),
		Password: os.Getenv("VSS_PASSWORD"),
	})
	now := time.Now().In(time.FixedZone("WIB", 7*3600))
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	for d := 0; d < 3; d++ {
		day := today.AddDate(0, 0, -d)
		evs, err := c.SearchAlarmEvidence(context.Background(), device, day, day.Add(24*time.Hour), "")
		if err != nil {
			t.Errorf("%s: %v", day.Format("2006-01-02"), err)
			continue
		}
		video := 0
		for _, e := range evs {
			for _, f := range e.Files {
				if (f.FileType == 1 || f.FileType == 2) && f.DownURL != "" {
					video++
					break
				}
			}
		}
		t.Logf("%s: %d events, %d with video", day.Format("2006-01-02"), len(evs), video)
	}
}
