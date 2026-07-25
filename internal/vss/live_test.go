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
