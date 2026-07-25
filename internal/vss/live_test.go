package vss

import (
	"context"
	"os"
	"testing"
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
