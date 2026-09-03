// Package gmt talks to the GMT Teletech public API (api.gmteletech.com) to pull
// live vehicle positions. Auth is a Bearer API key. One endpoint carries the
// whole fleet's current status, so a single call per poll covers every device.
//
// Docs: https://api.gmteletech.com/docs/  (base https://api.gmteletech.com/v1)
package gmt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.gmteletech.com/v1"

// Config holds the connection settings. APIKey is the Bearer token; the key is a
// secret and must come from the environment (never committed).
type Config struct {
	BaseURL string
	APIKey  string
}

func (c Config) Enabled() bool { return strings.TrimSpace(c.APIKey) != "" }

// Client is a GMT API client, safe for concurrent use.
type Client struct {
	cfg Config
	hc  *http.Client
}

func New(cfg Config) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	return &Client{cfg: cfg, hc: &http.Client{Timeout: 30 * time.Second}}
}

func (c *Client) Enabled() bool { return c.cfg.Enabled() }

// VehicleStatus is the subset of GMT's VehicleStatusData we persist. GMT returns
// far more (harsh counts, trip sums, alerts); we keep the position core plus the
// bits FMS surfaces (fuel, temperature, doors) in metadata.
type VehicleStatus struct {
	IMEI           string  `json:"imei"`
	LicensePlate   string  `json:"licensePlate"`
	HullNo         string  `json:"hullNo"`
	Latitude       float64 `json:"latitude"`
	Longitude      float64 `json:"longitude"`
	Altitude       float64 `json:"altitude"`
	Speed          float64 `json:"speed"`
	Direction      float64 `json:"direction"`
	EngineOn       bool    `json:"engineOn"`
	MotionStatus   string  `json:"motionStatus"`
	LastPacket     string  `json:"lastPacket"`  // device fix time (string)
	LastReceive    string  `json:"lastReceive"` // platform receive time
	Battery        float64 `json:"battery"`
	SignalStrength float64 `json:"signalStrength"`
	FuelFiltered   float64 `json:"fuelFiltered"`
	FuelCapacity   float64 `json:"fuelCapacity"`
	Fuel           []any   `json:"fuel"`
	Temperature    []any   `json:"temperature"`
	Door1Status    int     `json:"door1Status"`
	Door2Status    int     `json:"door2Status"`
}

type statusesResponse struct {
	Message string          `json:"message"`
	Data    []VehicleStatus `json:"data"`
}

// VehicleStatuses returns the current status of every vehicle the API key can
// see (GET /vehicles/statuses).
func (c *Client) VehicleStatuses(ctx context.Context) ([]VehicleStatus, error) {
	url := strings.TrimRight(c.cfg.BaseURL, "/") + "/vehicles/statuses"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	req.Header.Set("Accept", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gmt /vehicles/statuses: http %d: %s", resp.StatusCode, snippet(body))
	}
	var out statusesResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("gmt /vehicles/statuses: decode: %w (body %s)", err, snippet(body))
	}
	return out.Data, nil
}

func snippet(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "…"
	}
	return string(b)
}
