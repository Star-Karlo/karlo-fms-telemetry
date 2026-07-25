package vss

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// The Gtrack VSS real-time protocol (API PDF §4). Everything is a JSON frame
// with an integer `action`; the server pushes on the same socket after we
// subscribe.
const (
	wsActionLogin     = 80000 // client -> server: authenticate the socket
	wsActionSubscribe = 80001 // client -> server: start receiving pushes
	wsActionHeartbeat = 80009 // client -> server: keepalive, every minute
	wsActionGPS       = 80003 // server -> client: position/status
	wsActionAlarm     = 80004 // server -> client: an alarm/event
	wsActionOnOff     = 80005 // server -> client: device online/offline
)

// Alarm is a normalized device event, decoded from an 80004 push.
type Alarm struct {
	DeviceID   string    // VSS deviceno
	AlarmID    string    // VSS event uuid, for dedup
	Code       int       // event type (§8.9 device event code)
	OccurredAt time.Time // device time of the event
	Lat, Lon   *float64  // where it fired, when the push carried a location
	Speed      *float64
	Raw        string // the original payload, kept for anything we did not model
}

// wsFrame is the envelope for a client->server instruction.
//
// action is a STRING on the wire ("80000"), not a number — the vendor's own
// examples quote it, and sending a bare int is silently ignored by the server.
type wsFrame struct {
	Action  string `json:"action"`
	Payload any    `json:"payload"`
}

// frame builds an outgoing frame, formatting the numeric action as the string
// the server expects.
func frame(action int, payload any) wsFrame {
	return wsFrame{Action: strconv.Itoa(action), Payload: payload}
}

// SubscribeAlarms holds a live subscription to the VSS real-time feed and calls
// onAlarm for every alarm push until ctx is cancelled or the socket drops.
//
// It does NOT reconnect on its own — that is the caller's job (see the runner in
// internal/dashcam), so backoff and logging live in one place. One call = one
// connection attempt; it returns when the connection ends.
//
// The heartbeat matters: the vendor drops a socket that goes quiet, and without
// the minute-ly 80009 the feed simply stops with no error until the next read.
func (c *Client) SubscribeAlarms(ctx context.Context, onAlarm func(Alarm)) error {
	if !c.cfg.Enabled() {
		return ErrNotConfigured
	}
	// The WS handshake needs BOTH the http token and the pid, and the pid comes
	// only from a fresh login — so force one and read the pid out.
	if _, err := c.Token(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	token, pid := c.token, c.pid
	c.mu.Unlock()
	if pid == "" {
		return fmt.Errorf("vss ws: no pid from login")
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.wsURL(), nil)
	if err != nil {
		return fmt.Errorf("vss ws dial: %w", err)
	}
	defer conn.Close()

	// Login on the socket, then subscribe. The server does not reply to
	// subscribe with data; pushes just start arriving.
	if err := conn.WriteJSON(frame(wsActionLogin, map[string]string{
		"username": c.cfg.Username, "pid": pid, "token": token,
	})); err != nil {
		return fmt.Errorf("vss ws login: %w", err)
	}
	if err := conn.WriteJSON(frame(wsActionSubscribe, "")); err != nil {
		return fmt.Errorf("vss ws subscribe: %w", err)
	}

	// Heartbeat in the background. Stops when the connection closes (writes then
	// error) or ctx ends.
	hbCtx, cancelHB := context.WithCancel(ctx)
	defer cancelHB()
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				_ = conn.WriteJSON(frame(wsActionHeartbeat, map[string]string{
					"username": c.cfg.Username, "token": token,
				}))
			}
		}
	}()

	// Close the socket when ctx ends so the blocking read below returns.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("vss ws read: %w", err)
		}
		if a, ok := parseAlarm(data); ok {
			onAlarm(a)
		}
	}
}

// wsURL builds the WebSocket URL from the http base. The vendor serves ws on
// 36300 (and wss on 36301); we swap the http host's port for the ws port and
// the scheme for ws/wss accordingly.
func (c *Client) wsURL() string {
	base := strings.TrimRight(c.cfg.BaseURL, "/")
	u, err := url.Parse(base)
	if err != nil {
		// Fall back to a best-effort string swap; DialContext will error clearly
		// if this is wrong.
		return strings.Replace(base, "http", "ws", 1) + ":36300/ws"
	}
	host := u.Hostname()
	if u.Scheme == "https" {
		return "wss://" + host + ":36301/ws"
	}
	return "ws://" + host + ":36300/ws"
}

// parseAlarm decodes an 80004 push into an Alarm. Returns false for any other
// action (GPS, online/offline, login acks) so the read loop ignores them.
//
// The payload nests the event under payload.payload and the position under
// payload.location, with lat/lon as strings — hence the lenient decode.
func parseAlarm(data []byte) (Alarm, bool) {
	var head struct {
		Action  any `json:"action"` // string on the wire; number-tolerant via toInt
		Payload struct {
			DeviceID string `json:"deviceID"`
			AlarmID  string `json:"alarmID"`
			Payload  struct {
				DTU string `json:"dtu"`
				EC  any    `json:"ec"`
			} `json:"payload"`
			Location struct {
				Lat   any `json:"latitude"`
				Lon   any `json:"longitude"`
				Speed any `json:"speed"`
			} `json:"location"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(data, &head); err != nil || toInt(head.Action) != wsActionAlarm {
		return Alarm{}, false
	}
	p := head.Payload
	a := Alarm{DeviceID: p.DeviceID, AlarmID: p.AlarmID, Code: toInt(p.Payload.EC), Raw: string(data)}
	if t, err := time.Parse("2006-01-02 15:04:05", p.Payload.DTU); err == nil {
		a.OccurredAt = t
	} else {
		a.OccurredAt = time.Now()
	}
	if lat, ok := toFloat(p.Location.Lat); ok {
		if lon, ok2 := toFloat(p.Location.Lon); ok2 {
			a.Lat, a.Lon = &lat, &lon
		}
	}
	if spd, ok := toFloat(p.Location.Speed); ok {
		a.Speed = &spd
	}
	return a, true
}

func toInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(n))
		return i
	}
	return 0
}
