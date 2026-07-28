// Package vss talks to a Gtrack VSS platform — the server the dashcams (MDVRs)
// report to.
//
// THIS LIVES IN THE TELEMETRY SERVICE, not FMS. The architecture is: dashcam ->
// VSS -> this service -> FMS. Raw video/GPS/events stay on this side (the same
// layer as GPS ingest); FMS only ever consumes this service's API and never
// touches VSS. When Karlo builds its own VSS later, only this package's target
// changes — FMS is unaffected.
//
// THREE THINGS THE VENDOR API DOES THAT SHAPE THIS CLIENT:
//
//  1. THE TOKEN EXPIRES AFTER 30 MINUTES OF INACTIVITY and there is no refresh
//     call — you log in again. So the token is cached with an expiry and
//     re-acquired on demand, and a 10004/10023 ("not logged in" / "token
//     error") response triggers exactly one retry after a fresh login. Without
//     that retry every screen breaks after half an hour of quiet.
//
//  2. ERRORS COME BACK AS HTTP 200. The status lives in a JSON `status` field —
//     10000 is success, everything else is a failure with a human message. A
//     naive client that only checks the HTTP code treats every failure as a
//     successful empty response.
//
//  3. THE PASSWORD IS SENT AS AN MD5 HEX DIGEST, not the plaintext and not a
//     salted hash. That is the vendor's protocol and cannot be improved from
//     this side; the value is stored as a normal secret (SSM) and never logged.
package vss

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Vendor status codes worth naming. The full table is in the API PDF §8.1.
const (
	statusOK            = 10000
	statusNotLoggedIn   = 10004
	statusTokenError    = 10023
	statusDeviceOffline = 10043
	// statusTooFrequent is the per-API throttle ("Requests too frequent, please
	// try again later"). Distinct from the login throttle (10082): it fires on a
	// normal call — the GPS-track lookup is the one users hit — so a short backoff
	// and retry turns a surfaced 502 into a slightly slower success.
	statusTooFrequent = 10129
)

// tokenTTL is deliberately shorter than the vendor's 30-minute idle expiry.
// Re-logging in early is cheap; discovering expiry mid-request costs a failed
// call and a retry, and the vendor clock is not ours to trust to the second.
const tokenTTL = 20 * time.Minute

// Config is the connection to a VSS deployment.
//
// BaseURL is the platform root INCLUDING scheme and port, e.g.
// "https://vss.example.com" or "http://10.0.0.5:9966". The vendor defaults are
// 9966 for HTTP and 443 for HTTPS; the documentation's 192.168.x addresses are
// examples, not a real server.
type Config struct {
	BaseURL  string
	Username string
	Password string
}

// Enabled reports whether the integration is configured. Everything else
// returns ErrNotConfigured when it is not, so a deployment without a VSS server
// answers a clear 503 rather than dialling an empty host.
func (c Config) Enabled() bool {
	return c.BaseURL != "" && c.Username != "" && c.Password != ""
}

// ErrNotConfigured is returned when no VSS server has been configured.
var ErrNotConfigured = fmt.Errorf("vss: not configured (set VSS_BASE_URL, VSS_USERNAME, VSS_PASSWORD)")

// Client is a VSS API client. Safe for concurrent use; the cached token is
// guarded and only one login runs at a time.
type Client struct {
	cfg  Config
	http *http.Client

	mu       sync.Mutex
	token    string
	pid      string // WebSocket credential, returned by the same login
	fetched  time.Time
	loginErr error     // last login failure, held during the cooldown below
	cooldown time.Time // do not attempt another login before this
}

// loginCooldown throttles login attempts after a failure. The real VSS server
// rate-limits logins hard (status 10082 "Login too frequently"), so a burst of
// requests each triggering a fresh login would turn one failure into a
// self-inflicted lockout. After a failed login every caller gets the same
// cached error until the cooldown passes, then exactly one retry is allowed.
const loginCooldown = 30 * time.Second

// New builds a client. A long timeout: VSS sits behind mobile networks and the
// device-list call over a few thousand units is not fast.
func New(cfg Config) *Client {
	return &Client{cfg: cfg, http: &http.Client{Timeout: 30 * time.Second}}
}

// Enabled reports whether this client can talk to anything.
func (c *Client) Enabled() bool { return c.cfg.Enabled() }

// envelope is the shape every VSS response shares.
type envelope struct {
	Status int             `json:"status"`
	Msg    string          `json:"msg"`
	Data   json.RawMessage `json:"data"`
}

// APIError carries a vendor status code so callers can distinguish "device
// offline" (expected, show a message) from "server error" (log it).
type APIError struct {
	Status int
	Msg    string
	Path   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("vss %s: status %d: %s", e.Path, e.Status, e.Msg)
}

// Offline reports whether the failure was the device being unreachable rather
// than anything wrong with the request.
func (e *APIError) Offline() bool { return e.Status == statusDeviceOffline }

// md5Hex is the password encoding the vendor's login expects.
func md5Hex(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec // vendor protocol, not our choice
	return hex.EncodeToString(sum[:])
}

// login acquires a fresh token + pid. Caller holds c.mu.
func (c *Client) login(ctx context.Context) error {
	body, _ := json.Marshal(map[string]string{
		"username": c.cfg.Username,
		"password": md5Hex(c.cfg.Password),
	})
	raw, err := c.postRaw(ctx, "/vss/user/apiLogin.action", body)
	if err != nil {
		return err
	}
	var out struct {
		Token string `json:"token"`
		PID   string `json:"pid"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("vss login: decode: %w", err)
	}
	if out.Token == "" {
		return fmt.Errorf("vss login: empty token")
	}
	c.token, c.pid, c.fetched = out.Token, out.PID, time.Now()
	return nil
}

// Token returns a valid API token, logging in when the cached one is stale.
//
// Exported because the video pages are iframes: the browser loads them straight
// from VSS and the token has to travel in that URL. See the note on
// PlaybackURL — the FMS side keeps the token out of persisted state and mints
// it per view.
func (c *Client) Token(ctx context.Context) (string, error) {
	if !c.cfg.Enabled() {
		return "", ErrNotConfigured
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Since(c.fetched) < tokenTTL {
		return c.token, nil
	}
	// Still cooling down from a recent login failure — hand back the same error
	// rather than hammering the rate-limited login endpoint again.
	if c.loginErr != nil && time.Now().Before(c.cooldown) {
		return "", c.loginErr
	}
	if err := c.login(ctx); err != nil {
		c.loginErr, c.cooldown = err, time.Now().Add(loginCooldown)
		return "", err
	}
	c.loginErr = nil
	return c.token, nil
}

// invalidate drops the cached token so the next call logs in again.
func (c *Client) invalidate() {
	c.mu.Lock()
	c.token, c.fetched = "", time.Time{}
	c.mu.Unlock()
}

// postRaw performs one JSON POST and unwraps the envelope, without the token
// dance — used by login itself.
func (c *Client) postRaw(ctx context.Context, path string, body []byte) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.cfg.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vss %s: %w", path, err)
	}
	defer resp.Body.Close()
	// Capped read: a misconfigured BaseURL pointing at something that is not
	// VSS should fail, not stream an unbounded body into memory.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("vss %s: read: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vss %s: HTTP %d", path, resp.StatusCode)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("vss %s: not a VSS response (check VSS_BASE_URL): %w", path, err)
	}
	if env.Status != statusOK {
		return nil, &APIError{Status: env.Status, Msg: env.Msg, Path: path}
	}
	return env.Data, nil
}

// post calls an authenticated endpoint, retrying once through a fresh login if
// the token had expired. `params` is merged with the token.
func (c *Client) post(ctx context.Context, path string, params map[string]any) (json.RawMessage, error) {
	if !c.cfg.Enabled() {
		return nil, ErrNotConfigured
	}
	attempt := func() (json.RawMessage, error) {
		tok, err := c.Token(ctx)
		if err != nil {
			return nil, err
		}
		merged := make(map[string]any, len(params)+1)
		for k, v := range params {
			merged[k] = v
		}
		merged["token"] = tok
		body, err := json.Marshal(merged)
		if err != nil {
			return nil, err
		}
		return c.postRaw(ctx, path, body)
	}
	// One refresh-and-retry when the token aged out between calls.
	withTokenRetry := func() (json.RawMessage, error) {
		data, err := attempt()
		var apiErr *APIError
		if err != nil && asAPIError(err, &apiErr) &&
			(apiErr.Status == statusNotLoggedIn || apiErr.Status == statusTokenError) {
			c.invalidate()
			return attempt()
		}
		return data, err
	}
	// Bounded backoff on the vendor's per-API throttle. A history reload or a
	// quick date change can fire the GPS-track call fast enough to trip 10129;
	// waiting a beat and retrying is far better than surfacing "unreachable".
	const maxRateRetries = 3
	for i := 0; ; i++ {
		data, err := withTokenRetry()
		var apiErr *APIError
		if err != nil && asAPIError(err, &apiErr) && apiErr.Status == statusTooFrequent && i < maxRateRetries {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(i+1) * 800 * time.Millisecond):
			}
			continue
		}
		return data, err
	}
}

func asAPIError(err error, target **APIError) bool {
	e, ok := err.(*APIError)
	if ok {
		*target = e
	}
	return ok
}

// ---- devices ---------------------------------------------------------------

// Device is one MDVR as VSS knows it.
type Device struct {
	DeviceNo    string `json:"deviceno"`
	DeviceName  string `json:"devicename"`
	DeviceType  string `json:"devicetype"`
	ChannelName string `json:"channelname"`
	// AccessMode follows the vendor's device-status table: -1 offline, 1
	// ethernet, 2 wifi, 3..6 2G/3G/4G/5G.
	AccessMode      int    `json:"accessmode"`
	VideoChannels   int    `json:"videoencodernumber"`
	LastOnlineTime  string `json:"lastonlinetime"`
	LastOfflineTime string `json:"lastofflinetime"`
	Longitude       any    `json:"longitude"`
	Latitude        any    `json:"latitude"`
}

// Online reports whether the device is reachable. -1 is offline and 0 is
// "unknown", which is not the same thing but is equally unwatchable.
func (d Device) Online() bool { return d.AccessMode > 0 }

// ListDevices returns every device the account can see, one page at a time.
// pageCount -1 asks VSS for everything; the vendor supports it and a fleet's
// device list is small enough that paging adds only failure modes.
func (c *Client) ListDevices(ctx context.Context, keyword string) ([]Device, error) {
	data, err := c.post(ctx, "/vss/vehicle/findAll.action", map[string]any{
		"pageNum": 1, "pageCount": -1, "keyword": keyword,
	})
	if err != nil {
		return nil, err
	}
	var out struct {
		DataList []Device `json:"dataList"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("vss devices: decode: %w", err)
	}
	return out.DataList, nil
}

// Status is the live online state of one device.
type Status struct {
	DeviceID        string `json:"deviceID"`
	AccessMode      int    `json:"accessMode"`
	LastOnlineTime  string `json:"lastOnlineTime"`
	LastOfflineTime string `json:"lastOfflineTime"`
}

// Online reports reachability, as with Device.
func (s Status) Online() bool { return s.AccessMode > 0 }

// DeviceStatus returns the online state of one or more devices.
//
// Uses the MULTI-device endpoint even for one id: it returns an array either
// way, whereas the single-device call returns a bare object, and having one
// decode path removes a whole class of "works for one, panics for two" bug.
func (c *Client) DeviceStatus(ctx context.Context, deviceIDs ...string) ([]Status, error) {
	if len(deviceIDs) == 0 {
		return nil, nil
	}
	data, err := c.post(ctx, "/vss/onoffline/queryMoreDeviceStatus.action", map[string]any{
		"deviceID": strings.Join(deviceIDs, ","),
	})
	if err != nil {
		return nil, err
	}
	var out []Status
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("vss status: decode: %w", err)
	}
	return out, nil
}

// ---- video URLs ------------------------------------------------------------

// LiveURL builds the embeddable live-preview page for a device.
//
// THE TOKEN TRAVELS IN THIS URL. That is the vendor's iframe contract and
// cannot be avoided while using their player. Two consequences the caller must
// respect: mint it per view rather than storing it, and understand that the
// token carries whatever privileges the VSS ACCOUNT has — with an admin
// account that includes apiVehicleControl, whose actions 6 and 8 cut a
// vehicle's oil and electrical circuits. A view-only VSS user removes that
// exposure entirely and is a one-line env change.
//
// stream: 1 main stream, 0 sub-stream. Sub is the default — a fleet screen
// showing several trucks over mobile links wants the smaller stream.
func (c *Client) LiveURL(ctx context.Context, deviceID string, channels []int, mainStream bool) (string, error) {
	tok, err := c.Token(ctx)
	if err != nil {
		return "", err
	}
	stream := "0"
	if mainStream {
		stream = "1"
	}
	q := url.Values{}
	q.Set("token", tok)
	q.Set("deviceId", deviceID)
	q.Set("chs", joinChannels(channels))
	q.Set("stream", stream)
	q.Set("wnum", "1")
	q.Set("panel", "0")
	// buffer=0 on the vendor's advice for anything phone-shaped; a 2 s buffer
	// on a live camera is a long time when you are watching an incident.
	q.Set("buffer", "0")
	return strings.TrimRight(c.cfg.BaseURL, "/") + "/vss/apiPage/RealVideo.html?" + q.Encode(), nil
}

// PlaybackURL builds the embeddable playback page for a time range.
// from/to are formatted as the vendor requires: YYYYMMDDHHmmss.
func (c *Client) PlaybackURL(ctx context.Context, deviceID string, channels []int, from, to time.Time) (string, error) {
	tok, err := c.Token(ctx)
	if err != nil {
		return "", err
	}
	const layout = "20060102150405"
	q := url.Values{}
	q.Set("token", tok)
	q.Set("deviceId", deviceID)
	q.Set("chs", joinChannels(channels))
	q.Set("st", from.Format(layout))
	q.Set("et", to.Format(layout))
	q.Set("wnum", "1")
	q.Set("panel", "0")
	q.Set("buffer", "0")
	return strings.TrimRight(c.cfg.BaseURL, "/") + "/vss/apiPage/ReplayVideo.html?" + q.Encode(), nil
}

// joinChannels renders the vendor's underscore-separated channel list. Channels
// are 1-based in VSS; an empty list means channel 1 so a caller that knows
// nothing still gets a picture.
func joinChannels(chs []int) string {
	if len(chs) == 0 {
		return "1"
	}
	parts := make([]string, 0, len(chs))
	for _, c := range chs {
		parts = append(parts, fmt.Sprint(c))
	}
	return strings.Join(parts, "_")
}

// ---- video file search (§3.11) --------------------------------------------

// VideoFile is one clip on the device SD card (or server), as VSS lists it.
type VideoFile struct {
	DeviceID string `json:"deviceID"`
	Channel  int    `json:"channel"`
	Duration int    `json:"duration"` // seconds
	Size     int64  `json:"size"`     // bytes
	Type     int    `json:"type"`     // 1 ordinary, 2 alarm, 3/4 picture
	Start    string `json:"start"`    // "2017-12-20 00:00:00"
	Stop     string `json:"stop"`
	Path     string `json:"path"` // the device path, needed to download this exact file
}

// SearchVideos lists clips for a device over a time range.
//
// location 1 = the device's own SD card, which is what "check what's on the
// dashcam for this date" means. channels is 1-based; empty means channel 1.
// The vendor accepts a date-only start/end to ask "is there anything on this
// day", but we pass full timestamps so the returned list is the actual clips.
func (c *Client) SearchVideos(ctx context.Context, deviceID string, channels []int, from, to time.Time) ([]VideoFile, error) {
	const layout = "2006-01-02 15:04:05"
	data, err := c.post(ctx, "/vss/record/videoFileSearch.action", map[string]any{
		"deviceID":    deviceID,
		"startTime":   from.Format(layout),
		"endTime":     to.Format(layout),
		"channelList": joinChannelsSemicolon(channels),
		"fileType":    1, // ordinary video
		"location":    1, // device SD card
	})
	if err != nil {
		return nil, err
	}
	var out struct {
		Files []VideoFile `json:"files"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("vss video search: decode: %w", err)
	}
	return out.Files, nil
}

// ---- alarm evidence (§3.12) ------------------------------------------------

// AlarmEvidence is one device alarm with its evidence files, from the evidence
// search endpoint. This is the POLLED alternative to the push WebSocket: FMS
// learns of DMS/ADAS/collision events this way and gets the clip + snapshot to
// show and download.
type AlarmEvidence struct {
	DeviceID  string
	AlarmGuid string   // unique event id — the dedup key
	AlarmType int      // event type; see the vendor alarm-of-event table
	Lat, Lon  *float64 // from alarmGps ("lon,lat"), when present
	AlarmTime string   // device time "YYYY-MM-DD HH:MM:SS"
	Files     []EvidenceFile
}

// EvidenceFile is one video/picture attached to an alarm, with a direct URL.
type EvidenceFile struct {
	FileType  int // 1 ordinary video, 2 alarm video, 3 normal picture, 4 alarm picture
	Channel   int // 1-based; 0 when unspecified
	StartTime string
	StopTime  string
	DownURL   string // direct, token-bearing download URL
	Thumbnail string
}

// SearchAlarmEvidence lists a device's alarms in [from,to], each with its
// evidence files (§3.12). alarmType "" returns all types.
func (c *Client) SearchAlarmEvidence(ctx context.Context, deviceID string, from, to time.Time, alarmType string) ([]AlarmEvidence, error) {
	const layout = "2006-01-02 15:04:05"
	data, err := c.post(ctx, "/vss/record/evidenceToRetrieve.action", map[string]any{
		"conditionName": deviceID,
		"startTime":     from.Format(layout),
		"endTime":       to.Format(layout),
		"alarmType":     alarmType,
		"scheme":        "https",
	})
	if err != nil {
		return nil, err
	}
	// post() already unwrapped the envelope, so `data` is the alarm array itself.
	var rows []struct {
		DeviceID  string `json:"deviceID"`
		AlarmGuid string `json:"alarmGuid"`
		AlarmType any    `json:"alarmType"`
		AlarmGps  string `json:"alarmGps"`
		AlarmTime string `json:"alarmTime"`
		AlarmFile []struct {
			FileType      any    `json:"fileType"`
			Channel       any    `json:"channel"`
			FileStartTime string `json:"fileStartTime"`
			FileStopTime  string `json:"fileStopTime"`
			DownURL       string `json:"downUrl"`
			Thumbnail     string `json:"fileThumbnail"`
		} `json:"alarmFile"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("vss alarm evidence: decode: %w", err)
	}
	evs := make([]AlarmEvidence, 0, len(rows))
	for _, d := range rows {
		e := AlarmEvidence{DeviceID: d.DeviceID, AlarmGuid: d.AlarmGuid, AlarmType: toInt(d.AlarmType), AlarmTime: d.AlarmTime}
		if lon, lat, ok := parseLonLat(d.AlarmGps); ok {
			e.Lat, e.Lon = &lat, &lon
		}
		for _, f := range d.AlarmFile {
			e.Files = append(e.Files, EvidenceFile{
				FileType: toInt(f.FileType), Channel: toInt(f.Channel),
				StartTime: f.FileStartTime, StopTime: f.FileStopTime,
				DownURL: f.DownURL, Thumbnail: f.Thumbnail,
			})
		}
		evs = append(evs, e)
	}
	return evs, nil
}

// HistoryAlarm is one alarm from the general alarm-list endpoint (§3.9). Unlike
// AlarmEvidence it carries no evidence files — but it returns EVERY alarm, not
// only ones that produced a clip, which is what the vendor's own alarm page
// shows.
type HistoryAlarm struct {
	Guid       string
	AlarmType  int
	Lat, Lon   *float64
	CreateTime string // device-local "YYYY-MM-DD HH:MM:SS"
	Speed      *float64
	AlarmState int // 1 start, 0 end, 2 ongoing
}

// HistoryAlarms lists a device's alarms in [from,to] via apiFindAllByTime (§3.9)
// — all alarms regardless of evidence. alarmType "" returns every type.
func (c *Client) HistoryAlarms(ctx context.Context, deviceID string, from, to time.Time, alarmType string) ([]HistoryAlarm, error) {
	const layout = "2006-01-02 15:04:05"
	data, err := c.post(ctx, "/vss/alarm/apiFindAllByTime.action", map[string]any{
		"deviceID":  deviceID,
		"pageNum":   1,
		"pageCount": -1, // -1 = all records
		"beginTime": from.Format(layout),
		"endTime":   to.Format(layout),
		"alarmType": alarmType,
	})
	if err != nil {
		return nil, err
	}
	var out struct {
		DataList []struct {
			Guid       string `json:"guid"`
			AlarmType  any    `json:"alarmtype"`
			AlarmGps   string `json:"alarmGps"`
			CreateTime string `json:"createtime"`
			Speed      any    `json:"speed"`
			AlarmState int    `json:"alarmState"`
		} `json:"dataList"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("vss history alarms: decode: %w", err)
	}
	res := make([]HistoryAlarm, 0, len(out.DataList))
	for _, d := range out.DataList {
		a := HistoryAlarm{Guid: d.Guid, AlarmType: toInt(d.AlarmType), CreateTime: d.CreateTime, AlarmState: d.AlarmState}
		if lon, lat, ok := parseLonLat(d.AlarmGps); ok {
			a.Lat, a.Lon = &lat, &lon
		}
		if s, ok := toFloat(d.Speed); ok {
			a.Speed = &s
		}
		res = append(res, a)
	}
	return res, nil
}

// parseLonLat parses VSS's "lon,lat" alarmGps string (longitude first). "0,0"
// and unparseable strings return ok=false so a fixless alarm is not plotted.
func parseLonLat(s string) (lon, lat float64, ok bool) {
	parts := strings.Split(strings.TrimSpace(s), ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	lo, e1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	la, e2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if e1 != nil || e2 != nil || (lo == 0 && la == 0) {
		return 0, 0, false
	}
	return lo, la, true
}

// ---- GPS track (§3.7), for deriving a clip's coordinate ---------------------

// TrackPoint is one GPS fix the device reported.
type TrackPoint struct {
	Lat  float64
	Lon  float64
	Time time.Time
}

// Track returns the device's own GPS track over a range.
//
// The clip list carries no coordinate, so "where was the truck when this clip
// was recorded" is answered by matching the clip's timestamp against this — the
// dashcam's own GPS, which is what the operator means by the clip's location.
func (c *Client) Track(ctx context.Context, deviceID string, from, to time.Time) ([]TrackPoint, error) {
	const layout = "2006-01-02 15:04:05"
	data, err := c.post(ctx, "/vss/track/getTrackList.action", map[string]any{
		"deviceID":  deviceID,
		"beginTime": from.Format(layout),
		"endTime":   to.Format(layout),
		"pageNum":   1,
		"pageCount": -1,
	})
	if err != nil {
		return nil, err
	}
	// The vendor nests the fix under a "location" object and formats lat/lon as
	// strings; parse leniently so one odd row does not drop the whole track.
	var rows []struct {
		Location struct {
			Lat any    `json:"latitude"`
			Lon any    `json:"longitude"`
			DTU string `json:"dtu"`
		} `json:"location"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("vss track: decode: %w", err)
	}
	out := make([]TrackPoint, 0, len(rows))
	for _, r := range rows {
		lat, ok1 := toFloat(r.Location.Lat)
		lon, ok2 := toFloat(r.Location.Lon)
		t, err := time.Parse("2006-01-02 15:04:05", r.Location.DTU)
		if !ok1 || !ok2 || err != nil {
			continue
		}
		out = append(out, TrackPoint{Lat: lat, Lon: lon, Time: t})
	}
	return out, nil
}

// NearestFix returns the track point closest in time to want, or false if the
// track is empty. Linear scan — a day's track is small and this runs once per
// clip, not per request.
func NearestFix(track []TrackPoint, want time.Time) (TrackPoint, bool) {
	if len(track) == 0 {
		return TrackPoint{}, false
	}
	best := track[0]
	bestGap := absDur(want.Sub(best.Time))
	for _, p := range track[1:] {
		if g := absDur(want.Sub(p.Time)); g < bestGap {
			best, bestGap = p, g
		}
	}
	return best, true
}

// ---- download task (§3.15) -------------------------------------------------

// DownloadTask is the handle to an in-progress web download on the VSS side.
type DownloadTask struct {
	TaskID         string `json:"taskId"`
	FileServerHost string `json:"fileServerHost"`
	ResWebPath     string `json:"resWebPath"`
}

// FileURL is the absolute URL of the finished file on the VSS file server.
func (t DownloadTask) FileURL() string {
	return strings.TrimRight(t.FileServerHost, "/") + t.ResWebPath
}

// DownloadURL returns the ready-to-fetch URL for a finished download task, from
// the task-list endpoint (§3.15.4). The insert response's resWebPath is NOT
// directly servable — the file server answers 404 on it and instead serves the
// clip through a token-bearing downUrl (api/fileDown.php?filePath=…&token=…)
// that only this endpoint returns. taskID is DownloadTask.TaskID.
func (c *Client) DownloadURL(ctx context.Context, deviceID, taskID, resWebPath string) (string, error) {
	const layout = "2006-01-02 15:04:05"
	now := time.Now()
	data, err := c.post(ctx, "/vss/webdownrecord/findPage.action", map[string]any{
		"pageNum":       1,
		"pageCount":     100,
		"deviceNo":      deviceID,
		"username":      c.cfg.Username,
		"scheme":        "http",
		"taskStartTime": now.Add(-48 * time.Hour).Format(layout),
		"taskEndTime":   now.Add(24 * time.Hour).Format(layout),
	})
	if err != nil {
		return "", err
	}
	// This VSS names the array "list" (not the doc's "dataList").
	var out struct {
		List []struct {
			Guid       string `json:"guid"`
			DownURL    string `json:"downUrl"`
			ResWebPath string `json:"resWebPath"`
		} `json:"list"`
	}
	_ = json.Unmarshal(data, &out)
	for _, d := range out.List {
		if d.DownURL != "" && (d.ResWebPath == resWebPath || d.Guid == taskID) {
			return d.DownURL, nil
		}
	}
	raw := string(data)
	if len(raw) > 900 {
		raw = raw[:900]
	}
	log.Printf("dashcam: downUrl not found (task=%s rows=%d) findPage=%s", taskID, len(out.List), raw)
	return "", fmt.Errorf("vss download url: task not ready")
}

// StartDownload asks VSS to pull one clip (by its device path) onto the VSS
// file server as an mp4. Returns a task to poll. `path` is the VideoFile.Path
// from a prior search — downloading by file is exact, versus by time range
// which can span several clips.
func (c *Client) StartDownload(ctx context.Context, deviceID string, channel int, from, to time.Time, path string) (*DownloadTask, error) {
	const layout = "2006-01-02 15:04:05"
	data, err := c.post(ctx, "/vss/webdownrecord/insert.action", map[string]any{
		"deviceNo":          deviceID,
		"channel":           channel,
		"resStartTime":      from.Format(layout),
		"resEndTime":        to.Format(layout),
		"resDeviceFileName": path,
		"fileFormat":        "mp4",
	})
	if err != nil {
		return nil, err
	}
	var task DownloadTask
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("vss download start: decode: %w", err)
	}
	return &task, nil
}

// progressPct decodes VSS's progress field whether it arrives as a JSON number
// (90) or a string ("90"). The vendor doc says string, but the live server sends
// a bare number — and a string field rejecting a number failed the WHOLE status
// decode, so every poll errored and the download "failed" with an unmarshal error.
type progressPct string

func (p *progressPct) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "null" {
		s = ""
	}
	*p = progressPct(s)
	return nil
}

// DownloadProgress is the state of a download task.
type DownloadProgress struct {
	TaskID    string      `json:"taskId"`
	DownState int         `json:"downState"` // 2 complete; -7 no data; -8 offline; -9 error
	Progress  progressPct `json:"progress"`  // 0-100
}

// Done reports whether the download finished successfully.
func (p DownloadProgress) Done() bool { return p.DownState == 2 }

// Failed reports a terminal failure (no data, device offline, abnormal stop).
func (p DownloadProgress) Failed() bool {
	return p.DownState == -7 || p.DownState == -8 || p.DownState == -9
}

// Terminal reports a failure that resuming cannot fix: the clip is not on the
// device (-7) or the device is offline (-8).
func (p DownloadProgress) Terminal() bool { return p.DownState == -7 || p.DownState == -8 }

// Resumable reports a transfer that stopped part-way but can be CONTINUED — a
// pause (-1) or an abnormal stop (-9). Over cellular this is the common case: a
// signal blip drops the pull at, say, 90%, and the vendor's guidance is to
// resume the task rather than start over (§3.15.2: "when the status is 0/-1/-9
// the user can choose to continue downloading").
func (p DownloadProgress) Resumable() bool { return p.DownState == -1 || p.DownState == -9 }

// Reason turns a TERMINAL downState into a message a dispatcher can act on.
// Pulling a clip is a LIVE command to the dashcam, so "device offline" is the
// common one: the truck's camera must be powered on and connected for VSS to
// fetch the file off its SD card.
func (p DownloadProgress) Reason() string {
	switch p.DownState {
	case -7:
		return "clip not found on the SD card — it may have been overwritten"
	case -8:
		return "dashcam is offline — power the camera on and retry"
	default:
		return fmt.Sprintf("dashcam download failed (state %d)", p.DownState)
	}
}

// ResumeDownload continues a paused / abnormally-stopped web-download task
// (taskManagement action 1), so a transfer that dropped mid-way finishes from
// where it stopped instead of restarting.
func (c *Client) ResumeDownload(ctx context.Context, taskID, resWebPath string) error {
	_, err := c.post(ctx, "/vss/webdownrecord/taskManagement.action", map[string]any{
		"taskId":     taskID,
		"action":     1, // resume
		"resWebPath": resWebPath,
		"username":   c.cfg.Username,
	})
	return err
}

// DownloadStatus polls a task.
func (c *Client) DownloadStatus(ctx context.Context, taskID, resWebPath string) (*DownloadProgress, error) {
	data, err := c.post(ctx, "/vss/webdownrecord/findTaskProgress.action", map[string]any{
		"taskId":     taskID,
		"resWebPath": resWebPath,
	})
	if err != nil {
		return nil, err
	}
	var p DownloadProgress
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("vss download status: decode: %w", err)
	}
	return &p, nil
}

// joinChannelsSemicolon renders the vendor's SEMICOLON-separated channel list
// (video search wants "1;2", unlike the underscore form the video URLs use).
func joinChannelsSemicolon(chs []int) string {
	if len(chs) == 0 {
		return "1"
	}
	parts := make([]string, 0, len(chs))
	for _, c := range chs {
		parts = append(parts, fmt.Sprint(c))
	}
	return strings.Join(parts, ";")
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
