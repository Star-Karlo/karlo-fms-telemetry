package vss

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeVSS is a stand-in for the vendor platform, reproducing the two behaviours
// that break naive clients: errors arrive as HTTP 200 with a status field, and
// tokens expire without warning.
type fakeVSS struct {
	logins   int32
	token    atomic.Value // string — the only token it will currently accept
	expireIn int32        // authed calls to serve before the token "expires"
	served   int32
}

func newFake() *fakeVSS {
	f := &fakeVSS{expireIn: 1 << 30}
	f.token.Store("tok-1")
	return f
}

func (f *fakeVSS) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/vss/user/apiLogin.action", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		// The vendor expects an md5 hex digest, never the plaintext.
		if body["password"] == "secret" {
			writeEnv(w, 10001, "Account or password error", nil)
			return
		}
		if body["password"] != md5Hex("secret") {
			writeEnv(w, 10001, "Account or password error", nil)
			return
		}
		n := atomic.AddInt32(&f.logins, 1)
		tok := "tok-" + string(rune('0'+n))
		f.token.Store(tok)
		atomic.StoreInt32(&f.served, 0)
		writeEnv(w, 10000, "Success", map[string]string{"token": tok, "pid": "pid-x"})
	})
	mux.HandleFunc("/vss/onoffline/queryMoreDeviceStatus.action", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["token"] != f.token.Load().(string) {
			writeEnv(w, 10023, "Access token error", nil)
			return
		}
		if atomic.AddInt32(&f.served, 1) > atomic.LoadInt32(&f.expireIn) {
			// Silently rotate: the old token stops working, exactly as a
			// 30-minute idle expiry behaves.
			f.token.Store("rotated")
			writeEnv(w, 10004, "Not logged in", nil)
			return
		}
		writeEnv(w, 10000, "Success", []Status{{DeviceID: "dev1", AccessMode: 5}})
	})
	mux.HandleFunc("/vss/vehicle/findAll.action", func(w http.ResponseWriter, r *http.Request) {
		writeEnv(w, 10000, "Success", map[string]any{
			"dataList": []Device{{DeviceNo: "dev1", DeviceName: "H-9925-OA", AccessMode: 5, VideoChannels: 2, ChannelName: "Front;Cabin"}},
		})
	})
	return mux
}

func writeEnv(w http.ResponseWriter, status int, msg string, data any) {
	raw, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	// ALWAYS 200 — that is the vendor's behaviour and the point of the test.
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(envelope{Status: status, Msg: msg, Data: raw})
}

func newTestClient(t *testing.T, f *fakeVSS) *Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return New(Config{BaseURL: srv.URL, Username: "u", Password: "secret"})
}

func TestPasswordIsSentAsMD5(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	// The fake rejects the plaintext outright, so a successful login proves the
	// client hashed it.
	if _, err := c.Token(context.Background()); err != nil {
		t.Fatalf("login: %v", err)
	}
}

func TestErrorsArriveAsHTTP200(t *testing.T) {
	f := newFake()
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL, Username: "u", Password: "wrong"})
	_, err := c.Token(context.Background())
	if err == nil {
		t.Fatal("expected a failure; a 10001 body returned with HTTP 200 must not read as success")
	}
	var apiErr *APIError
	if !asAPIError(err, &apiErr) || apiErr.Status != 10001 {
		t.Fatalf("want APIError status 10001, got %v", err)
	}
}

func TestTokenIsCachedAcrossCalls(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := c.DeviceStatus(ctx, "dev1"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&f.logins); got != 1 {
		t.Fatalf("expected 1 login for 3 calls, got %d — the token is not being cached", got)
	}
}

func TestExpiredTokenRetriesOnceAfterFreshLogin(t *testing.T) {
	f := newFake()
	atomic.StoreInt32(&f.expireIn, 1) // second authed call gets "not logged in"
	c := newTestClient(t, f)
	ctx := context.Background()
	if _, err := c.DeviceStatus(ctx, "dev1"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	// This one hits the expiry. The fake resets its per-token call counter on
	// login, so the retry that follows succeeds — leaving the login count as
	// the evidence. Without the retry the whole camera UI breaks after 30 quiet
	// minutes, which is the failure this test exists to catch.
	if _, err := c.DeviceStatus(ctx, "dev1"); err != nil {
		t.Fatalf("second call should have recovered via re-login: %v", err)
	}
	if got := atomic.LoadInt32(&f.logins); got != 2 {
		t.Fatalf("expected exactly 2 logins (initial + one retry), got %d", got)
	}
}

func TestNotConfiguredIsDistinctFromFailure(t *testing.T) {
	c := New(Config{})
	if c.Enabled() {
		t.Fatal("empty config must not report enabled")
	}
	if _, err := c.ListDevices(context.Background(), ""); err != ErrNotConfigured {
		t.Fatalf("want ErrNotConfigured, got %v", err)
	}
}

func TestLiveURLCarriesDeviceAndChannels(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	u, err := c.LiveURL(context.Background(), "dev1", []int{1, 2}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"RealVideo.html", "deviceId=dev1", "chs=1_2", "stream=0", "token=tok-1"} {
		if !strings.Contains(u, want) {
			t.Errorf("live URL missing %q:\n%s", want, u)
		}
	}
}

func TestPlaybackURLUsesVendorTimeFormat(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	from := time.Date(2026, 7, 24, 9, 5, 3, 0, time.UTC)
	u, err := c.PlaybackURL(context.Background(), "dev1", []int{1}, from, from.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// YYYYMMDDHHmmss — anything else and VSS returns no footage at all.
	if !strings.Contains(u, "st=20260724090503") || !strings.Contains(u, "et=20260724100503") {
		t.Fatalf("playback URL times not in vendor format:\n%s", u)
	}
}

func TestChannelsDefaultToOne(t *testing.T) {
	if got := joinChannels(nil); got != "1" {
		t.Fatalf("no channels should fall back to channel 1, got %q", got)
	}
}

func TestListDevicesDecodesVendorShape(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	devs, err := c.ListDevices(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(devs) != 1 || devs[0].DeviceNo != "dev1" || !devs[0].Online() || devs[0].VideoChannels != 2 {
		t.Fatalf("unexpected decode: %+v", devs)
	}
}
