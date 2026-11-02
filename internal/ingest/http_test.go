package ingest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthorizedRead_ServiceToken(t *testing.T) {
	h := (&Handler{apiKey: "k1", serviceTokens: map[string]struct{}{}}).WithServiceTokens([]string{" tok-a ", "tok-b", ""})
	mk := func(hdr, val string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/v1/live", nil)
		r.Header.Set(hdr, val)
		return r
	}
	if !h.AuthorizedRead(mk("X-Ingest-Key", "k1")) {
		t.Fatal("ingest key must still pass")
	}
	if !h.AuthorizedRead(mk("Authorization", "Bearer tok-a")) {
		t.Fatal("accepted service token must pass")
	}
	if h.AuthorizedRead(mk("Authorization", "Bearer nope")) || h.AuthorizedRead(mk("Authorization", "tok-a")) {
		t.Fatal("unknown or non-bearer token must fail")
	}
	if h.AuthorizedRead(httptest.NewRequest(http.MethodGet, "/v1/live", nil)) {
		t.Fatal("no credentials must fail")
	}
}

func TestIdentify_NamesTheSender(t *testing.T) {
	// Shared secret: authorised, and labelled as the shared key so rows it
	// writes are distinguishable from a named sender's.
	h := &Handler{apiKey: "k1"}
	if name, ok := h.identify(t.Context(), "k1"); !ok || name != SharedKeyName {
		t.Fatalf("shared secret: got (%q, %v), want (%q, true)", name, ok, SharedKeyName)
	}
	if name, ok := h.identify(t.Context(), "wrong"); ok || name != "" {
		t.Fatalf("wrong secret: got (%q, %v), want (\"\", false)", name, ok)
	}
	if name, ok := h.identify(t.Context(), ""); ok || name != "" {
		t.Fatalf("no key: got (%q, %v), want (\"\", false)", name, ok)
	}

	// Nothing configured: the endpoint stays open for local runs, and there is
	// no sender to attribute — so no stamp rather than a made-up one.
	open := &Handler{}
	if name, ok := open.identify(t.Context(), ""); !ok || name != "" {
		t.Fatalf("unconfigured: got (%q, %v), want (\"\", true)", name, ok)
	}

	// authorized must agree with identify, since it now delegates.
	if h.authorized(t.Context(), "k1") != true || h.authorized(t.Context(), "wrong") != false {
		t.Fatal("authorized disagrees with identify")
	}
}

func TestStampSender(t *testing.T) {
	// A nil metadata map must be created, not panicked on.
	readings := []Reading{{IMEI: "1"}, {IMEI: "2", Metadata: map[string]any{"source": "generic"}}}
	stampSender(readings, "teltonika-relay")
	for i, rd := range readings {
		if rd.Metadata["ingest_key"] != "teltonika-relay" {
			t.Fatalf("reading %d: ingest_key = %v, want teltonika-relay", i, rd.Metadata["ingest_key"])
		}
	}
	// The existing source label is untouched — the two answer different
	// questions (transport vs sender) and both are needed.
	if readings[1].Metadata["source"] != "generic" {
		t.Fatalf("source was overwritten: %v", readings[1].Metadata["source"])
	}

	// An empty sender stamps nothing, leaving no key field to misread.
	plain := []Reading{{IMEI: "3"}}
	stampSender(plain, "")
	if _, ok := plain[0].Metadata["ingest_key"]; ok {
		t.Fatal("empty sender must not stamp an ingest_key")
	}
}
