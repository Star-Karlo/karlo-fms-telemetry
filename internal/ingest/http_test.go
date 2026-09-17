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
