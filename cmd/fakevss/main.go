// Command fakevss runs the vsstest fake camera platform for local development,
// so telemetry's evidence poller and clip downloads work without the vendor's
// production account (its logins are rate-limited and shared with production).
//
//	FAKE_VSS_ADDR=:8090 FAKE_VSS_DEVICES=FAKE-CAM-1,FAKE-CAM-2 go run ./cmd/fakevss
//	VSS_BASE_URL=http://localhost:8090 VSS_USERNAME=x VSS_PASSWORD=x make run-ingest
package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/karlo/fms/internal/vss/vsstest"
)

func main() {
	addr := envOr("FAKE_VSS_ADDR", ":8090")
	devices := strings.Split(envOr("FAKE_VSS_DEVICES", "FAKE-CAM-1,FAKE-CAM-2"), ",")
	fake := vsstest.New(devices...).Handler()
	logged := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		if r.URL.Path == "/vss/webdownrecord/insert.action" {
			log.Printf("SD-CARD PULL requested (should never happen for auto-save)")
		}
		fake.ServeHTTP(w, r)
	})
	log.Printf("fake VSS on %s, devices %v", addr, devices)
	log.Fatal(http.ListenAndServe(addr, logged))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
