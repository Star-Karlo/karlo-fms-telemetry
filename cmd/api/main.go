// Package main starts the Karlo FMS HTTP API.
//
// @title           Karlo FMS API
// @version         0.1
// @description     Backend API for the Fleet Management System (vehicles, devices, telemetry).
// @BasePath        /api/v1
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/karlo/fms/internal/api"
)

func main() {
	addr := os.Getenv("API_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("API listening on %s  (swagger: http://localhost%s/swagger/index.html)", addr, addr)
	if err := http.ListenAndServe(addr, api.NewRouter()); err != nil {
		log.Fatal(err)
	}
}
