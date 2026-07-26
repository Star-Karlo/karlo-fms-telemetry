package ingest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// End-to-end against the local DB: the user's exact example payload must land in
// green_driving_events as a braking event with value 45. Skips without a DB.
func TestExtractExamplePayload(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	imei := "864454076648348"
	when, _ := time.Parse(time.RFC3339, "2025-07-14T00:00:00Z")
	_, _ = pool.Exec(context.Background(),
		`DELETE FROM green_driving_events WHERE imei=$1 AND occurred_at=$2`, imei, when)

	// Reading built from the example io bag (confirmed key names + values).
	rd := Reading{
		IMEI:    imei,
		GPSTime: when,
		Lat:     -6.68943, Lon: 108.33757, Speed: 56,
		Metadata: map[string]any{
			"Ignition": float64(1), "Movement": float64(1), "External Voltage": float64(28211),
			"Green driving type": float64(2), "Green Driving Value": float64(45),
			"Green driving event duration": float64(1200),
		},
	}
	NewGreenStore(pool).Extract(context.Background(), []Reading{rd})

	var typ, dur int
	var value float64
	err = pool.QueryRow(context.Background(),
		`SELECT event_type, value, duration_ms FROM green_driving_events WHERE imei=$1 AND occurred_at=$2`,
		imei, when).Scan(&typ, &value, &dur)
	if err != nil {
		t.Fatalf("row not stored: %v", err)
	}
	if typ != 2 || value != 45 || dur != 1200 {
		t.Errorf("stored type=%d value=%v dur=%d; want 2, 45, 1200", typ, value, dur)
	}
	t.Logf("stored OK: type=%d (braking) value=%v -> %.2fg dur=%dms", typ, value, value*0.01, dur)
}
