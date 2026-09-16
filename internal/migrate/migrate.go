// Package migrate converts legacy MongoDB tracking exports (JSON arrays in
// s3://karlo-cold-storage/trackings/...) into the cold Parquet schema
// (s3://karlo-fms-cold-storage/telemetry/date=YYYY-MM-DD/), geocoding each
// point so old history is queryable through the same /v1/history dual-read.
package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/karlo/fms/internal/archive"
	"github.com/karlo/fms/internal/geocode"
)

// legacyRec mirrors a record in the Mongo export. Only fields we keep are typed.
type legacyRec struct {
	IMEITracker   string      `json:"imeiTracker"`
	Latitude      *float64    `json:"latitude"`
	Longitude     *float64    `json:"longitude"`
	Altitude      *float64    `json:"altitude"`
	Speed         *float64    `json:"speed"`
	Bearing       *float64    `json:"bearing"`
	Sat           *int        `json:"sat"`
	BatteryStatus string      `json:"batteryStatus"`
	BatteryLevel  *float64    `json:"batteryLevel"`
	Status        string      `json:"status"`
	HDOP          *float64    `json:"HDOP"`
	PDOP          *float64    `json:"PDOP"`
	GpsOn         *bool       `json:"gpsOn"`
	FakeGps       *bool       `json:"fakeGps"`
	ErrorDesc     flexStrings `json:"errorDesc"`
	TrackingCount *int        `json:"trackingCount"`
	Truck         mongoID     `json:"truck"`
	TimeReceived  mongoTime   `json:"timeReceived"` // preferred event time (newer schema)
	CreatedAt     mongoTime   `json:"createdAt"`    // fallback event time (older 2023/2024 schema has no timeReceived)
}

// mongoTime unmarshals a timestamp in any shape the exports use: a plain RFC3339
// string, MongoDB relaxed extended JSON {"$date":"ISO"}, or canonical
// {"$date":{"$numberLong":"millis"}}.
type mongoTime struct {
	t  time.Time
	ok bool
}

func (m *mongoTime) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' { // plain "ISO"
		var s string
		if json.Unmarshal(b, &s) == nil {
			m.set(s)
		}
		return nil
	}
	var o struct {
		Date json.RawMessage `json:"$date"`
	}
	if json.Unmarshal(b, &o) != nil || len(o.Date) == 0 {
		return nil
	}
	d := bytes.TrimSpace(o.Date)
	if len(d) > 0 && d[0] == '"' { // {"$date":"ISO"}
		var s string
		if json.Unmarshal(d, &s) == nil {
			m.set(s)
		}
		return nil
	}
	var nl struct { // {"$date":{"$numberLong":"ms"}}
		NumberLong string `json:"$numberLong"`
	}
	if json.Unmarshal(d, &nl) == nil && nl.NumberLong != "" {
		if ms, err := strconv.ParseInt(nl.NumberLong, 10, 64); err == nil {
			m.t, m.ok = time.UnixMilli(ms).UTC(), true
		}
		return nil
	}
	var ms int64 // bare epoch millis
	if json.Unmarshal(d, &ms) == nil {
		m.t, m.ok = time.UnixMilli(ms).UTC(), true
	}
	return nil
}

func (m *mongoTime) set(s string) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		m.t, m.ok = t.UTC(), true
	}
}

// flexStrings unmarshals a field that is sometimes a JSON array of strings and
// sometimes a single string (e.g. errorDesc: ["NORMAL"] vs "NORMAL").
type flexStrings []string

func (f *flexStrings) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '[' {
		var a []string
		if json.Unmarshal(b, &a) == nil {
			*f = a
		}
		return nil
	}
	if b[0] == '"' {
		var s string
		if json.Unmarshal(b, &s) == nil {
			*f = []string{s}
		}
	}
	return nil
}

// mongoID unmarshals an ObjectId as a plain string or {"$oid":"..."}.
type mongoID struct{ s string }

func (m *mongoID) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		return json.Unmarshal(b, &m.s)
	}
	var o struct {
		Oid string `json:"$oid"`
	}
	if json.Unmarshal(b, &o) == nil {
		m.s = o.Oid
	}
	return nil
}

// Migrator holds shared clients/config for processing many source files.
type Migrator struct {
	S3       *s3.Client
	Src, Dst string
	Geo      *geocode.Service // nil => skip geocoding
	TmpDir   string
}

// FileResult summarizes one processed source file.
type FileResult struct {
	Key        string
	Skipped    bool // already done (resume marker present)
	Rows       int
	Dates      int
	Parsed     int
	BadRecords int // records that failed to unmarshal (skipped, not fatal)
}

// ProcessFile converts one source JSON file into per-day Parquet in the cold
// bucket. Idempotent: writes a _migrate-done/<key> marker and skips if present.
func (m *Migrator) ProcessFile(ctx context.Context, key string) (FileResult, error) {
	res := FileResult{Key: key}
	marker := "_migrate-done/" + key
	if _, err := m.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &m.Dst, Key: &marker}); err == nil {
		res.Skipped = true
		return res, nil
	}

	obj, err := m.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: &m.Src, Key: &key})
	if err != nil {
		return res, fmt.Errorf("get %s: %w", key, err)
	}
	raw, err := io.ReadAll(obj.Body)
	obj.Body.Close()
	if err != nil {
		return res, fmt.Errorf("read %s: %w", key, err)
	}

	// Decode the array element-by-element so a single malformed record can't
	// fail the whole file (the exports have occasional type quirks).
	var rawRecs []json.RawMessage
	if err := json.Unmarshal(raw, &rawRecs); err != nil {
		return res, fmt.Errorf("parse %s: %w", key, err)
	}
	raw = nil // free the big buffer; rawRecs holds copies
	res.Parsed = len(rawRecs)

	byDate := map[string][]archive.Row{}
	for _, rr := range rawRecs {
		var rec legacyRec
		if err := json.Unmarshal(rr, &rec); err != nil {
			res.BadRecords++
			continue
		}
		row, date, ok := m.recToRow(ctx, &rec)
		if !ok {
			continue
		}
		byDate[date] = append(byDate[date], row)
	}

	base := sanitize(strings.TrimSuffix(path.Base(key), ".json"))
	for date, rows := range byDate {
		// Unique temp file per write — workers run concurrently and must not
		// share a path (a shared file gets truncated mid-upload by another worker).
		tmp, err := os.CreateTemp(m.TmpDir, "part-*.parquet")
		if err != nil {
			return res, err
		}
		local := tmp.Name()
		tmp.Close()
		werr := archive.WriteParquet(rows, local)
		if werr != nil {
			os.Remove(local)
			return res, fmt.Errorf("write parquet %s/%s: %w", date, key, werr)
		}
		dstKey := fmt.Sprintf("telemetry/date=%s/hist_%s.parquet", date, base)
		uerr := m.put(ctx, dstKey, local)
		os.Remove(local)
		if uerr != nil {
			return res, fmt.Errorf("upload %s: %w", dstKey, uerr)
		}
		res.Rows += len(rows)
	}
	res.Dates = len(byDate)

	// Resume marker (empty object).
	if _, err := m.S3.PutObject(ctx, &s3.PutObjectInput{Bucket: &m.Dst, Key: &marker, Body: bytes.NewReader(nil)}); err != nil {
		return res, fmt.Errorf("marker %s: %w", marker, err)
	}
	return res, nil
}

// recToRow maps a legacy record to an archive.Row + its date partition. Returns
// ok=false for records with no usable event time / imei.
func (m *Migrator) recToRow(ctx context.Context, r *legacyRec) (archive.Row, string, bool) {
	if r.IMEITracker == "" {
		return archive.Row{}, "", false
	}
	// Event time: prefer timeReceived (newer schema), else createdAt (2023/2024).
	var ts time.Time
	switch {
	case r.TimeReceived.ok:
		ts = r.TimeReceived.t
	case r.CreatedAt.ok:
		ts = r.CreatedAt.t
	default:
		return archive.Row{}, "", false
	}
	ms := ts.UnixMilli()

	row := archive.Row{
		TimeMs:     ms,
		ReceivedMs: &ms,
		IMEI:       r.IMEITracker,
		Lat:        r.Latitude,
		Lon:        r.Longitude,
		Bearing:    r.Bearing,
		Speed:      r.Speed,
		Ignition:   nil, // no source field
	}

	// Best-effort reverse geocode (a miss leaves region columns empty).
	if m.Geo != nil && r.Latitude != nil && r.Longitude != nil {
		if g, err := m.Geo.Reverse(ctx, *r.Latitude, *r.Longitude); err == nil && g.Found {
			row.Kecamatan, row.Kelurahan = g.Kecamatan, g.Kelurahan
			row.Kabupaten, row.Kota = g.Kabupaten, g.Kota
			row.Provinsi, row.Jalan = g.Provinsi, g.Jalan
		}
	}

	meta := map[string]any{"source": "historical"}
	putIfF(meta, "altitude", r.Altitude)
	if r.Sat != nil {
		meta["satellites"] = *r.Sat
	}
	putIfF(meta, "batteryLevel", r.BatteryLevel)
	putIfS(meta, "batteryStatus", r.BatteryStatus)
	putIfS(meta, "status", r.Status)
	putIfF(meta, "hdop", r.HDOP)
	putIfF(meta, "pdop", r.PDOP)
	if r.GpsOn != nil {
		meta["gpsOn"] = *r.GpsOn
	}
	if r.FakeGps != nil {
		meta["fakeGps"] = *r.FakeGps
	}
	if len(r.ErrorDesc) > 0 {
		meta["errorDesc"] = r.ErrorDesc
	}
	if r.TrackingCount != nil {
		meta["trackingCount"] = *r.TrackingCount
	}
	putIfS(meta, "truck", r.Truck.s)
	if b, err := json.Marshal(meta); err == nil {
		row.Metadata = string(b)
	} else {
		row.Metadata = "{}"
	}

	return row, ts.Format("2006-01-02"), true
}

func (m *Migrator) put(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = m.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       &m.Dst,
		Key:          &key,
		Body:         f,
		StorageClass: types.StorageClassGlacierIr,
		ContentType:  aws.String("application/vnd.apache.parquet"),
	})
	return err
}

func putIfF(m map[string]any, k string, v *float64) {
	if v != nil {
		m[k] = *v
	}
}
func putIfS(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}

// sanitize keeps an S3-key-safe basename.
func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}
