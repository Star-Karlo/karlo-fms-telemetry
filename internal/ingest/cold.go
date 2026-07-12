package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	atypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// imeiRe guards the IMEI before it's interpolated into Athena SQL (the hot path
// uses bound params; Athena is string SQL, so we allow only safe chars).
var imeiRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func validIMEI(s string) bool { return imeiRe.MatchString(s) }

// ColdReader queries archived telemetry Parquet in S3 via Athena.
type ColdReader struct {
	client   *athena.Client
	database string
	table    string
	output   string // s3://.../ location for query results
}

func NewColdReader(ctx context.Context, database, table, output string) (*ColdReader, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &ColdReader{client: athena.NewFromConfig(cfg), database: database, table: table, output: output}, nil
}

// History returns cold points for imei in the time window, ascending, up to
// limit rows. lowerStrict makes the lower bound exclusive (cursor paging);
// upperExclusive makes the upper bound exclusive (used at the hot/cold boundary).
func (c *ColdReader) History(ctx context.Context, imei string, lower, upper time.Time, lowerStrict, upperExclusive bool, limit int) ([]Position, error) {
	loOp, hiOp := ">=", "<="
	if lowerStrict {
		loOp = ">"
	}
	if upperExclusive {
		hiOp = "<"
	}
	q := fmt.Sprintf(`SELECT time_ms, received_ms, lat, lon, speed, bearing, ignition,
       kecamatan, kelurahan, kabupaten, kota, provinsi, jalan, metadata
FROM %s.%s
WHERE imei = '%s' AND date BETWEEN '%s' AND '%s'
  AND time_ms %s %d AND time_ms %s %d
ORDER BY time_ms LIMIT %d`,
		c.database, c.table, imei,
		lower.UTC().Format("2006-01-02"), upper.UTC().Format("2006-01-02"),
		loOp, lower.UnixMilli(), hiOp, upper.UnixMilli(), limit)

	start, err := c.client.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString:           &q,
		QueryExecutionContext: &atypes.QueryExecutionContext{Database: &c.database},
		ResultConfiguration:   &atypes.ResultConfiguration{OutputLocation: &c.output},
	})
	if err != nil {
		return nil, fmt.Errorf("athena start: %w", err)
	}
	qid := start.QueryExecutionId

	for { // poll to completion
		ex, err := c.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: qid})
		if err != nil {
			return nil, err
		}
		switch ex.QueryExecution.Status.State {
		case atypes.QueryExecutionStateSucceeded:
			return c.collect(ctx, qid, imei)
		case atypes.QueryExecutionStateFailed, atypes.QueryExecutionStateCancelled:
			reason := ""
			if r := ex.QueryExecution.Status.StateChangeReason; r != nil {
				reason = *r
			}
			return nil, fmt.Errorf("athena %s: %s", ex.QueryExecution.Status.State, reason)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(400 * time.Millisecond):
		}
	}
}

func (c *ColdReader) collect(ctx context.Context, qid *string, imei string) ([]Position, error) {
	var out []Position
	var token *string
	first := true
	for {
		res, err := c.client.GetQueryResults(ctx, &athena.GetQueryResultsInput{QueryExecutionId: qid, NextToken: token})
		if err != nil {
			return nil, err
		}
		for i, row := range res.ResultSet.Rows {
			if first && i == 0 {
				continue // header row
			}
			out = append(out, coldRow(imei, row.Data))
		}
		first = false
		if token = res.NextToken; token == nil {
			break
		}
	}
	return out, nil
}

// coldRow maps an Athena result row (all strings) to a Position. Column order
// matches the SELECT above.
func coldRow(imei string, d []atypes.Datum) Position {
	get := func(i int) *string {
		if i < len(d) {
			return d[i].VarCharValue
		}
		return nil
	}
	p := Position{IMEI: imei}
	if v := get(0); v != nil {
		if ms, err := strconv.ParseInt(*v, 10, 64); err == nil {
			p.Time = time.UnixMilli(ms).UTC()
		}
	}
	if v := get(1); v != nil {
		if ms, err := strconv.ParseInt(*v, 10, 64); err == nil {
			t := time.UnixMilli(ms).UTC()
			p.ReceivedAt = &t
		}
	}
	p.Lat = floatPtr(get(2))
	p.Lon = floatPtr(get(3))
	p.Speed = floatPtr(get(4))
	p.Bearing = floatPtr(get(5))
	if v := get(6); v != nil {
		b := *v == "true"
		p.Ignition = &b
	}
	p.Kecamatan = str(get(7))
	p.Kelurahan = str(get(8))
	p.Kabupaten = str(get(9))
	p.Kota = str(get(10))
	p.Provinsi = str(get(11))
	p.Jalan = str(get(12))
	p.Metadata = stripRaw(get(13))
	return p
}

func floatPtr(s *string) *float64 {
	if s == nil || *s == "" {
		return nil
	}
	if f, err := strconv.ParseFloat(*s, 64); err == nil {
		return &f
	}
	return nil
}

func str(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// stripRaw drops the bulky _raw key from stored metadata, matching the hot read.
func stripRaw(s *string) json.RawMessage {
	if s == nil || *s == "" {
		return json.RawMessage("{}")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(*s), &m); err != nil {
		return json.RawMessage(*s)
	}
	delete(m, "_raw")
	b, err := json.Marshal(m)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}
