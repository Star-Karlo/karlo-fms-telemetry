package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	atypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// imeiRe guards the IMEI before it's interpolated into Athena SQL (the hot path
// uses bound params; Athena is string SQL, so we allow only safe chars).
var imeiRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func validIMEI(s string) bool { return imeiRe.MatchString(s) }

// athenaAPI is the part of the Athena client the cold reader uses, so the
// paging below can be tested without AWS.
type athenaAPI interface {
	StartQueryExecution(context.Context, *athena.StartQueryExecutionInput, ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
	GetQueryExecution(context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
	GetQueryResults(context.Context, *athena.GetQueryResultsInput, ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
}

const (
	// coldCursorTTL is how long a query's result stays resumable. A client
	// walking a track asks for the next page within seconds.
	coldCursorTTL = 10 * time.Minute
	// coldCursorMax bounds how many results are held open at once. Each holds
	// at most one Athena page beyond what it has handed out.
	coldCursorMax = 16
	// coldReuseMinutes lets Athena answer an identical query — the same report
	// run twice — from its stored result instead of scanning S3 again.
	coldReuseMinutes = 60
)

// ColdReader queries archived telemetry Parquet in S3 via Athena.
//
// Every Athena query is billed for the whole partition it scans — the date
// range, for every device, because the table is not partitioned by device —
// and Athena bills per byte scanned (the archive also sat in Glacier Instant
// Retrieval until October 2026, which charged again for every byte read). A
// LIMIT does not shrink that. So a cursor-paged read must not
// start a query per page: one 10,000-row page of a week-long track costs the
// same ~13 GB scan as the whole week. The first page runs the query for the
// entire window; later pages are read from that query's stored result, which
// costs nothing further.
type ColdReader struct {
	client   athenaAPI
	database string
	table    string
	output   string // s3://.../ location for query results
	keyCol   string // identity column: "imei" for GPS, "driver" for mobile

	mu      sync.Mutex
	cursors map[string]*coldCursor
}

// coldCursor is a query whose result is being paged through.
type coldCursor struct {
	qid     *string
	token   *string // next GetQueryResults page
	header  bool    // the next page fetched is the first, whose row 0 is the header
	done    bool    // no pages left to fetch
	buf     []atypes.Row
	expires time.Time
}

func NewColdReader(ctx context.Context, database, table, output string) (*ColdReader, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &ColdReader{client: athena.NewFromConfig(cfg), database: database, table: table, output: output, keyCol: "imei",
		cursors: map[string]*coldCursor{}}, nil
}

// NewColdReaderFor is NewColdReader with an explicit identity column, so the
// same reader serves the mobile cold table (keyed by driver, own S3 folder).
func NewColdReaderFor(ctx context.Context, database, table, output, keyCol string) (*ColdReader, error) {
	c, err := NewColdReader(ctx, database, table, output)
	if err != nil {
		return nil, err
	}
	c.keyCol = keyCol
	return c, nil
}

// run starts a query, polls to completion and returns its execution id.
func (c *ColdReader) run(ctx context.Context, q string) (*string, error) {
	reuse := int32(coldReuseMinutes)
	start, err := c.client.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString:           &q,
		QueryExecutionContext: &atypes.QueryExecutionContext{Database: &c.database},
		ResultConfiguration:   &atypes.ResultConfiguration{OutputLocation: &c.output},
		ResultReuseConfiguration: &atypes.ResultReuseConfiguration{
			ResultReuseByAgeConfiguration: &atypes.ResultReuseByAgeConfiguration{Enabled: true, MaxAgeInMinutes: &reuse},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("athena start: %w", err)
	}
	qid := start.QueryExecutionId
	for {
		ex, err := c.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: qid})
		if err != nil {
			return nil, err
		}
		switch ex.QueryExecution.Status.State {
		case atypes.QueryExecutionStateSucceeded:
			return qid, nil
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

// timeBounds renders the shared WHERE fragment for a cold time window.
func timeBounds(lower, upper time.Time, lowerStrict, upperExclusive bool) (loOp, hiOp, loDate, hiDate string) {
	loOp, hiOp = ">=", "<="
	if lowerStrict {
		loOp = ">"
	}
	if upperExclusive {
		hiOp = "<"
	}
	return loOp, hiOp, lower.UTC().Format("2006-01-02"), upper.UTC().Format("2006-01-02")
}

// rowMs is a result row's time_ms, the first column of every cold SELECT.
func rowMs(r atypes.Row) int64 {
	if len(r.Data) == 0 || r.Data[0].VarCharValue == nil {
		return 0
	}
	ms, _ := strconv.ParseInt(*r.Data[0].VarCharValue, 10, 64)
	return ms
}

// take hands out the open cursor for key when this call continues it: a
// cursor page whose lower bound falls on a row the cursor still holds. The
// cursor leaves the map while in use, so two callers never share one.
func (c *ColdReader) take(key string, lower time.Time, lowerStrict bool) *coldCursor {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, ok := c.cursors[key]
	if !ok {
		return nil
	}
	delete(c.cursors, key)
	if !lowerStrict || time.Now().After(cur.expires) || len(cur.buf) == 0 || lower.UnixMilli() < rowMs(cur.buf[0]) {
		return nil
	}
	return cur
}

// keep stores cur for the next page, dropping expired cursors and, past
// coldCursorMax, the one closest to expiry.
func (c *ColdReader) keep(key string, cur *coldCursor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	cur.expires = now.Add(coldCursorTTL)
	c.cursors[key] = cur
	for k, v := range c.cursors {
		if now.After(v.expires) {
			delete(c.cursors, k)
		}
	}
	for len(c.cursors) > coldCursorMax {
		oldest := ""
		for k, v := range c.cursors {
			if oldest == "" || v.expires.Before(c.cursors[oldest].expires) {
				oldest = k
			}
		}
		delete(c.cursors, oldest)
	}
}

// rows returns up to limit result rows for id in the window, ascending. cols
// is the SELECT list; its first column must be time_ms. A call that continues
// the previous page of the same window reads on from that query's result
// instead of starting a new one (see ColdReader).
func (c *ColdReader) rows(ctx context.Context, cols, id string, lower, upper time.Time, lowerStrict, upperExclusive bool, limit int) ([]atypes.Row, error) {
	key := fmt.Sprintf("%s|%d|%t", id, upper.UnixMilli(), upperExclusive)
	cur := c.take(key, lower, lowerStrict)
	if cur == nil {
		loOp, hiOp, loDate, hiDate := timeBounds(lower, upper, lowerStrict, upperExclusive)
		q := fmt.Sprintf(`SELECT %s
FROM %s.%s
WHERE %s = '%s' AND date BETWEEN '%s' AND '%s'
  AND time_ms %s %d AND time_ms %s %d
ORDER BY time_ms`,
			cols, c.database, c.table, c.keyCol, id, loDate, hiDate,
			loOp, lower.UnixMilli(), hiOp, upper.UnixMilli())
		qid, err := c.run(ctx, q)
		if err != nil {
			return nil, err
		}
		cur = &coldCursor{qid: qid, header: true}
	}

	lo := lower.UnixMilli()
	after := func(r atypes.Row) bool {
		if lowerStrict {
			return rowMs(r) > lo
		}
		return rowMs(r) >= lo
	}
	var buf []atypes.Row
	for _, r := range cur.buf {
		if after(r) {
			buf = append(buf, r)
		}
	}
	for len(buf) < limit && !cur.done {
		res, err := c.client.GetQueryResults(ctx, &athena.GetQueryResultsInput{QueryExecutionId: cur.qid, NextToken: cur.token})
		if err != nil {
			return nil, err
		}
		page := res.ResultSet.Rows
		if cur.header && len(page) > 0 {
			page = page[1:]
		}
		cur.header = false
		for _, r := range page {
			if after(r) {
				buf = append(buf, r)
			}
		}
		if cur.token = res.NextToken; cur.token == nil {
			cur.done = true
		}
	}

	n := min(limit, len(buf))
	out := append([]atypes.Row(nil), buf[:n]...)
	// Keep what was not handed out, plus the last two rows that were: callers
	// over-fetch by one and resume from the last row they kept, so the next
	// page's lower bound lands on one of those two.
	cur.buf = append([]atypes.Row(nil), buf[max(0, n-2):]...)
	if len(cur.buf) > 0 {
		c.keep(key, cur)
	}
	return out, nil
}

// History returns cold points for imei in the time window, ascending, up to
// limit rows. lowerStrict makes the lower bound exclusive (cursor paging);
// upperExclusive makes the upper bound exclusive (used at the hot/cold boundary).
func (c *ColdReader) History(ctx context.Context, imei string, lower, upper time.Time, lowerStrict, upperExclusive bool, limit int) ([]Position, error) {
	rows, err := c.rows(ctx, `time_ms, received_ms, lat, lon, speed, bearing, ignition,
       kecamatan, kelurahan, kabupaten, kota, provinsi, jalan, metadata`,
		imei, lower, upper, lowerStrict, upperExclusive, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Position, 0, len(rows))
	for _, r := range rows {
		out = append(out, coldRow(imei, r.Data))
	}
	return out, nil
}

// HistoryMobile is History for the driver-keyed mobile cold table: same flow and
// same Parquet layout, minus `ignition` (phones have none) and returning
// MobilePosition.
func (c *ColdReader) HistoryMobile(ctx context.Context, driver string, lower, upper time.Time, lowerStrict, upperExclusive bool, limit int) ([]MobilePosition, error) {
	rows, err := c.rows(ctx, `time_ms, received_ms, lat, lon, speed, bearing,
       kecamatan, kelurahan, kabupaten, kota, provinsi, jalan, metadata`,
		driver, lower, upper, lowerStrict, upperExclusive, limit)
	if err != nil {
		return nil, err
	}
	out := make([]MobilePosition, 0, len(rows))
	for _, r := range rows {
		out = append(out, mobileColdRow(driver, r.Data))
	}
	return out, nil
}

// mobileColdRow maps an Athena result row to a MobilePosition. Column order
// matches the SELECT in HistoryMobile.
func mobileColdRow(driver string, d []atypes.Datum) MobilePosition {
	get := func(i int) *string {
		if i < len(d) {
			return d[i].VarCharValue
		}
		return nil
	}
	p := MobilePosition{Driver: driver}
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
	p.Kecamatan = str(get(6))
	p.Kelurahan = str(get(7))
	p.Kabupaten = str(get(8))
	p.Kota = str(get(9))
	p.Provinsi = str(get(10))
	p.Jalan = str(get(11))
	p.Metadata = stripRaw(get(12))
	return p
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
