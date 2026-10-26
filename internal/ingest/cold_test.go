package ingest

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	atypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// fakeAthena answers every query with the same rows, served in pages of
// pageSize behind a header row, the way GetQueryResults does.
type fakeAthena struct {
	times    []int64
	pageSize int
	queries  []string
	reuse    []bool
}

func (f *fakeAthena) StartQueryExecution(_ context.Context, in *athena.StartQueryExecutionInput, _ ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
	f.queries = append(f.queries, *in.QueryString)
	r := in.ResultReuseConfiguration
	f.reuse = append(f.reuse, r != nil && r.ResultReuseByAgeConfiguration != nil && r.ResultReuseByAgeConfiguration.Enabled)
	id := strconv.Itoa(len(f.queries))
	return &athena.StartQueryExecutionOutput{QueryExecutionId: &id}, nil
}

func (f *fakeAthena) GetQueryExecution(_ context.Context, in *athena.GetQueryExecutionInput, _ ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
	return &athena.GetQueryExecutionOutput{QueryExecution: &atypes.QueryExecution{
		QueryExecutionId: in.QueryExecutionId,
		Status:           &atypes.QueryExecutionStatus{State: atypes.QueryExecutionStateSucceeded},
	}}, nil
}

func (f *fakeAthena) GetQueryResults(_ context.Context, in *athena.GetQueryResultsInput, _ ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
	all := []atypes.Row{{Data: []atypes.Datum{{VarCharValue: strPtr("time_ms")}}}}
	for _, t := range f.times {
		all = append(all, atypes.Row{Data: []atypes.Datum{{VarCharValue: strPtr(strconv.FormatInt(t, 10))}}})
	}
	from := 0
	if in.NextToken != nil {
		from, _ = strconv.Atoi(*in.NextToken)
	}
	to := min(from+f.pageSize, len(all))
	out := &athena.GetQueryResultsOutput{ResultSet: &atypes.ResultSet{Rows: all[from:to]}}
	if to < len(all) {
		next := strconv.Itoa(to)
		out.NextToken = &next
	}
	return out, nil
}

func strPtr(s string) *string { return &s }

func newFakeCold(times []int64, pageSize int) (*ColdReader, *fakeAthena) {
	f := &fakeAthena{times: times, pageSize: pageSize}
	return &ColdReader{client: f, database: "fms", table: "telemetry_cold", output: "s3://x/", keyCol: "imei",
		cursors: map[string]*coldCursor{}}, f
}

// walk pages through the window the way Reader.History does: over-fetch by
// one, hand out limit, resume strictly after the last row handed out.
func walk(t *testing.T, c *ColdReader, from, upper time.Time, limit int) []int64 {
	t.Helper()
	var got []int64
	lower, strict := from, false
	for range 100 {
		ps, err := c.History(context.Background(), "354002392595920", lower, upper, strict, true, limit+1)
		if err != nil {
			t.Fatal(err)
		}
		if len(ps) > limit {
			ps = ps[:limit]
			for _, p := range ps {
				got = append(got, p.Time.UnixMilli())
			}
			lower, strict = ps[limit-1].Time, true
			continue
		}
		for _, p := range ps {
			got = append(got, p.Time.UnixMilli())
		}
		return got
	}
	t.Fatal("walk did not finish")
	return nil
}

// TestColdPagingRunsOneQuery pins the fix for the October 2026 Glacier bill:
// every page of a cursor walk used to start its own Athena query, and each
// one scanned the whole week for every device. A walk is one query now.
func TestColdPagingRunsOneQuery(t *testing.T) {
	var times []int64
	for i := range 57 {
		times = append(times, 1_000_000+int64(i)*1000)
	}
	c, f := newFakeCold(times, 7)
	got := walk(t, c, time.UnixMilli(0), time.UnixMilli(2_000_000), 10)

	if len(f.queries) != 1 {
		t.Fatalf("a cursor walk started %d queries, want 1", len(f.queries))
	}
	if len(got) != len(times) {
		t.Fatalf("walk returned %d rows, want %d", len(got), len(times))
	}
	for i := range times {
		if got[i] != times[i] {
			t.Fatalf("row %d = %d, want %d (gap or repeat across pages)", i, got[i], times[i])
		}
	}
	if strings.Contains(f.queries[0], "LIMIT") {
		t.Error("the query carries a LIMIT, so later pages could not be read from its result")
	}
	if !f.reuse[0] {
		t.Error("result reuse is off, so an identical report re-run scans S3 again")
	}
}

// Rows sharing the cursor's millisecond are skipped, exactly as the strict
// `time_ms >` of a fresh per-page query skipped them.
func TestColdPagingSameMillisecondAtCursor(t *testing.T) {
	times := []int64{1, 2, 3, 3, 3, 4, 5}
	c, _ := newFakeCold(times, 2)
	got := walk(t, c, time.UnixMilli(0), time.UnixMilli(10), 3)
	want := []int64{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// A call that does not continue the open cursor — a fresh first page, or a
// different window — starts its own query rather than reading another's.
func TestColdPagingRestartIsANewQuery(t *testing.T) {
	c, f := newFakeCold([]int64{1, 2, 3, 4, 5, 6}, 2)
	ctx := context.Background()
	if _, err := c.History(ctx, "354002392595920", time.UnixMilli(0), time.UnixMilli(10), false, true, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.History(ctx, "354002392595920", time.UnixMilli(0), time.UnixMilli(10), false, true, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.History(ctx, "354002392595920", time.UnixMilli(2), time.UnixMilli(11), true, true, 3); err != nil {
		t.Fatal(err)
	}
	if len(f.queries) != 3 {
		t.Fatalf("started %d queries, want 3", len(f.queries))
	}
}

func TestColdBoundaryIsMidnightUTC(t *testing.T) {
	b := coldBoundary(7)
	if !b.Equal(b.Truncate(24 * time.Hour)) {
		t.Fatalf("boundary %s is not midnight UTC", b)
	}
	if age := time.Since(b); age < 7*24*time.Hour || age >= 8*24*time.Hour {
		t.Fatalf("boundary %s is %s old, want between 7 and 8 days", b, age)
	}
}
