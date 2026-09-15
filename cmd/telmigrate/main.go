// Command telmigrate moves the telemetry database from Tiger Cloud Timescale
// to RDS Postgres, from inside the VPC.
//
//	telmigrate schema            apply the RDS schema (db/rds/schema.sql, embedded) to DST_DSN
//	telmigrate copy [days]       stream every table SRC_DSN -> DST_DSN; hypertables day by day
//	telmigrate verify [days]     row counts on both sides
//
// Streaming COPY, one table at a time, and the two hypertables one DAY at a
// time. A day is ~1.7 M rows; streaming keeps memory flat regardless, and
// per-day bounds mean a failure part-way costs one day's re-copy, not the
// whole week. Each day is written inside a transaction that first deletes
// that day on the destination, so a re-run converges rather than duplicating.
//
// Rides in the ingest image (distroless): no psql, no pg_dump — and Tiger runs
// PostgreSQL 18, which a client-side pg_dump 17 refuses anyway. Reads on the
// source decompress Timescale chunks transparently; the destination sees
// plain rows and routes them to the right daily partition.
package main

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed schema.sql
var schemaSQL string

// tables, in dependency order. The two hypertables go last and day by day.
var plainTables = []string{
	"tenants", "ingest_key", "device_generation", "geofences",
	"kelurahan", "road", "coverage_grid", "coverage_signal", "coverage_rollup_log",
	"green_driving_events", "dashcam_alarms", "vehicle_day",
	"latest", "mobile_latest",
}
var dayTables = []string{"telemetry", "mobile_telemetry"}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: telmigrate schema | copy [days] | verify [days]")
	}
	ctx := context.Background()
	src, dst := os.Getenv("SRC_DSN"), os.Getenv("DST_DSN")
	if dst == "" {
		log.Fatal("DST_DSN is not set")
	}
	days := 8
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			days = n
		}
	}
	switch os.Args[1] {
	case "schema":
		applySchema(ctx, dst)
	case "copy":
		copyAll(ctx, src, dst, days)
	case "verify":
		verify(ctx, src, dst, days)
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}

func connect(ctx context.Context, dsn, what string) *pgx.Conn {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect %s: %v", what, err)
	}
	return c
}

func applySchema(ctx context.Context, dst string) {
	c := connect(ctx, dst, "dst")
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, schemaSQL); err != nil {
		log.Fatalf("schema: %v", err)
	}
	var n int
	_ = c.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname='public'`).Scan(&n)
	fmt.Printf("schema applied: %d tables\n", n)
}

// columns of a table on the SOURCE, so the COPY lists them explicitly and a
// column-order difference between the two schemas cannot silently shift data.
func columns(ctx context.Context, c *pgx.Conn, table string) []string {
	rows, err := c.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position`, table)
	if err != nil {
		log.Fatalf("columns %s: %v", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		out = append(out, pgx.Identifier{n}.Sanitize())
	}
	return out
}

// stream runs COPY TO on src and COPY FROM on dst joined by a pipe, so no row
// is ever held in memory beyond the pipe buffer.
func stream(ctx context.Context, src, dst *pgx.Conn, selectSQL, copyFromSQL string) int64 {
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		_, err := src.PgConn().CopyTo(ctx, pw, "COPY ("+selectSQL+") TO STDOUT WITH (FORMAT csv)")
		pw.CloseWithError(err)
		errc <- err
	}()
	tag, err := dst.PgConn().CopyFrom(ctx, pr, copyFromSQL+" FROM STDIN WITH (FORMAT csv)")
	if err != nil {
		log.Fatalf("copy from: %v", err)
	}
	if err := <-errc; err != nil {
		log.Fatalf("copy to: %v", err)
	}
	return tag.RowsAffected()
}

func copyAll(ctx context.Context, srcDSN, dstDSN string, days int) {
	if srcDSN == "" {
		log.Fatal("SRC_DSN is not set")
	}
	src := connect(ctx, srcDSN, "src")
	defer src.Close(ctx)
	dst := connect(ctx, dstDSN, "dst")
	defer dst.Close(ctx)

	for _, t := range plainTables {
		cols := columns(ctx, src, t)
		if len(cols) == 0 {
			fmt.Printf("%-24s absent on source, skipped\n", t)
			continue
		}
		list := strings.Join(cols, ",")
		start := time.Now()
		// Whole-table tables are replaced atomically. DELETE rather than
		// TRUNCATE: tenants is referenced by geofences and TRUNCATE refuses
		// that even inside one transaction; the order above has referenced
		// tables first, so re-runs converge.
		tx, err := dst.Begin(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+pgx.Identifier{t}.Sanitize()); err != nil {
			log.Fatalf("clear %s: %v", t, err)
		}
		n := stream(ctx, src, tx.Conn(), fmt.Sprintf("SELECT %s FROM %s", list, t), fmt.Sprintf("COPY %s(%s)", t, list))
		if err := tx.Commit(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-24s %9d rows  %s\n", t, n, time.Since(start).Round(time.Second))
	}

	// Hypertables: one day per transaction, oldest first, bounded by `days`.
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for _, t := range dayTables {
		cols := columns(ctx, src, t)
		list := strings.Join(cols, ",")
		for d := days - 1; d >= -1; d-- { // -1 = tomorrow, for fixes timestamped just ahead
			from := today.AddDate(0, 0, -d)
			to := from.AddDate(0, 0, 1)
			start := time.Now()
			tx, err := dst.Begin(ctx)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE "time" >= $1 AND "time" < $2`, t), from, to); err != nil {
				log.Fatalf("clear %s %s: %v", t, from.Format("2006-01-02"), err)
			}
			sel := fmt.Sprintf(`SELECT %s FROM %s WHERE "time" >= '%s' AND "time" < '%s'`, list, t, from.Format(time.RFC3339), to.Format(time.RFC3339))
			n := stream(ctx, src, tx.Conn(), sel, fmt.Sprintf("COPY %s(%s)", t, list))
			if err := tx.Commit(ctx); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%-16s %s %9d rows  %s\n", t, from.Format("2006-01-02"), n, time.Since(start).Round(time.Second))
		}
	}
}

func verify(ctx context.Context, srcDSN, dstDSN string, days int) {
	src := connect(ctx, srcDSN, "src")
	defer src.Close(ctx)
	dst := connect(ctx, dstDSN, "dst")
	defer dst.Close(ctx)
	count := func(c *pgx.Conn, sql string) int64 {
		var n int64
		if err := c.QueryRow(ctx, sql).Scan(&n); err != nil {
			return -1
		}
		return n
	}
	bad := 0
	for _, t := range plainTables {
		a, b := count(src, "SELECT count(*) FROM "+t), count(dst, "SELECT count(*) FROM "+t)
		mark := "  "
		if a != b {
			mark = "!!"
			bad++
		}
		fmt.Printf("%s %-24s src %9d  dst %9d\n", mark, t, a, b)
	}
	since := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -(days - 1))
	for _, t := range dayTables {
		q := fmt.Sprintf(`SELECT count(*) FROM %s WHERE "time" >= '%s'`, t, since.Format(time.RFC3339))
		a, b := count(src, q), count(dst, q)
		mark := "  "
		if a != b {
			mark = "!!"
			bad++
		}
		fmt.Printf("%s %-24s src %9d  dst %9d   (since %s)\n", mark, t, a, b, since.Format("2006-01-02"))
	}
	if bad > 0 {
		os.Exit(1)
	}
}
