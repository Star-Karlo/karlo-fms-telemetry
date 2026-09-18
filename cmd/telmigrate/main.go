// Command telmigrate moves the telemetry database from Tiger Cloud Timescale
// to RDS Postgres, from inside the VPC.
//
//	telmigrate schema            apply the RDS schema (db/rds/schema.sql, embedded) to DST_DSN
//	telmigrate copy [days]       stream every table SRC_DSN -> DST_DSN; hypertables day by day
//	telmigrate verify [days]     row counts on both sides
//	telmigrate role              set telemetry_app's password from APP_DSN (the URL the services will use)
//	telmigrate catchup FROM TO   AFTER cutover: append rows Tiger received in [FROM,TO) (RFC3339) that RDS lacks
//	telmigrate indexes           build the forward-geocoding trigram indexes CONCURRENTLY on DST_DSN (safe on a live DB)
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
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/karlo/fms/db/rds"
)

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
	case "role":
		setAppRole(ctx, dst, os.Getenv("APP_DSN"))
	case "indexes":
		buildIndexes(ctx, dst)
	case "catchup":
		if len(os.Args) < 4 {
			log.Fatal("usage: telmigrate catchup <from RFC3339> <to RFC3339>")
		}
		from, err1 := time.Parse(time.RFC3339, os.Args[2])
		to, err2 := time.Parse(time.RFC3339, os.Args[3])
		if err1 != nil || err2 != nil || !to.After(from) {
			log.Fatal("catchup: bad window")
		}
		catchup(ctx, src, dst, from, to)
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

// buildIndexes creates the /search trigram indexes one statement at a time,
// CONCURRENTLY, outside any transaction. The same statements sit in
// schema.sql for a fresh database; on the LIVE one, running them inside the
// schema's single implicit transaction deadlocked against ingest (a GIN
// build takes a share lock on road/kelurahan while inserts and partition
// maintenance hold locks of their own).
func buildIndexes(ctx context.Context, dst string) {
	c := connect(ctx, dst, "dst")
	defer c.Close(ctx)
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_road_name_trgm ON road USING gin (name gin_trgm_ops)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_kelurahan_kel_trgm ON kelurahan USING gin (kel_name gin_trgm_ops)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_kelurahan_kec_trgm ON kelurahan USING gin (kec_name gin_trgm_ops)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_kelurahan_kab_trgm ON kelurahan USING gin (kab_name gin_trgm_ops)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_kelurahan_kab_bare_trgm ON kelurahan USING gin ((regexp_replace(kab_name, '^(Kota|Kabupaten) ', '')) gin_trgm_ops)`,
	}
	for _, st := range stmts {
		start := time.Now()
		if _, err := c.Exec(ctx, st); err != nil {
			log.Fatalf("%s: %v", st[:40], err)
		}
		fmt.Printf("%-60.60s %s\n", st, time.Since(start).Round(time.Second))
	}
}

// setAppRole gives telemetry_app the password carried in APP_DSN, then proves
// the URL works by connecting with it. The password reaches the task as a
// secret, never as an argument or an override.
func setAppRole(ctx context.Context, dst, appDSN string) {
	cfg, err := pgx.ParseConfig(appDSN)
	if err != nil || cfg.User == "" || cfg.Password == "" {
		log.Fatal("APP_DSN must be a full URL with user and password")
	}
	c := connect(ctx, dst, "dst")
	if _, err := c.Exec(ctx, fmt.Sprintf("ALTER ROLE %s PASSWORD %s", pgx.Identifier{cfg.User}.Sanitize(), quoteLiteral(cfg.Password))); err != nil {
		log.Fatalf("alter role: %v", err)
	}
	c.Close(ctx)
	app := connect(ctx, appDSN, "app")
	var n int
	if err := app.QueryRow(ctx, "SELECT count(*) FROM telemetry WHERE false").Scan(&n); err != nil {
		log.Fatalf("app role cannot read telemetry: %v", err)
	}
	app.Close(ctx)
	fmt.Printf("role %s: password set, connect + select ok\n", cfg.User)
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func applySchema(ctx context.Context, dst string) {
	c := connect(ctx, dst, "dst")
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, rds.Schema); err != nil {
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
			// The schema only pre-creates yesterday..+3; older days need their
			// partition first. In its OWN transaction: CREATE TABLE ... PARTITION
			// OF takes ACCESS EXCLUSIVE on the parent, and holding that for the
			// length of a day's copy would stall ingest once it writes here.
			part := fmt.Sprintf("%s_%s", t, from.Format("20060102"))
			if _, err := dst.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
				pgx.Identifier{part}.Sanitize(), pgx.Identifier{t}.Sanitize(), from.Format("2006-01-02"), to.Format("2006-01-02"))); err != nil {
				log.Fatalf("partition %s: %v", part, err)
			}
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

// catchup is the post-cutover tool. `copy` replaces whole tables, which is
// right while RDS is idle and wrong the moment ingest writes there: it would
// clobber `latest` and every event the new tasks stored. This instead
// appends what Tiger received in the window and RDS lacks, matched on the
// natural key (never the id — both databases have been generating those
// independently since the roll). Aggregates (vehicle_day, coverage_*) are
// left alone: a few minutes' gap in a rollup is noise; a clobber is not.
func catchup(ctx context.Context, srcDSN, dstDSN string, from, to time.Time) {
	if srcDSN == "" {
		log.Fatal("SRC_DSN is not set")
	}
	src := connect(ctx, srcDSN, "src")
	defer src.Close(ctx)
	dst := connect(ctx, dstDSN, "dst")
	defer dst.Close(ctx)

	type spec struct {
		table, timeCol string
		cols           []string // empty = every column; set to omit generated ids
		notExists      string   // correlates tmp t with live x
	}
	specs := []spec{
		{"telemetry", "time", nil, `x.imei = t.imei AND x."time" = t."time"`},
		{"mobile_telemetry", "time", nil, `x.driver = t.driver AND x."time" = t."time"`},
		{"green_driving_events", "received_at", []string{"imei", "event_type", "value", "duration_ms", "occurred_at", "lat", "lon", "speed", "raw", "received_at"},
			`x.imei = t.imei AND x.occurred_at = t.occurred_at AND x.event_type = t.event_type`},
		{"dashcam_alarms", "received_at", []string{"device_id", "alarm_id", "code", "occurred_at", "lat", "lon", "speed", "raw", "received_at", "gforce"},
			`x.device_id = t.device_id AND x.occurred_at = t.occurred_at AND x.code = t.code`},
	}
	for _, sp := range specs {
		cols := sp.cols
		if cols == nil {
			cols = columns(ctx, src, sp.table)
		}
		list := strings.Join(cols, ",")
		start := time.Now()
		tx, err := dst.Begin(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TEMP TABLE t_catchup ON COMMIT DROP AS SELECT %s FROM %s WHERE false`, list, sp.table)); err != nil {
			log.Fatalf("%s temp: %v", sp.table, err)
		}
		sel := fmt.Sprintf(`SELECT %s FROM %s WHERE %q >= '%s' AND %q < '%s'`, list, sp.table, sp.timeCol, from.Format(time.RFC3339), sp.timeCol, to.Format(time.RFC3339))
		got := stream(ctx, src, tx.Conn(), sel, fmt.Sprintf("COPY t_catchup(%s)", list))
		tag, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(%s) SELECT %s FROM t_catchup t WHERE NOT EXISTS (SELECT 1 FROM %s x WHERE %s)`,
			sp.table, list, list, sp.table, sp.notExists))
		if err != nil {
			log.Fatalf("%s insert: %v", sp.table, err)
		}
		if err := tx.Commit(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-22s tiger had %8d in window, appended %8d  %s\n", sp.table, got, tag.RowsAffected(), time.Since(start).Round(time.Second))
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
