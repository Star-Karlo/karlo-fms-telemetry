// Command ingestkey issues and revokes keys for POST /v1/ingest.
//
// Keys are stored as sha256 only: `create` prints the plaintext once and it
// cannot be recovered afterwards — reissue if it is lost. Revoking one key never
// affects any other sender.
//
//	DATABASE_URL=postgres://... go run ./cmd/ingestkey create teltonika-relay
//	DATABASE_URL=postgres://... go run ./cmd/ingestkey list
//	DATABASE_URL=postgres://... go run ./cmd/ingestkey rename 3 mast-gateway
//	DATABASE_URL=postgres://... go run ./cmd/ingestkey revoke 3
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/karlo/fms/internal/ingest"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
	}

	ctx := context.Background()
	// DATABASE_URL, or DST_DSN so the tool also runs inside the dbtool task
	// (whose secret of that name is the telemetry database as admin).
	pool, err := pgxpool.New(ctx, env("DATABASE_URL", env("DST_DSN", "postgres://postgres:postgres@localhost:5432/fms")))
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	switch os.Args[1] {
	case "create":
		if len(os.Args) < 3 {
			log.Fatal("usage: ingestkey create <name>   (e.g. teltonika-relay)")
		}
		create(ctx, pool, os.Args[2])
	case "list":
		list(ctx, pool)
	case "revoke":
		if len(os.Args) < 3 {
			log.Fatal("usage: ingestkey revoke <id>   (see `ingestkey list`)")
		}
		revoke(ctx, pool, os.Args[2])
	case "rename":
		if len(os.Args) < 4 {
			log.Fatal("usage: ingestkey rename <id> <new-name>   (see `ingestkey list`)")
		}
		rename(ctx, pool, os.Args[2], os.Args[3])
	default:
		usage()
	}
}

func create(ctx context.Context, pool *pgxpool.Pool, name string) {
	plaintext, hash, err := ingest.GenerateKey()
	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	var id int64
	err = pool.QueryRow(ctx,
		`INSERT INTO ingest_key (name, key_prefix, key_hash) VALUES ($1, $2, $3) RETURNING id`,
		name, ingest.DisplayPrefix(plaintext), hash,
	).Scan(&id)
	if err != nil {
		log.Fatalf("insert: %v", err)
	}

	fmt.Printf("\nIngest key created  (id=%d, name=%q)\n\n", id, name)
	fmt.Printf("  %s\n\n", plaintext)
	fmt.Println("This is the only time the key is shown — store it now.")
	fmt.Println("Send it over a secrets channel (password manager), not email or chat.")
	fmt.Printf("\nUsage:\n  curl -X POST https://fms-tracking.karlo.id/v1/ingest \\\n")
	fmt.Printf("    -H 'X-Ingest-Key: %s' \\\n", plaintext)
	fmt.Printf("    -H 'Content-Type: application/json' \\\n")
	fmt.Printf("    -d '{\"imei\":\"...\",\"lat\":-6.17,\"lon\":106.82,\"speed\":0}'\n\n")
}

func list(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx,
		`SELECT id, name, key_prefix, created_at, last_used_at, revoked_at
		 FROM ingest_key ORDER BY id`)
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-24s %-20s %-12s %-20s %s\n", "ID", "NAME", "PREFIX", "STATUS", "LAST USED", "CREATED")
	for rows.Next() {
		var (
			id                  int64
			name, prefix        string
			created             time.Time
			lastUsed, revokedAt *time.Time
		)
		if err := rows.Scan(&id, &name, &prefix, &created, &lastUsed, &revokedAt); err != nil {
			log.Fatalf("scan: %v", err)
		}
		status := "active"
		if revokedAt != nil {
			status = "REVOKED"
		}
		fmt.Printf("%-4d %-24s %-20s %-12s %-20s %s\n",
			id, name, prefix+"…", status, ts(lastUsed), created.Format("2006-01-02"))
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows: %v", err)
	}
}

func revoke(ctx context.Context, pool *pgxpool.Pool, id string) {
	tag, err := pool.Exec(ctx,
		`UPDATE ingest_key SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		log.Fatalf("revoke: %v", err)
	}
	if tag.RowsAffected() == 0 {
		log.Fatalf("no active key with id=%s (already revoked, or wrong id — check `ingestkey list`)", id)
	}
	fmt.Printf("Key id=%s revoked. Takes effect within 60s (running services cache validations).\n", id)
}

func rename(ctx context.Context, pool *pgxpool.Pool, id, name string) {
	// Label only — the key value (hash) is untouched, so senders keep working.
	tag, err := pool.Exec(ctx,
		`UPDATE ingest_key SET name = $2 WHERE id = $1`, id, name)
	if err != nil {
		log.Fatalf("rename: %v", err)
	}
	if tag.RowsAffected() == 0 {
		log.Fatalf("no key with id=%s (check `ingestkey list`)", id)
	}
	fmt.Printf("Key id=%s renamed to %q. The key value is unchanged — senders keep working.\n", id, name)
}

func ts(t *time.Time) string {
	if t == nil {
		return "never"
	}
	return t.Format("2006-01-02 15:04")
}

func usage() {
	log.Fatal("usage: ingestkey <create <name> | list | rename <id> <new-name> | revoke <id>>")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
