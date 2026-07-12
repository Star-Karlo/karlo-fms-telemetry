// Command migrate backfills legacy MongoDB tracking exports into the cold
// Parquet store so old history is queryable via /v1/history. One-off, run as an
// on-demand ECS task; shard across tasks with SHARD_INDEX/SHARD_COUNT.
//
//	DATABASE_URL       postgres (PostGIS) — for geocoding
//	REDIS_ADDR         ElastiCache — geohash cache
//	SRC_BUCKET         source (default karlo-cold-storage)
//	DST_BUCKET         cold destination (default karlo-fms-cold-storage)
//	PREFIX             source prefix to process (e.g. trackings/2025/)
//	SHARD_INDEX/COUNT  split the file list across parallel tasks (default 0/1)
//	WORKERS            concurrent files (default 8) — also caps DB concurrency
//	GEOCODE            "false" to skip reverse geocoding (default true)
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/karlo/fms/internal/geocode"
	"github.com/karlo/fms/internal/migrate"
)

func main() {
	ctx := context.Background()

	src := env("SRC_BUCKET", "karlo-cold-storage")
	dst := env("DST_BUCKET", "karlo-fms-cold-storage")
	prefix := env("PREFIX", "")
	if prefix == "" {
		log.Fatal("PREFIX is required (e.g. trackings/2025/)")
	}
	shardIdx := envInt("SHARD_INDEX", 0)
	shardCnt := envInt("SHARD_COUNT", 1)
	workers := envInt("WORKERS", 8)
	doGeocode := env("GEOCODE", "true") != "false"

	awsCfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}
	s3c := s3.NewFromConfig(awsCfg)

	m := &migrate.Migrator{S3: s3c, Src: src, Dst: dst, TmpDir: env("TMPDIR", "/tmp")}

	if doGeocode {
		pool, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fms"))
		if err != nil {
			log.Fatalf("db connect: %v", err)
		}
		defer pool.Close()
		if err := pool.Ping(ctx); err != nil {
			log.Fatalf("db ping: %v", err)
		}
		rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
		if err := rdb.Ping(ctx).Err(); err != nil {
			log.Fatalf("redis ping: %v", err)
		}
		precision := uint(envInt("GEOHASH_PRECISION", 7))
		ttl := time.Duration(envInt("CACHE_TTL_HOURS", 720)) * time.Hour
		m.Geo = geocode.NewService(geocode.NewStore(pool), geocode.NewCache(rdb), precision, ttl)
		log.Printf("geocoding enabled")
	} else {
		log.Printf("geocoding DISABLED (region columns will be empty)")
	}

	// List all .json objects under PREFIX, keep this shard's slice.
	keys, err := listShard(ctx, s3c, src, prefix, shardIdx, shardCnt)
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	log.Printf("shard %d/%d: %d files under s3://%s/%s", shardIdx, shardCnt, len(keys), src, prefix)

	var done, skipped, failed, rows int64
	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				r, err := m.ProcessFile(ctx, key)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					log.Printf("ERROR %s: %v", key, err)
					continue
				}
				if r.Skipped {
					atomic.AddInt64(&skipped, 1)
				} else {
					atomic.AddInt64(&rows, int64(r.Rows))
				}
				n := atomic.AddInt64(&done, 1)
				if n%200 == 0 {
					log.Printf("progress: %d/%d files (skipped=%d failed=%d rows=%d)",
						n, len(keys), atomic.LoadInt64(&skipped), atomic.LoadInt64(&failed), atomic.LoadInt64(&rows))
				}
			}
		}()
	}
	for _, k := range keys {
		jobs <- k
	}
	close(jobs)
	wg.Wait()

	log.Printf("DONE: %d files (skipped=%d failed=%d), %d rows written",
		done, skipped, failed, rows)
	if failed > 0 {
		os.Exit(1)
	}
}

// listShard returns the .json keys under prefix that belong to this shard
// (stable lexicographic order from S3, sharded by index%count).
func listShard(ctx context.Context, c *s3.Client, bucket, prefix string, idx, cnt int) ([]string, error) {
	var keys []string
	var token *string
	i := 0
	for {
		out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix, ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			k := *o.Key
			if !strings.HasSuffix(k, ".json") {
				continue
			}
			if i%cnt == idx {
				keys = append(keys, k)
			}
			i++
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
