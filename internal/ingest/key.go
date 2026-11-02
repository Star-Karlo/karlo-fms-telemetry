package ingest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// KeyPrefix marks a value as an FMS ingest key. Kept distinct from the dashboard's
// "fms_" API tokens so the two are never confused in a config file.
const KeyPrefix = "fms_ingest_"

// keyCacheTTL bounds how long a validation result is reused before hitting the
// DB again. It is also the worst-case delay before a revoked key stops working.
const keyCacheTTL = 60 * time.Second

// KeyStore validates X-Ingest-Key values against the ingest_key table.
//
// Ingest is the highest-volume endpoint, so results are cached in-process:
// a hot key costs one map read, not a query per reading. Revocation therefore
// takes effect within keyCacheTTL rather than instantly.
type KeyStore struct {
	pool *pgxpool.Pool

	mu    sync.RWMutex
	cache map[[32]byte]cachedKey
}

type cachedKey struct {
	valid   bool
	name    string // the key's label, cached alongside the verdict
	expires time.Time
}

func NewKeyStore(pool *pgxpool.Pool) *KeyStore {
	return &KeyStore{pool: pool, cache: make(map[[32]byte]cachedKey)}
}

// Valid reports whether presented is an active (existing, unrevoked) ingest key.
// Failures to reach the DB deny the request rather than opening the endpoint.
func (k *KeyStore) Valid(ctx context.Context, presented string) bool {
	_, ok := k.Identify(ctx, presented)
	return ok
}

// Identify is Valid plus the key's name, which the ingest handler stamps on
// every reading so a stored row says which sender supplied it. The name is
// returned from the same cache entry as the verdict, so attribution costs a
// map read, not a second query.
func (k *KeyStore) Identify(ctx context.Context, presented string) (string, bool) {
	if presented == "" {
		return "", false
	}
	sum := sha256.Sum256([]byte(presented))

	k.mu.RLock()
	c, ok := k.cache[sum]
	k.mu.RUnlock()
	if ok && time.Now().Before(c.expires) {
		return c.name, c.valid
	}

	// Negative results are cached too, so a misconfigured sender retrying in a
	// loop cannot turn into a query storm.
	var id int64
	var name string
	err := k.pool.QueryRow(ctx,
		`SELECT id, name FROM ingest_key WHERE key_hash = $1 AND revoked_at IS NULL`,
		sum[:],
	).Scan(&id, &name)
	valid := err == nil
	if !valid {
		name = ""
	}

	k.mu.Lock()
	k.cache[sum] = cachedKey{valid: valid, name: name, expires: time.Now().Add(keyCacheTTL)}
	k.mu.Unlock()

	if valid {
		k.touch(id)
	}
	return name, valid
}

// touch records last_used_at without blocking the request. Best-effort: a lost
// update here is not worth failing an ingest over.
func (k *KeyStore) touch(id int64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = k.pool.Exec(ctx, `UPDATE ingest_key SET last_used_at = now() WHERE id = $1`, id)
	}()
}

// GenerateKey returns a new random key and its sha256. The plaintext is the only
// copy that will ever exist — the caller must show it to the operator once.
func GenerateKey() (plaintext string, hash []byte, err error) {
	buf := make([]byte, 24)
	if _, err = rand.Read(buf); err != nil {
		return "", nil, err
	}
	plaintext = KeyPrefix + base64.RawURLEncoding.EncodeToString(buf)
	sum := sha256.Sum256([]byte(plaintext))
	return plaintext, sum[:], nil
}

// DisplayPrefix is the portion of a key safe to store and show in listings.
func DisplayPrefix(plaintext string) string {
	const n = len(KeyPrefix) + 6
	if len(plaintext) < n {
		return plaintext
	}
	return plaintext[:n]
}
