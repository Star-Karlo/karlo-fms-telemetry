-- 0020_ingest_key.sql — revocable API keys for POST /v1/ingest.
--
-- Replaces the "one shared secret in INGEST_API_KEY" model with a set of named
-- keys that can be issued and revoked independently: deleting one key does not
-- affect any other sender. The shared env key keeps working alongside these
-- (see internal/ingest/key.go) so existing senders are unaffected.
--
-- Only the sha256 of each key is stored — the plaintext is shown once at
-- creation and is unrecoverable afterwards. key_prefix exists purely so a key
-- can be identified in listings without revealing it.

CREATE TABLE ingest_key (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name         text NOT NULL,                    -- human label, e.g. "teltonika-relay"
  key_prefix   text NOT NULL,                    -- leading chars, for display only
  key_hash     bytea NOT NULL UNIQUE,            -- sha256(plaintext key)
  created_at   timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz,                      -- updated opportunistically on use
  revoked_at   timestamptz                       -- non-null = dead, rejected at auth
);

-- Auth looks up active keys by hash on every cache miss; partial index keeps it
-- to the live rows only.
CREATE INDEX idx_ingest_key_active ON ingest_key (key_hash) WHERE revoked_at IS NULL;
