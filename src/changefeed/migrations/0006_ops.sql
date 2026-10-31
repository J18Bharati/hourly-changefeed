-- M5b: raw bodies are pruned after 14 days (except each board's latest), so the log keeps the
-- row and loses the body; and a daily record of how big everything is.
ALTER TABLE fetch_log DROP CONSTRAINT fetch_log_body_hash_fkey;
ALTER TABLE fetch_log ADD CONSTRAINT fetch_log_body_hash_fkey
    FOREIGN KEY (body_hash) REFERENCES raw_bodies (hash) ON DELETE SET NULL;
CREATE INDEX fetch_log_body_hash ON fetch_log (body_hash);

CREATE TABLE storage_reports (
    at             timestamptz PRIMARY KEY DEFAULT now(),
    db_bytes       bigint NOT NULL,
    raw_bytes      bigint NOT NULL,  -- on disk, after lz4
    raw_rows       integer NOT NULL,
    raw_bytes_24h  bigint NOT NULL,  -- uncompressed bytes of bodies first seen in the last day
    versions_24h   integer NOT NULL,
    pruned_rows    integer NOT NULL DEFAULT 0,
    pruned_bytes   bigint NOT NULL DEFAULT 0
);
