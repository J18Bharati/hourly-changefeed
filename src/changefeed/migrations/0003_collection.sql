-- M3: scheduling, the fetch log, raw bodies, and the current set of postings per board.

-- Each board carries its own schedule and lease (the claim pattern from gator, migration 0005).
CREATE SEQUENCE board_claim_token;
ALTER TABLE boards
    ADD COLUMN poll_interval        interval    NOT NULL DEFAULT '1 hour',
    ADD COLUMN next_fetch_at        timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN last_attempt_at      timestamptz,
    ADD COLUMN last_fetched_at      timestamptz,
    ADD COLUMN consecutive_failures integer     NOT NULL DEFAULT 0,
    ADD COLUMN claimed_until        timestamptz,
    ADD COLUMN claim_token          bigint,
    ADD COLUMN disabled_at          timestamptz,
    ADD COLUMN disabled_reason      text,
    ADD COLUMN etag                 text,
    ADD COLUMN last_body_hash       text;  -- sha256 of the last raw body stored from this board
CREATE INDEX boards_due ON boards (next_fetch_at, id) WHERE status = 'active' AND disabled_at IS NULL;

-- One row per `changefeed run`. M5a adds the hours that had no run at all.
CREATE TABLE collection_runs (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status      text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'ok', 'partial', 'missed')),
    reason      text,
    counts      jsonb
);

-- Every response body we parsed or tried to, once per distinct body. lz4 because these are
-- the bulk of the database; M5b prunes all but the latest per board after 14 days.
CREATE TABLE raw_bodies (
    hash          text PRIMARY KEY,  -- sha256 of the bytes as received (after Content-Encoding)
    body          bytea COMPRESSION lz4 NOT NULL,
    bytes         integer NOT NULL,
    first_board   bigint REFERENCES boards (id) ON DELETE SET NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE fetch_log (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    board_id    bigint NOT NULL REFERENCES boards (id) ON DELETE CASCADE,
    run_id      bigint REFERENCES collection_runs (id) ON DELETE SET NULL,
    started_at  timestamptz NOT NULL,
    duration_ms integer NOT NULL,
    outcome     text NOT NULL CHECK (outcome IN ('ok', 'not_modified', 'unchanged', 'stale', 'http_error',
                    'network_error', 'parse_error', 'refused', 'lease_lost', 'crashed')),
    baseline    boolean NOT NULL DEFAULT false,  -- the board's first successful parse: not "changes"
    status      smallint,
    headers     jsonb,                           -- an allowlist, to learn what each API really sends
    body_hash   text REFERENCES raw_bodies (hash),
    bytes       integer,
    postings    integer,
    error       text,
    retry_after timestamptz
);
CREATE INDEX fetch_log_board ON fetch_log (board_id, id DESC);

-- The postings in each board's latest accepted body. M4 adds the per-field history.
CREATE TABLE postings_current (
    board_id      bigint NOT NULL REFERENCES boards (id) ON DELETE CASCADE,
    external_id   text NOT NULL,
    fields        jsonb NOT NULL,
    source_ts     timestamptz,  -- the source's own last-changed time, if it gives one; never versioned
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (board_id, external_id)
);
