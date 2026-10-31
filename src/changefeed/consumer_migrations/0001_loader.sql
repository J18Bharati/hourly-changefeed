-- Part B M7: the consumer's copy of the change feed, loaded from hourly change files only.

-- One row per hourly file loaded: the ledger that makes a reload a no-op and the chain checkable.
CREATE TABLE loaded_files (
    hour                 timestamptz PRIMARY KEY,
    file                 text NOT NULL,
    rows                 integer NOT NULL,
    sha256               text NOT NULL,
    manifest_sha256      text NOT NULL UNIQUE,
    prev_manifest_sha256 text UNIQUE,  -- each manifest has exactly one successor
    runs                 integer NOT NULL,
    missed               text,         -- why no run happened: the hour is a gap, not a quiet hour
    loaded_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE postings (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source      text NOT NULL,
    board       text NOT NULL,
    external_id text NOT NULL,
    UNIQUE (source, board, external_id)
);

-- The producer's valid time strictly increases per (posting, field), so it is the key.
CREATE TABLE field_versions (
    posting_id        bigint NOT NULL REFERENCES postings (id),
    field             text NOT NULL,
    valid_from        timestamptz NOT NULL,
    value             jsonb,
    valid_from_source text NOT NULL,
    recorded_at       timestamptz NOT NULL,  -- the producer's knowledge time, not our load time
    run_id            bigint,
    baseline          boolean NOT NULL,
    hour              timestamptz NOT NULL REFERENCES loaded_files (hour),
    PRIMARY KEY (posting_id, field, valid_from),
    CHECK (valid_from <= recorded_at)
);
