-- Part B M6: which hourly change files have been written, and their hashes (the manifest chain).
CREATE TABLE emitted_hours (
    hour            timestamptz PRIMARY KEY,
    rows            integer NOT NULL,
    sha256          text NOT NULL,  -- of the .jsonl file
    manifest_sha256 text NOT NULL,  -- of the .manifest.json file; the next manifest points at it
    emitted_at      timestamptz NOT NULL DEFAULT now()
);
