-- M4: per-field history of every posting, with valid time (when it was true) and recorded time
-- (when we learned it), so a query can ask what was known at any moment, not just what is known now.

ALTER TABLE postings_current
    ADD COLUMN id           bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    ADD COLUMN content_hash text,
    ADD COLUMN present      boolean NOT NULL DEFAULT true,  -- false once absence is confirmed
    ADD COLUMN absent_count integer NOT NULL DEFAULT 0,     -- agreeing fetches without it, so far
    ADD COLUMN absent_since timestamptz;                    -- the first of those fetches

-- The transaction time of the store, which is what versions record. Rows from before this
-- migration get the end of their fetch, the closest thing they have.
ALTER TABLE fetch_log
    ADD COLUMN recorded_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN versions    integer;
UPDATE fetch_log SET recorded_at = started_at + duration_ms * interval '1 millisecond';

ALTER TABLE boards ADD COLUMN normalizer_version integer;  -- of the postings_current rows for this board

CREATE TABLE posting_field_versions (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    posting_id        bigint NOT NULL REFERENCES postings_current (id) ON DELETE CASCADE,
    field             text NOT NULL,  -- an adapter field, or 'present'
    value             jsonb,
    valid_from        timestamptz NOT NULL,
    valid_from_source text NOT NULL CHECK (valid_from_source IN ('source', 'observed', 'clamped')),
    recorded_at       timestamptz NOT NULL,
    run_id            bigint REFERENCES collection_runs (id) ON DELETE SET NULL,
    fetch_log_id      bigint REFERENCES fetch_log (id) ON DELETE SET NULL,
    CHECK (valid_from <= recorded_at)
);
CREATE INDEX posting_field_versions_lookup ON posting_field_versions (posting_id, field, valid_from DESC, id DESC);
CREATE INDEX posting_field_versions_recorded ON posting_field_versions (recorded_at);  -- hourly change files

CREATE VIEW posting_field_history AS
SELECT v.*, lead(valid_from) OVER (PARTITION BY posting_id, field ORDER BY valid_from, id) AS valid_to
FROM posting_field_versions v;

-- The postings that were open at `valid`, as we knew them at `known`. as_of(t, now()) is "as it
-- stands"; as_of(t, t) is "as it stood", which is what a backtest at t was allowed to see.
CREATE FUNCTION postings_as_of(valid timestamptz, known timestamptz)
RETURNS TABLE (posting_id bigint, fields jsonb) LANGUAGE sql STABLE AS $$
    WITH latest AS (
        SELECT DISTINCT ON (v.posting_id, v.field) v.posting_id, v.field, v.value
        FROM posting_field_versions v
        WHERE v.valid_from <= valid AND v.recorded_at <= known
        ORDER BY v.posting_id, v.field, v.valid_from DESC, v.id DESC)
    SELECT l.posting_id, coalesce(jsonb_object_agg(l.field, l.value) FILTER (WHERE l.field <> 'present'), '{}')
    FROM latest l
    GROUP BY l.posting_id
    HAVING bool_or(l.field = 'present' AND l.value = 'true'::jsonb)
$$;
