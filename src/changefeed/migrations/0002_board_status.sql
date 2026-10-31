-- A board starts as a candidate (sampled from a published token list) and becomes active only after
-- one fetch returned 200 with at least one posting. `validation` keeps what that fetch saw.
ALTER TABLE boards
    ADD COLUMN status       text NOT NULL DEFAULT 'candidate' CHECK (status IN ('candidate', 'active', 'rejected')),
    ADD COLUMN validated_at timestamptz,
    ADD COLUMN validation   jsonb;
