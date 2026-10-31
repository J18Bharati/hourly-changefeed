-- One row per public job board we collect. A board is a (source, token) pair: the token is the
-- company's slug on that ATS (boards-api.greenhouse.io/v1/boards/<token>, api.lever.co/v0/postings/<token>,
-- api.ashbyhq.com/posting-api/job-board/<token>).
CREATE TABLE boards (
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source   text NOT NULL CHECK (source IN ('greenhouse', 'lever', 'ashby')),
    token    text NOT NULL CHECK (token <> ''),
    company  text,
    added_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, token)
);
