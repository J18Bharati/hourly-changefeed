-- Hours are UTC hours. date_trunc('hour', ...) truncates in the session's time zone, and in
-- Asia/Kolkata (+05:30) that put every "hour" at :30 UTC (NOTES.md, 2026-11-01).
ALTER TABLE collection_runs ALTER COLUMN hour SET DEFAULT date_trunc('hour', now(), 'UTC');
UPDATE collection_runs SET hour = date_trunc('hour', CASE WHEN status = 'missed' THEN hour ELSE started_at END, 'UTC');
UPDATE boards SET next_fetch_at = date_trunc('hour', next_fetch_at, 'UTC') WHERE next_fetch_at > now();
