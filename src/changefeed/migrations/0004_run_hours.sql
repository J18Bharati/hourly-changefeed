-- The hour each run belongs to. A laptop that slept through 03:00-06:00 gets 'missed' rows for
-- those hours from the next run, so gaps are recorded as gaps rather than inferred later.
ALTER TABLE collection_runs ADD COLUMN hour timestamptz NOT NULL DEFAULT date_trunc('hour', now());
CREATE INDEX collection_runs_hour ON collection_runs (hour);
