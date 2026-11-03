# hourly-changefeed

**An hourly change feed of about 4,650 job postings at 201 companies.** Every field of every posting is versioned with when it was true (valid time) and when we learned it (recorded time). The feed is published as one hash-chained JSONL file per UTC hour, which a second database loads idempotently and can query "as it was known then".

It polls the public job-board APIs of Greenhouse, Lever and Ashby. It has run hourly on a laptop as a systemd user timer since 2026-11-01. Python 3, psycopg 3 and raw SQL, Postgres 17, Nix. No containers. 212 tests, all against a real Postgres.

## Run

```sh
nix develop
./scripts/pg-init && ./scripts/pg-start
uv run pytest                          # 212 tests, each on its own database cloned from a migrated template
./scripts/pg-stop
```

The repo's `.pgdata` is for development and tests. The live collector keeps its data in its own cluster:

```sh
./scripts/install-collector            # build, create ~/.local/share/hourly-changefeed/pgdata, enable the timers (idempotent; re-run to deploy)
systemctl --user list-timers 'changefeed*'
changefeed health                      # exit 1 unless a board was fetched in the last 2 h and change files are current
changefeed status                      # runs, outcomes, churn, and anything that needs a look
changefeed report                      # consumer: fill rates, freshness, missed hours
```

Each hour the timer runs `run-hour` (fetch every due board), then `emit` (write the files for every finished hour), then `load` (the consumer applies them).

## How it works

| Stage | What it does | Where |
|---|---|---|
| Fetch | Per-API workers claim due boards with `FOR UPDATE SKIP LOCKED`. Conditional GET with stored ETags. Host allowlist, 64 MiB cap. Every attempt is a `fetch_log` row; bodies are kept lz4-compressed and deduplicated by hash. | `fetch.py`, `sched.py`, `collect.py` |
| Normalise | Three adapters map to one `Posting`. A response without the expected shape is a parse error, never an empty board. | `adapters.py` |
| Version | Append-only `posting_field_versions(field, value, valid_from, valid_from_source, recorded_at)`. Presence is a field too, so closing and reopening are ordinary versions. A posting is closed only after 2 agreeing fetches. `rebuild-versions` replays everything from raw bodies. | `versions.py` |
| Publish | One `changes/YYYY/MM/DD/HH.jsonl` per UTC hour, empty hours included, plus a manifest: rows, sha256, runs, missed hours, each board's fetch outcome, and the previous manifest's hash. Written atomically, only after the hour has closed. The same hour always gives the same bytes. | `emit.py` |
| Consume | A separate database and role. A file is loaded only if its size, hash, line count and chain all check out. Loading twice is a no-op. `as_of(valid, known)` compares against the producer's recorded time, not the load time. | `load.py`, `reports.py` |

`postings_as_of(valid, known)` answers both "what was open at noon, as we know it now" and "what did we believe at noon". The difference between the two is the look-ahead bias a backtest on "latest" data would carry.

## Measured (live, 2026-11-01)

| | |
|---|---|
| Boards | 201 (98 Greenhouse, 68 Ashby, 35 Lever), from 380 sampled candidates |
| Open postings | about 4,650 |
| One full sweep | 58.4 MB of JSON; stored as 10 MB of raw bodies (lz4, 5.8×) |
| An hourly run where nothing changed | 201 × 304, about 24 s |
| Field versions | 46,415 at the first sweep; **32 more** in the next 16 hours |
| Hours emitted | 16, of which **5 had no run**, all while the laptop was suspended (09:31–12:34 and 13:56–17:47 IST). They are declared as missed in the manifests, not skipped silently. |
| Database size | 90 MB, of which raw bodies are 10 MB (see below) |

**Fill rate** of open postings, counted over the boards that publish a field at all. A field no posting on a board has ever had never gets a version, so that board drops out of the denominator for it.

| Field | Fill rate |
|---|---|
| Lever `salary` | 127 / 282 (45%); 209 more Lever postings are on boards that never publish one |
| Ashby `locations` (secondary locations) | 302 / 1,347 (22%) |
| Ashby `workplace_type` | 1,156 / 1,329 (87%) |
| Greenhouse `offices` | 2,758 / 2,817 (98%) |
| title, url, description, location | 100% on all three |

**Freshness** (from a source's edit time to our recorded time) has data on only 3 boards so far: p50 of 9, 33 and 59 minutes. Lever gives no update time at all, and Ashby only a publish time, so most edits are timed by observation and have no measurable lag. In practice this is a Greenhouse metric.

**Look-ahead backtest** (`changefeed backtest`, consumer side). For each board and time t it computes a hiring signal three ways:
- **point-in-time:** what the feed knew at t;
- **hindsight:** what was true at t, as known now;
- **naive:** postings open now whose `published_at` is at or before t. This is what a backtest built on a latest snapshot sees.

It then splits the gap into survivorship (open at t, closed since), late deletions and late additions. Last, it correlates each signal with the change in open postings over the following horizon.

On 2026-11-02, with 23 h of history (6 h window, 6 h horizon, hourly points), it found 3,417 points:
- naive is 67 below hindsight, out of 79,131 open. 16 of those are survivorship and 51 are postings with no publish date.
- all 16 survivorship postings are on one board.
- both correlations are −0.00 over 2,211 pairs.

**That is a null result.** One day is too short for postings to close, and almost every posting entered in a baseline fetch. The method itself is tested on a seeded 42-day synthetic history.

## Numbers that looked wrong

Full write-ups, with the queries, are in [`NOTES.md`](NOTES.md).

- **The first change file was `20.jsonl`, for an hour that began at `20:30Z`.** No UTC hour starts at :30. `date_trunc('hour', …)` truncates in the session's time zone, and this machine is IST (+05:30). Every hour boundary was 30 minutes off. On a UTC server it would never have shown. Every truncation now passes `'UTC'`, and a test runs the database in Asia/Kolkata. Afterwards, the corrected hour's sha256 matched the misdated file's exactly: all its rows fell inside both windows.
- **201 bodiless 304s took 76 s, nearly as long as fetching everything.** Lever computes its weak ETag from the rendered body, so a 304 costs it as much as a 200 (median 1.4 s against 2.1 s). On top of that, my shared worker pool blocked on a per-API semaphore. Per-API workers brought it to 24 s.
- **The database was 93 MB, but raw bodies, the thing I'd planned around, were 10 MB of it.** Descriptions are stored three times (raw body, current row, first version), and jsonb is TOAST-compressed with pglz, not lz4. So the growth to watch is edited descriptions, which `storage_reports` tracks.
- **The 03:00 run fetched nothing.** A manual run at 02:42 had scheduled boards for "an hour from now, minus 10 minutes", which was 03:32. Successes are now due at the top of the next hour.
- **`UNHEALTHY: last success 3.5 h ago` after a laptop sleep.** The catch-up run started 7 s before the Wi-Fi connected, and all 201 fetches failed DNS. The unit now waits up to 60 s for DNS first, and still runs and records the failures if DNS never comes.
- **Only 32% of sampled Lever tokens were live**, against 58% for Greenhouse and 68% for Ashby. The source list accumulates every token it has ever seen, and dead Lever boards stay on it. So the registry over-represents the other two, and any per-ATS comparison has to say so.

## What it doesn't do (yet)

- **Detecting a lagging CDN copy works on Greenhouse only.** None of the three APIs sends `Last-Modified`, and only Greenhouse gives a per-posting update time. On Lever and Ashby, the 2-agreeing-fetches rule is the defence against a stale copy closing postings.
- **The hash chain is unkeyed.** It catches corruption, gaps and reordering, not a forger who can write the changes directory.
- **The cluster uses trust auth on a private socket.** The loader's role keeps its code away from the producer, but doesn't stop a hostile local user.
- **Change files aren't backed up.** The nightly `pg_dump` leaves them out.
- **The backtest has too little history to say much yet** (see below).

## Provenance

The migration runner, the test-database fixtures, and the fetch/claim/backoff skeleton are adapted from my Python rebuild of [andrefetch/gator](https://github.com/andrefetch/gator), a Go project. Each adapted file says so in its header. The board tokens were sampled from [Riley-Dorrington/job-board-aggregator](https://github.com/Riley-Dorrington/job-board-aggregator) (MIT; see `registry/NOTICE`).

## Licence

MIT; see `LICENSE`. The board tokens in `registry/` are under their own MIT notice in `registry/NOTICE`.
