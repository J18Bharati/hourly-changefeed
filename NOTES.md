# Notes: dated findings

## 2026-10-31: what the three board APIs send (M1 fixtures)

Recorded with curl against one board each; the headers are in `tests/fixtures/README.md`.

- **None of them sends `Last-Modified`.** All three send a weak `ETag`. So the plan's first stale test (a `Last-Modified` that goes backwards, which caught the danluu.com replicas in gator) has nothing to compare here. The stale check has to come from the bodies: the newest per-posting source time (Greenhouse `updated_at`, Lever `createdAt`, Ashby `publishedAt`).
- **Ashby is served from Cloudflare's cache** (`cf-cache-status: HIT`, `cache-control: public, max-age=60, stale-while-revalidate=60`). A fetch can get a copy up to about 2 minutes old by design. With hourly polling that's harmless on its own, but it's the setup in which gator saw history run backwards, so M3 logs `CF-Cache-Status` and `Age` per fetch.
- **Only Greenhouse gives a count** (`meta.total`), and it matched the list (41 = 41). The adapter treats a mismatch as a parse error.
- **Lever has no update time** at all, only `createdAt`. A Lever posting edited in place has no source timestamp for when, so its valid time will be the time we observed the change (`valid_from_source = 'observed'`).
- **Error shapes:** Greenhouse 404 is JSON `{"status":404,"error":"Job not found"}`; Lever 404 is `{"ok":false,"error":"Document not found"}`; Ashby 404 is `text/plain` `Not Found`. Served with a 200, each would fail the adapter's shape check rather than read as an empty board. Two Lever tokens (`mistral`, `whoop`) answered 200 `[]`: real but empty boards, or abandoned ones. M2's `seed validate` drops boards with no postings.

## 2026-11-01: seeding the registry (M2)

380 candidates sampled from Riley-Dorrington/job-board-aggregator's token lists (two seeds, see `registry/NOTICE`), each fetched once, at most 2 at a time per API. 6 workers took 1 m 49 s for the first 230.

| Source | Sampled | Active | 404 | Empty board | Parse or fetch error |
|---|---|---|---|---|---|
| greenhouse | 170 | 98 | 60 | 12 | 0 |
| lever | 110 | 35 | 65 | 10 | 0 |
| ashby | 100 | 68 | 25 | 7 | 0 |

- **Number that looked wrong: Lever kept only 32%.** In the first batch 37 of 60 Lever tokens were 404, against 37 of 110 for Greenhouse and 19 of 60 for Ashby. The source list is an accumulation of every token its scraper has ever seen, and Lever tokens die (companies leaving Lever, or renaming the board) without the list forgetting them. So the registry over-represents Greenhouse and Ashby relative to the published list; any per-ATS comparison later must say so.
- **No adapter failures on 201 live boards**, beyond the 3 fixtures they were written against.
- **One full sweep is 58.4 MB uncompressed.** Medians are about 10 postings per board; the largest board is 3.8 MB (Ashby). That's the number to watch against disk in M3/M5b: it's the cost per hour if every board changed, before dedupe by body hash and lz4.

## 2026-11-01: first runs against the 201 boards (M3)

Four runs on the dev database, about 25 minutes apart in total.

| Run | What was sent | Result | Wall time |
|---|---|---|---|
| 1 | nothing (first fetch) | ok: 201 | 85 s |
| 2 | stored ETag | not_modified: 201 | 76 s |
| 3 | no ETag | unchanged: 201 (same bytes as run 1) | 54 s |
| 4 | stored ETag, after the worker change below | not_modified: 201 | 24 s |

- **Number that looked wrong: 201 bodiless 304s took 76 s, nearly as long as fetching everything.** Two causes:
  - **Lever does the work before it says 304.** Median 1.4 s for a 304 against 2.1 s for a full 200, worst 28 s. Its weak ETags (`W/"len-hash"`, the Express style) are computed from the rendered body, so a 304 saves bandwidth but not Lever's time, or ours.
  - **My worker pool made it worse.** 6 shared workers with a 2-per-API semaphore: a worker that claimed a Lever board blocked on the semaphore, so up to 4 workers sat waiting on Lever while Greenhouse and Ashby boards were due. Now each API gets its own 2 workers, which claim only that API's boards. The same all-304 run took 24 s.
- **The exit criterion passed trivially.** It asks for every board fetched twice, with any repeat whose normalised form differs treated as a normaliser bug. All 201 repeats were byte-identical, so nothing could differ. Re-parsing all 201 stored bodies twice gave identical results (4,653 postings). The real test is hourly data, which M4 is built against.
- **Stale rule, changed from the plan.** The plan said a copy is stale when the board's newest source timestamp goes backwards. But closing the newest posting does exactly that, and the board would read as stale until someone posted a newer job (`test_closing_the_newest_posting_is_not_stale` fails under the plan's rule). Instead, a copy is stale when a posting present in both copies has an older source timestamp. Lever and Ashby only give a creation or publish time, so this can only catch lagging copies on Greenhouse; for the other two, M4's rule that a posting is closed only after 2 agreeing fetches is the defence.
- **Raw bodies compress 5.8× with lz4:** 58.4 MB of bodies take 10 MB in `raw_bodies`. If every board changed every hour for 14 days that would be 3.4 GB; M5b reports the real rate.

## 2026-11-01: the collector is live (M5a)

- Installed with `scripts/install-collector`: Postgres 17 as `changefeed-postgres.service` (`Type=notify`; the nixpkgs build links libsystemd), the hourly `changefeed.timer` (`Persistent=true`), and linger on (`loginctl enable-linger` worked without sudo here). Data: `~/.local/share/hourly-changefeed/pgdata`.
- First run, started by hand through the service at 02:42 IST: 201 ok in 39 s.
- **`systemd-analyze verify` caught a no-op in the plan:** `RuntimeMaxSec=` has no effect on a `Type=oneshot` service, so the 50-minute guard against a hung run would never have fired. A oneshot's whole run is its start phase, so the limit is `TimeoutStartSec=50min`. `tests/test_units.py` now runs the verifier over `deploy/systemd/` and fails on any warning about our units.
- From here `fetch_log`, `raw_bodies` and `postings_current` grow every hour. M4's versioning engine is developed against that stored history: it can rebuild from raw bodies, so nothing collected before M4 is lost.

## 2026-11-01: versioning (M4), and an hour the timer lost

- **The 03:00 run fetched nothing.** The manual run at 02:42 scheduled each board for "an hour from now, minus 10 minutes of slack" = 03:32, so at 03:00 nothing was due. Any run finishing more than 10 minutes past the hour (a manual run, or a run on wake) made the next hourly run skip. A success is now due at the top of the next hour (`date_trunc('hour', now()) + poll_interval`), with a test named for the incident. `collection_runs` recorded that run honestly: `ok`, counts `{}`.
- **Backfill.** M4 was deployed on top of 201 boards' worth of raw bodies collected before versioning existed. `rebuild-versions` replayed them: 46,415 versions, 4,653 open postings, the same numbers on the dev database. All first sightings take their valid time from the source (`published_at`, `updated_at`, `createdAt`).
- **The test that paid for itself:** re-baselining after a normaliser change first read "the board's latest accepted body", which by then was the body being stored, so the re-baseline absorbed that fetch's real change. It now reads the latest body *before* this fetch.
- **Nulls aren't versioned on first sight** (a missing version means null), which saved about 40% of rows; a later change *to* null is still a version. Empty lists (`departments: []`) are values and are versioned.
- **Property tests**, over 50 seeded random histories: `as_of(t, t)` equals the state held right after the fetch at t, and valid time only moves forward and never passes recorded time. Making the engine version only `title` fails 25 of them.

## 2026-11-01: operations (M5b), and where the bytes actually go

- Live after deploy: `health` ok, `status` no alerts, the first backup (without raw bodies) is 11 MB, and the daily timer runs at 04:30.
- **Number that looked wrong: the database is 93 MB, but raw bodies, the thing the plan worried about, are 10 MB of it.** `postings_current` is 43 MB and `posting_field_versions` 28 MB, of which `description` is 16 MB. Descriptions are stored three times over: inside the raw body (lz4), in the current row, and in the first version. jsonb in `postings_current` is TOAST-compressed with pglz by default, not lz4.
- **So the growth to watch is edited descriptions,** each a full HTML copy in `posting_field_versions`, not raw bodies, which are deduplicated by hash and pruned after 14 days. `storage_reports.versions_24h` tracks it. If it gets large: set lz4 on the jsonb columns, or store descriptions once by hash.

## 2026-11-01: change files (M6), and hours that started at half past

- **Number that looked wrong: the first live change file was `2026/10/31/20.jsonl`, and its manifest said the hour began at `20:30:00+00:00`.** Collection had started at 02:42 IST, which is 21:12 UTC, so a 20:xx file was already odd, and no UTC hour starts at :30. `date_trunc('hour', now())` truncates in the session's time zone, and this machine is Asia/Kolkata (+05:30). Every "hour" (run hours, `next_fetch_at`, the emit watermark) began at :30 UTC.
- It would have been invisible on a UTC server and is only half visible in IST, where the files look plausible. `tests/test_utc_hours.py` sets the database's time zone to Asia/Kolkata and checks that run hours, `next_fetch_at` and manifest hours are all on UTC hours. It failed before the fix.
- Fix: every `date_trunc('hour', ...)` now passes `'UTC'`. Migration 0008 sets the same default on `collection_runs.hour`, re-derives existing run hours from `started_at`, and moves future `next_fetch_at` onto UTC hours. The one wrong file was deleted along with its `emitted_hours` row. Live, the three runs so far now share the 21:00 UTC hour (02:42, 03:00 and 03:01 IST), which the 04:00 IST timer run emits once that hour has ended.

## 2026-11-01: the consumer (M7–M9)

- **Loader (M7).** `changefeed load` reads change files only. Each file is checked before anything is written:
  - the manifest names its own data file and nothing else (no `../`);
  - size, sha256 and line count match;
  - `prev_manifest_sha256` is the last manifest loaded.

  The file then loads in one transaction (`COPY` into a temp table, merge, ledger row). Each check was removed in turn and a test failed every time (8 mutations).
- **What the chain buys:** a missing hour, a reordered hour, or an edited file whose manifest was "fixed up" all fail at the next manifest. An hour already loaded with a different manifest is refused rather than merged, because a producer rewriting history is something to look at, not to absorb.
- **The loader's role** (`changefeed_loader`) owns `changefeed_consumer` and has no CONNECT on the producer: checked live (`User does not have CONNECT privilege`). Honest limit: the cluster uses trust auth on a private socket, so this keeps the loader's *code* out of the producer. It is no barrier against a local user, who can connect as any role.
- **`as_of` on the consumer (M8)** compares `known` with the producer's `recorded_at`, carried in every row, never with the load time. Otherwise loading a week of files today would make today the moment everything was learned, the same look-ahead leak M4 exists to prevent. Property test: 15 random histories are emitted, loaded and probed at (t, t), (t, now) and random pairs. The answers match the producer's; dropping `recorded_at <= known` or flipping the valid-time order fails.
- **Reports (M9).** Fill rate counts `""`, `[]` and `{}` as missing as well as null, because an empty department list says no more than a missing one.
  - Freshness is measured only on non-baseline rows with a source valid time. A first sighting's source time is the posting's creation, which says how old the posting is, not how late we were.
  - So freshness is in practice a Greenhouse-only number; Lever and Ashby edits are 'observed' and have zero lag by construction.
  - **Deviation:** stale tokens stay in the producer's `status`. The change files carry versions, not fetch outcomes, so the consumer cannot see a 404.
