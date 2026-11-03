# Notes: dated findings

## 2026-10-31: what the three board APIs send (M1 fixtures)

Recorded with curl against one board each; the headers are in `tests/fixtures/README.md`.

- **None of them sends `Last-Modified`.** All three send a weak `ETag`. So the project plan's first stale test (a `Last-Modified` that goes backwards, which caught the danluu.com replicas in my gator rebuild) has nothing to compare here. The stale check has to come from the bodies: the newest per-posting source time (Greenhouse `updated_at`, Lever `createdAt`, Ashby `publishedAt`).
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
- **Stale rule, changed from the project plan.** The project plan said a copy is stale when the board's newest source timestamp goes backwards. But closing the newest posting does exactly that, and the board would read as stale until someone posted a newer job (`test_closing_the_newest_posting_is_not_stale` fails under the project plan's rule). Instead, a copy is stale when a posting present in both copies has an older source timestamp. Lever and Ashby only give a creation or publish time, so this can only catch lagging copies on Greenhouse; for the other two, M4's rule that a posting is closed only after 2 agreeing fetches is the defence.
- **Raw bodies compress 5.8× with lz4:** 58.4 MB of bodies take 10 MB in `raw_bodies`. If every board changed every hour for 14 days that would be 3.4 GB; M5b reports the real rate.

## 2026-11-01: the collector is live (M5a)

- Installed with `scripts/install-collector`: Postgres 17 as `changefeed-postgres.service` (`Type=notify`; the nixpkgs build links libsystemd), the hourly `changefeed.timer` (`Persistent=true`), and linger on (`loginctl enable-linger` worked without sudo here). Data: `~/.local/share/hourly-changefeed/pgdata`.
- First run, started by hand through the service at 02:42 IST: 201 ok in 39 s.
- **`systemd-analyze verify` caught a no-op in the project plan:** `RuntimeMaxSec=` has no effect on a `Type=oneshot` service, so the 50-minute guard against a hung run would never have fired. A oneshot's whole run is its start phase, so the limit is `TimeoutStartSec=50min`. `tests/test_units.py` now runs the verifier over `deploy/systemd/` and fails on any warning about our units.
- From here `fetch_log`, `raw_bodies` and `postings_current` grow every hour. M4's versioning engine is developed against that stored history: it can rebuild from raw bodies, so nothing collected before M4 is lost.

## 2026-11-01: versioning (M4), and an hour the timer lost

- **The 03:00 run fetched nothing.** The manual run at 02:42 scheduled each board for "an hour from now, minus 10 minutes of slack" = 03:32, so at 03:00 nothing was due. Any run finishing more than 10 minutes past the hour (a manual run, or a run on wake) made the next hourly run skip. A success is now due at the top of the next hour (`date_trunc('hour', now()) + poll_interval`), with a test named for the incident. `collection_runs` recorded that run honestly: `ok`, counts `{}`.
- **Backfill.** M4 was deployed on top of 201 boards' worth of raw bodies collected before versioning existed. `rebuild-versions` replayed them: 46,415 versions, 4,653 open postings, the same numbers on the dev database. All first sightings take their valid time from the source (`published_at`, `updated_at`, `createdAt`).
- **The test that paid for itself:** re-baselining after a normaliser change first read "the board's latest accepted body", which by then was the body being stored, so the re-baseline absorbed that fetch's real change. It now reads the latest body *before* this fetch.
- **Nulls aren't versioned on first sight** (a missing version means null), which saved about 40% of rows; a later change *to* null is still a version. Empty lists (`departments: []`) are values and are versioned.
- **Property tests**, over 50 seeded random histories: `as_of(t, t)` equals the state held right after the fetch at t, and valid time only moves forward and never passes recorded time. Making the engine version only `title` fails 25 of them.

## 2026-11-01: operations (M5b), and where the bytes actually go

- Live after deploy: `health` ok, `status` no alerts, the first backup (without raw bodies) is 11 MB, and the daily timer runs at 04:30.
- **Number that looked wrong: the database is 93 MB, but raw bodies, the thing the project plan worried about, are 10 MB of it.** `postings_current` is 43 MB and `posting_field_versions` 28 MB, of which `description` is 16 MB. Descriptions are stored three times over: inside the raw body (lz4), in the current row, and in the first version. jsonb in `postings_current` is TOAST-compressed with pglz by default, not lz4.
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

## 2026-11-01: /review of the whole repo, weighted to part B

- **Who reviewed:**
  - six specialists: testing, maintainability, security, performance, data migration, simplification;
  - a red team and a Claude adversarial pass;
  - after the fixes, one adversarial re-review of the fix diff, plus my own pass over the second round.

  Codex was not installed, so there was no outside model. 22 regression tests were added; each failed before its fix.
- **Numbers that looked wrong, found by the reviewers:**
  - **A DST change would drop an hour from the feed without breaking the chain.** psycopg returns timestamps in the session's zone, and `hour += 1h` on a zoned datetime is wall-clock arithmetic. In New York, 01:00 EDT + 1 h = 02:00 EST, which is 05:00Z → 07:00Z. It is latent here, since IST has no DST, but it would go live with any time zone change. Hours now step in UTC.
  - **`OnCalendar=hourly` is local time**, so in IST the timer fired at :30 UTC, half way through the hours the files are cut on. It is now pinned to `*:00:00 UTC`.
  - **The first fix for one test was itself flaky.** `test_an_open_run_holds_back_its_hour` started its run "55 minutes ago", which falls in the current hour for 5 minutes of every hour. The replacement, "50 minutes before the hour", is more than an hour old after :10, so emit closes it as crashed. It now starts 1 µs before the hour.
  - **A NUL in one posting would have disabled its whole board.** jsonb refuses `\u0000` and lone surrogates, so the store transaction would crash every hour until the auto-disable. The adapters now drop both. No stored value changes, because none could have contained them.
- **Format 2.** Manifests now carry each board's last fetch outcome per hour. Without that, a board that stopped being fetched (404s, backoff) looked exactly like one that stopped changing, and its postings read as open forever. The consumer keeps `board_observations`, and reports leave out and name boards with no fresh fetch in 24 h. The loader still reads format 1.
- **History already published is now guarded and checked.**
  - `rebuild-versions` is refused once hours are emitted.
  - `changefeed run` takes the run lock.
  - `status` recounts each emitted hour and alerts if its rows changed: a clock stepping back after wake, a writer outside the lock, a rebuild.
  - `health` fails if change files fall more than 2 h behind.
- **Migration 0008 had two flaws, with no live effect.**
  - It floored `next_fetch_at` even for boards in backoff, which pulls a Retry-After earlier.
  - It ignored `emitted_hours`.

  Checked live: 0 boards had ever failed, there were 0 missed rows, and the one emitted :30 row had been removed by hand. 0008 is applied and checksummed, so it stays as is. emit now refuses to continue a chain from an off-hour row.
- **Not fixed, and known:**
  - The hash chain is unkeyed. It detects corruption and reordering, not a forger who can write the changes directory.
  - With trust auth, the loader role prevents mistakes, not a hostile local user.
  - Change files are not in the nightly backup, and nothing re-emits them; a restore makes the consumer refuse the rewritten hours.
- **Live after deploy:** the timer fired on reload, then run 4 (201 not modified), `emitted 1 hours`, `loaded 1 hours`.
  - Hour 21:00 UTC is 46,415 rows over 201 boards.
  - Its data sha256 equals the misdated `20.jsonl` from before the UTC fix: every version was recorded between 21:12 and 21:30 UTC, inside both windows, so the same rows gave the same bytes under different code.

## 2026-11-01: security review (static), the network-facing paths

- **Result: no supported findings in what I assessed.** Static review, 10-minute budget.
- **The review tool's saved report says `partial` with every domain `not_assessed`.** That is my error, not the code's: I called `finish` before submitting coverage, and a finished run accepts no evidence. The real coverage is below.
- **What I read, and what holds:**
  - **fetch.py.** The host allowlist is checked on every redirect hop. Only http(s) is allowed. `trust_env=False`, so no proxy comes from the environment. One Content-Encoding layer at most, inflated against a hard 64 MiB cap. A 120 s deadline covers redirects too. Retry-After is clamped to 24 h.
  - **Board tokens.** `Adapter.url` takes a regex-checked token, refuses `..`, and percent-quotes it. That covers `seed import` and every fetch. `seed load` (from the repo's TSV) inserts without the check, but every fetch goes back through `url()`.
  - **load.py.** A manifest may only name its own `HH.jsonl`, and its hour must match its path, so no path traversal. Every value reaches SQL as a parameter or through COPY. `provision` builds identifiers with `sql.Identifier`.
  - **ops.backup** runs `pg_dump` from an argument list, with no shell.
  - **Local boundary.** initdb leaves `pgdata` at 0700, and the trust-auth socket lives inside it, so no other OS user can reach the cluster. `changes/` and `backups/` are 0775, but group `zero` has no other members.
- **Hardening, not a finding:** `status` prints posting external ids and board tokens without `reports._printable`. Ids come from the ATS (companies don't choose them), so only a hostile or compromised ATS could put terminal escapes there.
- **Not covered:** dependency advisories (no scanner profile ran), CI (none exists), and the four gzip test fixtures (withheld by the review tool).

## 2026-11-01: a laptop sleep, and a catch-up run that beat the Wi-Fi

- **What health showed:** `UNHEALTHY: last success 3.5 h ago` at 13:00 IST.
- **What happened:**
  - The laptop slept from 09:31 to 12:34. Hours 10:30 and 11:30 IST are recorded as `missed: no run`, which is how they should be recorded.
  - On wake, `Persistent=true` started run 13 at 12:34:48. NetworkManager reported `CONNECTED_GLOBAL` at 12:34:55.
  - So every one of the 201 fetches failed with `ConnectError: [Errno -3] Temporary failure in name resolution`, and that hour got no data.
- **It cleared:** the 13:31 run was fine, and health was ok again.
- **Fix:** the unit's `ExecStartPre` waits up to 60 s for `boards-api.greenhouse.io` to resolve. It exits 0 either way, so a real outage is still fetched and recorded as network errors. `tests/test_units.py` runs that command with a host that never resolves and with `localhost`.
- **Not caused by the disk move:** that move happened during the sleep window, with the timer stopped by the move script.

## 2026-11-01: the DNS wait, tested by a real wake

- The laptop suspended at 13:56 IST, 1 minute after a reboot, and woke at 17:47. Hours 09:00, 10:00 and 11:00 UTC are recorded as `missed: no run`. Together with the morning's sleep, that makes 5 of the first 16 hours missed (`changefeed report`).
- The catch-up run started at 17:47:01, at wake, as the morning's had. This time the `ExecStartPre` DNS wait held it. The result was run 18: 200 `not_modified` and 1 `http_error`, with no DNS failures. It then emitted and loaded the 4 finished hours.

## 2026-11-02: the look-ahead backtest (M10), a null result on 23 hours of history

- **Command:** `changefeed backtest --window 0.25 --horizon 0.25 --step 0.0416667`, run on the consumer database. That is a 6 h window, a 6 h horizon and hourly points, over the loaded history: the first record is at 2026-10-31 21:12Z and the last at 2026-11-01 20:00Z. The project plan's 7-day window does not fit in that history. The run took 10 s.
- **Result:** 3,417 (board, hour) points.
  - Summed open postings: point-in-time 79,132, hindsight 79,131, naive (latest records) 79,064.
  - **The naive gap is 67, and it reconciles:** 16 postings that closed since (survivorship) plus 51 open postings with no `published_at`, which a naive series cannot place in time.
  - **All 16 survivorship postings are on one board, `greenhouse/braineffectjobs`.** So the survivorship rate across the whole feed is 0.02%.
  - 1 late deletion and 0 late additions. Over hours, the feed learns almost nothing late.
- **Forward test:** 2,211 pairs. The correlation of each signal with the change in open postings over the next 6 h is −0.00 for both. Only 15 new postings opened inside any window, and the two signals agree at every point. **That is a null result.**
  - **Why it is null:** almost every posting was first seen in a board's baseline fetch, with its valid time in the past. In one day, very little opens or closes.
  - Survivorship bias grows with how long a posting can close before "now". With 23 hours of history, and points that cover only the 17 h after the first window, that time is short.
- **What would change it:** weeks of history. `test_survivorship_matches_the_synthetic_truth` and the summary test check the method on a seeded 42-day history, in which the bias is clearly visible: naive < hindsight, and survivorship > 0.
- **Not corrected for:** a board that stops being fetched keeps its postings open, in this query as in every `as_of` query.
