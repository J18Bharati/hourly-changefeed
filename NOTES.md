# Notes: dated findings

## 2026-10-31: what the three board APIs send (M1 fixtures)

Recorded with curl against one board each; the headers are in `tests/fixtures/README.md`.

- **None of them sends `Last-Modified`.** All three send a weak `ETag`. So the plan's first stale test (a `Last-Modified` that goes backwards, which caught the danluu.com replicas in gator) has nothing to compare here. The stale check has to come from the bodies: the newest per-posting source time (Greenhouse `updated_at`, Lever `createdAt`, Ashby `publishedAt`).
- **Ashby is served from Cloudflare's cache** (`cf-cache-status: HIT`, `cache-control: public, max-age=60, stale-while-revalidate=60`). A fetch can get a copy up to about 2 minutes old by design. With hourly polling that's harmless on its own, but it's the setup in which gator saw history run backwards, so M3 logs `CF-Cache-Status` and `Age` per fetch.
- **Only Greenhouse gives a count** (`meta.total`), and it matched the list (41 = 41). The adapter treats a mismatch as a parse error.
- **Lever has no update time** at all, only `createdAt`. A Lever posting edited in place has no source timestamp for when, so its valid time will be the time we observed the change (`valid_from_source = 'observed'`).
- **Error shapes:** Greenhouse 404 is JSON `{"status":404,"error":"Job not found"}`; Lever 404 is `{"ok":false,"error":"Document not found"}`; Ashby 404 is `text/plain` `Not Found`. Served with a 200, each would fail the adapter's shape check rather than read as an empty board. Two Lever tokens (`mistral`, `whoop`) answered 200 `[]`: real but empty boards, or abandoned ones. M2's `seed validate` drops boards with no postings.
