# Recorded fixtures

Bodies recorded from the public board APIs on 2026-10-31 (20:50 UTC), gzipped here unchanged.

| File | Request | Status | Validators sent |
|---|---|---|---|
| `greenhouse-discord.json.gz` | `boards-api.greenhouse.io/v1/boards/discord/jobs?content=true` | 200, 41 jobs, `meta.total` 41 | weak `ETag`, no `Last-Modified` (CloudFront) |
| `greenhouse-404.json` | same, token `nosuchboardxyz123` | 404 | none |
| `lever-outreach-limit3.json.gz` | `api.lever.co/v0/postings/outreach?mode=json&limit=3` | 200, 3 postings | weak `ETag`, no `Last-Modified` |
| `lever-mistral-empty.json.gz` | `api.lever.co/v0/postings/mistral?mode=json` | 200, `[]` | weak `ETag` |
| `lever-404.json` | token `nosuchboardxyz123` | 404 | weak `ETag` |
| `ashby-linear.json.gz` | `api.ashbyhq.com/posting-api/job-board/linear` | 200, 31 jobs | weak `ETag`, `max-age=60, stale-while-revalidate=60`, `cf-cache-status: HIT` |
| `ashby-404.txt` | token `nosuchboardxyz123` | 404, `text/plain` "Not Found" | none |

The Lever fixture uses `&limit=3` to keep it small (the full board is 500 KB); the shape is the same. The collector itself never passes `limit`.
