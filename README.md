# hourly-changefeed

Polls the public job-board APIs of Greenhouse, Lever and Ashby every hour for about 200 companies, keeps every field of every posting versioned (when it was true, and when we learned it), and publishes one JSONL change file per hour that a second database loads idempotently.

**Status:** M2: adapters for the three APIs and a registry of 201 validated boards (`registry/boards.tsv`). The plan, milestone by milestone, is in the portfolio plan; this README gets its measured numbers as milestones land.

## Run

```sh
nix develop
./scripts/pg-init && ./scripts/pg-start
uv run pytest
uv run changefeed migrate
./scripts/pg-stop
```

The repo's `.pgdata` is for development and tests. The hourly collector keeps its data in its own cluster under `~/.local/share/hourly-changefeed/pgdata` (from M5a).

## Provenance

The migration runner and the test database fixtures are copied from my gator rebuild (`reimplemented/andrefetch-gator`); each copied file names the source commit in its header.
