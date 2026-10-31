# hourly-changefeed

Polls the public job-board APIs of Greenhouse, Lever and Ashby every hour for about 200 companies, keeps every field of every posting versioned (when it was true, and when we learned it), and publishes one JSONL change file per hour that a second database loads idempotently.

**Status:** M3: adapters, a registry of 201 validated boards (`registry/boards.tsv`), and `changefeed run`, which fetches every due board with per-API workers, logs each attempt, keeps raw bodies (lz4, 5.8×) and refuses stale copies. The collector has run hourly as a systemd user timer since 2026-11-01 (M5a). The plan, milestone by milestone, is in the portfolio plan; this README gets its measured numbers as milestones land.

## Run

```sh
nix develop
./scripts/pg-init && ./scripts/pg-start
uv run pytest
uv run changefeed migrate
./scripts/pg-stop
```

The repo's `.pgdata` is for development and tests. The hourly collector keeps its data in its own cluster:

```sh
./scripts/install-collector            # build, create ~/.local/share/hourly-changefeed/pgdata, enable the timer
systemctl --user list-timers changefeed.timer
journalctl --user -u changefeed.service -n 20
```

Re-run `install-collector` after a commit to deploy it; it is idempotent.

## Provenance

The migration runner and the test database fixtures are copied from my gator rebuild (`reimplemented/andrefetch-gator`); each copied file names the source commit in its header.
