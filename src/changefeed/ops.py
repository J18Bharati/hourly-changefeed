"""Running the collector: is it healthy, what needs a look, and keeping the disk in check."""
import subprocess
from datetime import timedelta
from pathlib import Path

import psycopg

HEALTHY_WITHIN = timedelta(hours=2)
KEEP_RAW = timedelta(days=14)
KEEP_BACKUPS = 3
SUCCESS = ("ok", "not_modified", "unchanged", "stale")


def health(conn) -> tuple[bool, str]:
    """Healthy if some board was fetched successfully within the last two hours."""
    last = conn.execute("SELECT max(recorded_at) FROM fetch_log WHERE outcome = ANY(%s)", (list(SUCCESS),)).fetchone()[0]
    if last is None:
        return False, "no successful fetch yet"
    age = conn.execute("SELECT now() - %s", (last,)).fetchone()[0]
    hours = age.total_seconds() / 3600
    return age <= HEALTHY_WITHIN, f"last success {hours:.1f} h ago ({last.isoformat(timespec='seconds')})"


def status(conn) -> str:
    """A plain-text summary: runs, outcomes, churn, and the boards and postings that need a look."""
    lines = []
    runs = conn.execute("""
        SELECT count(*) FILTER (WHERE status <> 'missed'), count(*) FILTER (WHERE status = 'missed')
        FROM collection_runs WHERE hour > now() - interval '7 days'""").fetchone()
    lines.append(f"runs (7 d): {runs[0]}; missed hours (7 d): {runs[1]}")
    outcomes = conn.execute("""SELECT outcome, count(*) FROM fetch_log WHERE recorded_at > now() - interval '24 hours'
                               GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    lines.append("fetches (24 h): " + (", ".join(f"{o} {n}" for o, n in outcomes) or "none"))
    churn = conn.execute("""SELECT count(*), count(DISTINCT posting_id) FROM posting_field_versions
                            WHERE recorded_at > now() - interval '24 hours'""").fetchone()
    lines.append(f"versions (24 h): {churn[0]} across {churn[1]} postings")
    open_ = conn.execute("SELECT count(*) FILTER (WHERE present), count(*) FROM postings_current").fetchone()
    lines.append(f"postings: {open_[0]} open of {open_[1]} seen")

    alerts = []
    for name, n in conn.execute("""
            SELECT b.source || '/' || b.token, count(*) FROM fetch_log l JOIN boards b ON b.id = l.board_id
            WHERE l.outcome = 'stale' AND l.recorded_at > now() - interval '24 hours'
            GROUP BY 1 HAVING count(*) >= 3 ORDER BY 1"""):
        alerts.append(f"{name}: {n} stale replies in 24 h")
    for name, n in conn.execute("""
            SELECT name, streak FROM (
                SELECT b.source || '/' || b.token AS name,
                       count(*) FILTER (WHERE l.error = 'HTTP 404'
                                        AND l.id > coalesce((SELECT max(id) FROM fetch_log x WHERE x.board_id = b.id
                                                             AND x.outcome = ANY(%s)), 0)) AS streak
                FROM boards b JOIN fetch_log l ON l.board_id = b.id
                WHERE b.status = 'active' GROUP BY b.id) s
            WHERE streak >= 3 ORDER BY 1""", (list(SUCCESS),)):
        alerts.append(f"{name}: {n} consecutive 404s (token gone?)")
    for name, eid, n in conn.execute("""
            SELECT b.source || '/' || b.token, p.external_id, count(*)
            FROM posting_field_versions v JOIN postings_current p ON p.id = v.posting_id JOIN boards b ON b.id = p.board_id
            WHERE v.field = 'present' AND v.value = 'false'::jsonb AND v.recorded_at > now() - interval '7 days'
            GROUP BY 1, 2 HAVING count(*) >= 2 ORDER BY 1, 2"""):
        alerts.append(f"{name} posting {eid}: closed {n} times in 7 d (flapping)")
    lines.append("alerts:" if alerts else "alerts: none")
    lines.extend(f"  {a}" for a in alerts)
    return "\n".join(lines)


def prune(conn, keep: timedelta = KEEP_RAW) -> tuple[int, int]:
    """Delete raw bodies not seen for `keep`, except each board's latest accepted body (a re-baseline
    after a normaliser change needs it). Returns (rows, bytes) deleted."""
    rows = conn.execute("""
        DELETE FROM raw_bodies r
        WHERE r.last_seen_at < now() - %s
          AND r.hash NOT IN (SELECT DISTINCT ON (board_id) body_hash FROM fetch_log
                             WHERE outcome = 'ok' AND body_hash IS NOT NULL ORDER BY board_id, id DESC)
        RETURNING r.bytes""", (keep,)).fetchall()
    return len(rows), sum(b for (b,) in rows)


def report(conn, pruned: tuple[int, int] = (0, 0)) -> dict:
    """Record and return today's sizes."""
    row = conn.execute("""
        INSERT INTO storage_reports (db_bytes, raw_bytes, raw_rows, raw_bytes_24h, versions_24h, pruned_rows, pruned_bytes)
        SELECT pg_database_size(current_database()), pg_total_relation_size('raw_bodies'),
               (SELECT count(*) FROM raw_bodies),
               (SELECT coalesce(sum(bytes), 0) FROM raw_bodies WHERE first_seen_at > now() - interval '24 hours'),
               (SELECT count(*) FROM posting_field_versions WHERE recorded_at > now() - interval '24 hours'),
               %s, %s
        RETURNING db_bytes, raw_bytes, raw_rows, raw_bytes_24h, versions_24h, pruned_rows, pruned_bytes""",
        pruned).fetchone()
    keys = ["db_bytes", "raw_bytes", "raw_rows", "raw_bytes_24h", "versions_24h", "pruned_rows", "pruned_bytes"]
    return dict(zip(keys, row))


def backup(conninfo: str, directory: Path, *, stamp: str, keep: int = KEEP_BACKUPS) -> Path:
    """pg_dump -Fc without raw_bodies' rows (the table's definition stays), keeping the newest `keep`."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    final = directory / f"changefeed-{stamp}.dump"
    partial = final.with_suffix(".partial")
    subprocess.run(["pg_dump", "-Fc", "--exclude-table-data=raw_bodies", "-f", str(partial), "-d", conninfo],
                   check=True)
    partial.rename(final)
    for old in sorted(directory.glob("changefeed-*.dump"))[:-keep]:
        old.unlink()
    return final
