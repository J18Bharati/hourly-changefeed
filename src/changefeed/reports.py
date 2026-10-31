"""Consumer reports: how complete each board's fields are, how fresh its changes are, and which
hours have no data. All read the consumer database only.

Freshness is recorded_at - valid_from, measured only where valid_from came from the source
('source') and the row is not a baseline: a first sighting's source time is the posting's creation,
which says how old the posting is, not how late we were. 'observed' rows have no lag by
construction. Only Greenhouse moves its source stamp on edits (updated_at), so in practice
freshness is a Greenhouse number; Lever and Ashby boards report no measurements.
"""
from datetime import datetime

EMPTY = ("null", '""', "[]", "{}")


def fill_rates(conn, at: datetime | None = None) -> list[tuple]:
    """(source, board, field, filled, open) for postings open at `at` as known then (default now)."""
    return conn.execute("""
        WITH open AS (SELECT * FROM postings_as_of(coalesce(%(at)s, now()), coalesce(%(at)s, now()))),
             fields AS (SELECT DISTINCT p.source, p.board, v.field
                        FROM field_versions v JOIN postings p ON p.id = v.posting_id
                        WHERE v.field <> 'present')
        SELECT f.source, f.board, f.field,
               count(*) FILTER (WHERE o.fields -> f.field IS NOT NULL
                                  AND (o.fields -> f.field)::text <> ALL (%(empty)s)) AS filled,
               count(*) AS open
        FROM fields f
        JOIN postings p ON (p.source, p.board) = (f.source, f.board)
        JOIN open o ON o.posting_id = p.id
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""", {"at": at, "empty": list(EMPTY)}).fetchall()


def freshness(conn) -> list[tuple]:
    """(source, board, measurements, p50 seconds, p95 seconds, last change recorded) per board."""
    return conn.execute("""
        SELECT p.source, p.board,
               count(*) FILTER (WHERE v.valid_from_source = 'source' AND NOT v.baseline),
               percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM v.recorded_at - v.valid_from))
                   FILTER (WHERE v.valid_from_source = 'source' AND NOT v.baseline),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM v.recorded_at - v.valid_from))
                   FILTER (WHERE v.valid_from_source = 'source' AND NOT v.baseline),
               max(v.recorded_at)
        FROM field_versions v JOIN postings p ON p.id = v.posting_id
        GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()


def missed_hours(conn) -> tuple[int, list[tuple]]:
    """(hours loaded, [(hour, reason)] for the hours no run covered)."""
    total = conn.execute("SELECT count(*) FROM loaded_files").fetchone()[0]
    return total, conn.execute("SELECT hour, missed FROM loaded_files WHERE missed IS NOT NULL ORDER BY hour").fetchall()


def _duration(seconds) -> str:
    return "-" if seconds is None else f"{seconds / 60:.0f} min" if seconds < 7200 else f"{seconds / 3600:.1f} h"


def render(conn) -> str:
    lines = ["fill rate (open postings now):"]
    lines += [f"  {s}/{b} {f} {filled}/{n} ({100 * filled / n:.0f}%)" for s, b, f, filled, n in fill_rates(conn)]
    lines.append("freshness (source edit to recorded):")
    lines += [f"  {s}/{b} n={n} p50 {_duration(p50)} p95 {_duration(p95)} last change {last:%Y-%m-%d %H:%M}Z"
              for s, b, n, p50, p95, last in freshness(conn)]
    total, missed = missed_hours(conn)
    lines.append(f"missed {len(missed)} of {total} hours" +
                 "".join(f"\n  {h:%Y-%m-%d %H}:00Z {why}" for h, why in missed))
    return "\n".join(lines)
