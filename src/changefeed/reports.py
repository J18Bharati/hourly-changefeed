"""Consumer reports: how complete each board's fields are, how fresh its changes are, and which
hours have no data. All read the consumer database only.

Freshness is recorded_at - valid_from, measured only where valid_from came from the source
('source') and the row is not a baseline: a first sighting's source time is the posting's creation,
which says how old the posting is, not how late we were. 'observed' rows have no lag by
construction. Only Greenhouse moves its source stamp on edits (updated_at), so in practice
freshness is a Greenhouse number; Lever and Ashby boards report no measurements.
"""
from datetime import datetime, timezone

EMPTY = ("null", '""', "[]", "{}")
OBSERVED = ["ok", "not_modified", "unchanged"]

# Boards with no fresh fetch (ok, not modified, unchanged; a stale reply is not fresh) in the 24 h
# before `at`. Their postings would read as open forever, so fill rates leave them out and the report
# names them. Applies once the feed has reported board outcomes at all (manifest format 2), and an
# empty window then counts: a day with no fetches is the strongest signal, not a reason to stop.
_UNSEEN = """
    SELECT DISTINCT p.source, p.board FROM postings p
    WHERE EXISTS (SELECT 1 FROM board_observations WHERE hour <= coalesce(%(at)s, now()))
      AND NOT EXISTS (SELECT 1 FROM board_observations o
                      WHERE (o.source, o.board) = (p.source, p.board) AND o.outcome = ANY(%(observed)s)
                        AND o.hour > coalesce(%(at)s, now()) - interval '24 hours' AND o.hour <= coalesce(%(at)s, now()))"""


def unseen_boards(conn, at: datetime | None = None) -> list[str]:
    return [f"{s}/{b}" for s, b in conn.execute(_UNSEEN + " ORDER BY 1, 2", {"at": at, "observed": OBSERVED})]


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
        WHERE (p.source, p.board) NOT IN (""" + _UNSEEN + """)
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""", {"at": at, "empty": list(EMPTY), "observed": OBSERVED}).fetchall()


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


def _printable(text: str) -> str:
    """Text that came from a change file, made safe for a terminal."""
    return "".join(c if c.isprintable() else repr(c)[1:-1] for c in text)


def render(conn) -> str:
    newest = conn.execute("SELECT max(hour) FROM loaded_files").fetchone()[0]
    lines = [f"newest hour loaded: {newest.astimezone(timezone.utc):%Y-%m-%d %H:%M}Z" if newest else "newest hour loaded: none",
             "fill rate (open postings now):"]
    lines += [f"  {s}/{b} {f} {filled}/{n} ({100 * filled / n:.0f}%)" for s, b, f, filled, n in fill_rates(conn)]
    lines.append("freshness (source edit to recorded):")
    lines += [f"  {s}/{b} n={n} p50 {_duration(p50)} p95 {_duration(p95)} last change {last.astimezone(timezone.utc):%Y-%m-%d %H:%M}Z"
              for s, b, n, p50, p95, last in freshness(conn)]
    unseen = unseen_boards(conn)
    lines.append(f"no fresh fetch in 24 h: {', '.join(unseen) or 'none'}")
    total, missed = missed_hours(conn)
    lines.append(f"missed {len(missed)} of {total} hours" +
                 "".join(f"\n  {h.astimezone(timezone.utc):%Y-%m-%d %H}:00Z {_printable(why)}" for h, why in missed))
    return "\n".join(lines)
