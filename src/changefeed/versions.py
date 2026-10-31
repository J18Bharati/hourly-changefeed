"""The versioning engine: turn each accepted board body into per-field versions.

Every version has two times. valid_from is when the value became true: the source's own
timestamp when it gives one that moved, otherwise the time we first saw the value. recorded_at is
when we learned it: the transaction time of the store. valid_from never passes recorded_at, and
for one (posting, field) it only moves forward, so history can't run backwards.

Presence is a field like any other ('present'). A posting missing from a body is not closed at
once: it takes ABSENT_CONFIRM agreeing fetches (EMPTY_CONFIRM if the whole board came back
empty, the shape of an error page), and the tombstone is then valid from the first of them.
A 304 or a byte-identical body counts as agreeing.

The live collector and `rebuild-versions` both call apply_body/apply_repeat; only recorded_at
differs (now() vs the original fetch's), so a rebuild reproduces the incremental history.
"""
import hashlib
import json
from dataclasses import dataclass, field as dfield
from datetime import datetime

import psycopg
from psycopg.types.json import Jsonb

from changefeed import adapters
from changefeed.adapters import Board

PRESENT = "present"
ABSENT_CONFIRM = 2
EMPTY_CONFIRM = 3


def posting_hash(fields: dict) -> str:
    return hashlib.sha256(json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass
class _Writer:
    conn: psycopg.Connection
    recorded_at: datetime
    run_id: int | None
    log_id: int | None
    latest: dict = dfield(default_factory=dict)  # (posting_id, field) -> latest valid_from
    written: int = 0

    def version(self, posting_id, field, value, candidate: datetime | None, *, ours=False) -> None:
        """candidate: the source's time for the change, or (ours=True) an earlier observation of ours."""
        last = self.latest.get((posting_id, field))
        if ours and candidate is not None and (last is None or candidate > last):
            valid_from, why = candidate, "observed"
        elif candidate is None or (last is not None and candidate <= last):
            valid_from, why = self.recorded_at, "observed"
        elif candidate > self.recorded_at:
            valid_from, why = self.recorded_at, "clamped"
        else:
            valid_from, why = candidate, "source"
        if last is not None and valid_from <= last:  # two changes within one recorded instant
            raise ValueError(f"posting {posting_id} {field}: valid time would not move forward")
        self.conn.execute("""
            INSERT INTO posting_field_versions (posting_id, field, value, valid_from, valid_from_source,
                                                recorded_at, run_id, fetch_log_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (posting_id, field, Jsonb(value), valid_from, why, self.recorded_at, self.run_id, self.log_id))
        self.latest[(posting_id, field)] = valid_from
        self.written += 1


def _writer(conn, board_id, recorded_at, run_id, log_id) -> _Writer:
    w = _Writer(conn, recorded_at, run_id, log_id)
    w.latest = {(pid, f): vf for pid, f, vf in conn.execute("""
        SELECT v.posting_id, v.field, max(v.valid_from) FROM posting_field_versions v
        JOIN postings_current p ON p.id = v.posting_id WHERE p.board_id = %s GROUP BY 1, 2""", (board_id,))}
    return w


def _count_absences(w: _Writer, posting_ids: list, need: int) -> None:
    """One more agreeing fetch without these open postings; close those that reach `need`."""
    rows = w.conn.execute("""
        UPDATE postings_current SET absent_count = absent_count + 1, absent_since = coalesce(absent_since, %s)
        WHERE id = ANY(%s) AND present
        RETURNING id, absent_count, absent_since""", (w.recorded_at, posting_ids)).fetchall()
    for posting_id, count, since in sorted(rows):
        if count >= need:
            w.version(posting_id, PRESENT, False, since, ours=True)
            w.conn.execute("UPDATE postings_current SET present = false, absent_count = 0 WHERE id = %s",
                           (posting_id,))


def apply_body(conn, board_id, board: Board, *, recorded_at, run_id=None, log_id=None) -> int:
    """Apply an accepted body. Call inside the store transaction. Returns versions written."""
    w = _writer(conn, board_id, recorded_at, run_id, log_id)
    current = {eid: (pid, h, fields, present) for eid, pid, h, fields, present in conn.execute(
        "SELECT external_id, id, content_hash, fields, present FROM postings_current WHERE board_id = %s", (board_id,))}
    seen = set()
    for p in board.postings:
        seen.add(p.external_id)
        h = posting_hash(p.fields)
        if p.external_id not in current:
            pid = conn.execute("""
                INSERT INTO postings_current (board_id, external_id, fields, source_ts, content_hash,
                                              first_seen_at, last_seen_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (board_id, p.external_id, Jsonb(p.fields), p.source_ts, h, recorded_at, recorded_at)).fetchone()[0]
            w.version(pid, PRESENT, True, p.source_ts)
            for name, value in p.fields.items():
                if value is not None:  # no version means null; a later change to null is versioned
                    w.version(pid, name, value, p.source_ts)
            continue
        pid, old_hash, old_fields, present = current[p.external_id]
        if not present:
            w.version(pid, PRESENT, True, None)  # back after a tombstone: from when we saw it
        if h != old_hash:
            for name in sorted(old_fields.keys() | p.fields.keys()):
                if old_fields.get(name) != p.fields.get(name):
                    w.version(pid, name, p.fields.get(name), p.source_ts)
        conn.execute("""
            UPDATE postings_current SET fields = %s, content_hash = %s, source_ts = %s, last_seen_at = %s,
                                        present = true, absent_count = 0, absent_since = NULL
            WHERE id = %s""", (Jsonb(p.fields), h, p.source_ts, recorded_at, pid))
    missing = [pid for eid, (pid, _, _, present) in current.items() if present and eid not in seen]
    _count_absences(w, missing, EMPTY_CONFIRM if not board.postings else ABSENT_CONFIRM)
    return w.written


def apply_repeat(conn, board_id, *, recorded_at, run_id=None, log_id=None) -> int:
    """A 304 or a byte-identical body: the same postings as last time, seen once more."""
    w = _writer(conn, board_id, recorded_at, run_id, log_id)
    conn.execute("""UPDATE postings_current SET last_seen_at = %s
                    WHERE board_id = %s AND present AND absent_count = 0""", (recorded_at, board_id))
    rows = conn.execute("SELECT id, absent_count FROM postings_current WHERE board_id = %s AND present",
                        (board_id,)).fetchall()
    missing = [pid for pid, count in rows if count > 0]
    _count_absences(w, missing, EMPTY_CONFIRM if len(missing) == len(rows) else ABSENT_CONFIRM)
    return w.written


def _latest_body(conn, board_id, before_log_id) -> bytes | None:
    row = conn.execute("""
        SELECT r.body FROM fetch_log l JOIN raw_bodies r ON r.hash = l.body_hash
        WHERE l.board_id = %s AND l.outcome = 'ok' AND l.id < %s ORDER BY l.id DESC LIMIT 1""",
        (board_id, before_log_id)).fetchone()
    return bytes(row[0]) if row else None


def rebaseline(conn, board_id, before_log_id) -> None:
    """After a normaliser change: re-normalise the board's latest accepted body and make that the
    current state, writing no versions. The next fetch is then compared like for like, so only
    real changes become versions. It must use the body *before* the fetch being stored: using
    that fetch's own body would absorb whatever really changed in it. (Normalisers may only get coarser; the old versions keep their
    old canonical form.)"""
    source, = conn.execute("SELECT source FROM boards WHERE id = %s", (board_id,)).fetchone()
    body = _latest_body(conn, board_id, before_log_id)
    if body is None:
        return
    for p in adapters.parse_board(source, body).postings:
        conn.execute("UPDATE postings_current SET fields = %s, content_hash = %s WHERE board_id = %s AND external_id = %s",
                     (Jsonb(p.fields), posting_hash(p.fields), board_id, p.external_id))


def rebuild(conn, board_id) -> int:
    """Throw away a board's postings and versions and replay every accepted fetch from its raw
    body, at its original recorded time. Call inside a transaction. Returns versions written."""
    source, = conn.execute("SELECT source FROM boards WHERE id = %s", (board_id,)).fetchone()
    conn.execute("DELETE FROM postings_current WHERE board_id = %s", (board_id,))
    written = 0
    for log_id, outcome, recorded_at, run_id, body in conn.execute("""
            SELECT l.id, l.outcome, l.recorded_at, l.run_id, r.body FROM fetch_log l
            LEFT JOIN raw_bodies r ON r.hash = l.body_hash
            WHERE l.board_id = %s AND l.outcome IN ('ok', 'unchanged', 'not_modified') ORDER BY l.id""",
            (board_id,)).fetchall():
        if outcome == "ok":
            if body is None:
                raise RuntimeError(f"fetch {log_id}: raw body pruned, cannot rebuild board {board_id}")
            n = apply_body(conn, board_id, adapters.parse_board(source, bytes(body)),
                           recorded_at=recorded_at, run_id=run_id, log_id=log_id)
        else:
            n = apply_repeat(conn, board_id, recorded_at=recorded_at, run_id=run_id, log_id=log_id)
        conn.execute("UPDATE fetch_log SET versions = %s WHERE id = %s", (n, log_id))
        written += n
    conn.execute("UPDATE boards SET normalizer_version = %s WHERE id = %s", (adapters.NORMALIZER_VERSION, board_id))
    return written
