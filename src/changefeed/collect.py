"""Fetch boards: one claimed board at a time, many at once, every attempt logged.

The per-board cycle is adapted from my Python rebuild of andrefetch/gator (https://github.com/andrefetch/gator, a Go
project): the fetch
runs with no transaction open, and everything after it commits in one transaction per board that
first checks the claim token, so one board failing never undoes another.

What changed from gator: JSON adapters instead of RSS; raw bodies are kept (lz4) so versions can
be rebuilt from them; and a copy is stale when a posting present in both it and our current set
carries an older source timestamp. (The plan said "the board's newest timestamp went backwards",
but closing the newest posting does that too, and would freeze the board. See NOTES.md.)
"""
import hashlib
import shutil
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta

import psycopg
from psycopg import errors
from psycopg.types.json import Jsonb

from changefeed import adapters, sched, versions
from changefeed.adapters import ADAPTERS, Board, ShapeError, parse_board
from changefeed.fetch import FetchError, FetchRefused, fetch, parse_retry_after

LEASE = timedelta(minutes=5)
PER_HOST = 2                               # workers per API, so at most this many requests to it at once
MIN_FREE = 10 * 1024**3                    # refuse to run with less free disk than this
CLOCK_SKEW = timedelta(minutes=5)          # a stored source time further ahead of when we saw it is bogus
ROLLBACK_AFTER = (3, timedelta(hours=24))  # stale replies, held this long, mean the source really went back
HEADERS = ("date", "age", "last-modified", "etag", "cache-control", "cf-cache-status", "via")
SUCCESS = {"ok", "not_modified", "unchanged", "stale"}
RUN_LOCK = 0x6366_7275  # "cfru": held for the whole of run_hour, so a manual run can't overlap the timer
PG_MAJOR = 17           # the flake pins postgresql_17; the collector's data directory must match


class WrongPostgres(RuntimeError):
    pass


# --- one board ------------------------------------------------------------------------------

def _keep_body(conn, board_id, body: bytes) -> str:
    h = hashlib.sha256(body).hexdigest()
    conn.execute("""
        INSERT INTO raw_bodies (hash, body, bytes, first_board) VALUES (%s, %s, %s, %s)
        ON CONFLICT (hash) DO UPDATE SET last_seen_at = now()""", (h, body, len(body), board_id))
    return h


def _log(conn, claim, run_id, started, outcome, *, resp=None, body_hash=None, postings=None,
         error=None, retry_after=None, baseline=False) -> int:
    ms = int((time.monotonic() - started) * 1000)
    return conn.execute("""
        INSERT INTO fetch_log (board_id, run_id, started_at, duration_ms, outcome, baseline, status, headers,
                               body_hash, bytes, postings, error, retry_after)
        VALUES (%s, %s, now() - %s * interval '1 millisecond', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id""",
        (claim.board_id, run_id, ms, ms, outcome, baseline,
         resp.status if resp else None,
         Jsonb({k: resp.headers[k] for k in HEADERS if k in resp.headers}) if resp else None,
         body_hash, len(resp.body) if resp else None, postings, error, retry_after)).fetchone()[0]


def _complete(conn, claim, run_id, started, outcome, *, ok, resp=None, keep=False, store=None, **log) -> str:
    """Finish the claim, keep the body if asked, log the attempt and run store(), in one transaction."""
    try:
        with conn.transaction():
            sched.finish(conn, claim, ok=ok, retry_after=log.get("retry_after"), error=log.get("error"))
            body_hash = _keep_body(conn, claim.board_id, resp.body) if keep else None
            log_id = _log(conn, claim, run_id, started, outcome, resp=resp, body_hash=body_hash, **log)
            if store:
                written = store(log_id)
                if written is not None:
                    conn.execute("UPDATE fetch_log SET versions = %s WHERE id = %s", (written, log_id))
    except sched.LeaseLost as e:
        try:
            with conn.transaction():
                _log(conn, claim, run_id, started, "lease_lost", resp=resp, error=str(e))
        except errors.ForeignKeyViolation:
            pass  # the board was deleted while we fetched it
        return "lease_lost"
    return outcome


def _stale(conn, claim, board: Board) -> str | None:
    """Why this copy looks older than what we hold, or None. Absent postings never count."""
    stored = {eid: (ts, seen) for eid, ts, seen in conn.execute(
        "SELECT external_id, source_ts, last_seen_at FROM postings_current WHERE board_id = %s AND source_ts IS NOT NULL",
        (claim.board_id,))}
    for p in board.postings:
        if p.external_id in stored and p.source_ts:
            ts, seen = stored[p.external_id]
            if p.source_ts < ts <= seen + CLOCK_SKEW:
                return f"posting {p.external_id} went back from {ts.isoformat()} to {p.source_ts.isoformat()}"
    return None


def _rollback_has_held(conn, claim) -> bool:
    """True once every reply since the last success has been stale, enough of them, for long enough."""
    count, since = conn.execute("""
        SELECT count(*), min(started_at) FROM fetch_log
        WHERE board_id = %(b)s AND outcome = 'stale'
          AND id > coalesce((SELECT max(id) FROM fetch_log WHERE board_id = %(b)s
                             AND outcome IN ('ok', 'not_modified', 'unchanged')), 0)""",
        {"b": claim.board_id}).fetchone()
    n, held = ROLLBACK_AFTER
    return count >= n and conn.execute("SELECT %s <= now() - %s", (since, held)).fetchone()[0]


def store_postings(conn, board_id, board: Board, run_id=None, log_id=None) -> int:
    """Version an accepted body (inside the store transaction). Returns versions written."""
    nv = conn.execute("SELECT normalizer_version FROM boards WHERE id = %s", (board_id,)).fetchone()[0]
    if nv is not None and nv != adapters.NORMALIZER_VERSION:
        versions.rebaseline(conn, board_id, before_log_id=log_id)
    conn.execute("UPDATE boards SET normalizer_version = %s WHERE id = %s", (adapters.NORMALIZER_VERSION, board_id))
    return versions.apply_body(conn, board_id, board, recorded_at=_now(conn), run_id=run_id, log_id=log_id)


def _now(conn):
    return conn.execute("SELECT now()").fetchone()[0]


def process(conn: psycopg.Connection, claim: sched.Claim, run_id=None) -> str:
    """Fetch, store and complete one claimed board. Never raises for a problem with the board."""
    started = time.monotonic()
    try:
        return _process(conn, claim, run_id, started)
    except psycopg.OperationalError:
        raise  # the database itself is gone
    except Exception as e:
        try:
            return _complete(conn, claim, run_id, started, "crashed", ok=False, error=f"{type(e).__name__}: {e}")
        except psycopg.Error:
            return "crashed"


def _process(conn, claim, run_id, started) -> str:
    row = conn.execute("SELECT etag, last_body_hash FROM boards WHERE id = %s", (claim.board_id,)).fetchone()
    if row is None:
        return "lease_lost"
    etag, last_hash = row
    try:
        resp = fetch(ADAPTERS[claim.source].url(claim.slug), etag=etag)
    except (FetchError, FetchRefused) as e:
        outcome = "refused" if isinstance(e, FetchRefused) else "network_error"
        return _complete(conn, claim, run_id, started, outcome, ok=False, error=str(e))
    db_now = conn.execute("SELECT now()").fetchone()[0]
    done = lambda outcome, **kw: _complete(conn, claim, run_id, started, outcome, resp=resp, **kw)

    def seen(new_hash=None):
        conn.execute("""UPDATE boards SET etag = coalesce(%s, etag), last_body_hash = coalesce(%s, last_body_hash),
                                          last_fetched_at = now() WHERE id = %s""",
                     (resp.headers.get("etag"), new_hash, claim.board_id))

    def again(log_id):  # a 304 or the same bytes: the same postings, seen once more
        seen()
        return versions.apply_repeat(conn, claim.board_id, recorded_at=_now(conn), run_id=run_id, log_id=log_id)

    if resp.status == 304:
        return done("not_modified", ok=True, store=again)
    if not 200 <= resp.status < 300:
        return done("http_error", ok=False, error=f"HTTP {resp.status}",
                    retry_after=parse_retry_after(resp.headers.get("retry-after"), now=db_now))
    body_hash = hashlib.sha256(resp.body).hexdigest()
    if body_hash == last_hash:
        return done("unchanged", ok=True, keep=True, store=again)
    try:
        board = parse_board(claim.source, resp.body)
    except ShapeError as e:
        return done("parse_error", ok=False, keep=True, error=str(e))
    why = _stale(conn, claim, board)
    if why and not _rollback_has_held(conn, claim):
        return done("stale", ok=True, keep=True, postings=len(board.postings), error=why)
    first = conn.execute("SELECT NOT EXISTS (SELECT 1 FROM fetch_log WHERE board_id = %s AND outcome = 'ok')",
                         (claim.board_id,)).fetchone()[0]

    def store(log_id):
        seen(body_hash)
        return store_postings(conn, claim.board_id, board, run_id, log_id)
    return done("ok", ok=True, keep=True, store=store, postings=len(board.postings), baseline=first)


def poll_board(conn: psycopg.Connection, board_id) -> str:
    """Claim and process one specific board now, whether or not it is due."""
    claim = sched.claim_board(conn, board_id, LEASE)
    return process(conn, claim) if claim else "busy"


# --- a run ------------------------------------------------------------------------------------

@dataclass
class Run:
    id: int
    status: str
    counts: Counter


def free_bytes(conn) -> int:
    return shutil.disk_usage(conn.execute("SHOW data_directory").fetchone()[0]).free


def record_missed(conn) -> int:
    """Write a 'missed' row for every whole hour since the last run that had no run at all."""
    return conn.execute("""
        INSERT INTO collection_runs (hour, started_at, finished_at, status, reason)
        SELECT h, h, h, 'missed', 'no run'
        FROM generate_series((SELECT max(hour) FROM collection_runs) + interval '1 hour',
                             date_trunc('hour', now(), 'UTC') - interval '1 hour', interval '1 hour') AS h""").rowcount


def run_hour(conninfo: str) -> Run | None:
    """The timer's entry point: one run at a time, gaps recorded, then fetch what is due.
    Returns None, writing nothing, if another run holds the lock."""
    with psycopg.connect(conninfo, autocommit=True) as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(%s)", (RUN_LOCK,)).fetchone()[0]:
            return None
        major = int(conn.execute("SHOW server_version_num").fetchone()[0]) // 10000
        if major != PG_MAJOR:
            raise WrongPostgres(f"collector database runs Postgres {major}, but this build pins {PG_MAJOR}")
        record_missed(conn)
        return run(conninfo)  # the lock is released when this connection closes


def run(conninfo: str) -> Run:
    """Fetch every due board and record the run.

    Each API gets its own PER_HOST workers, which claim only that API's boards. (A shared pool
    with a per-API semaphore let workers holding slow Lever claims block on the semaphore while
    Greenhouse and Ashby boards waited: NOTES.md, 2026-11-01.)
    """
    with psycopg.connect(conninfo, autocommit=True) as conn:
        free = free_bytes(conn)
        if free < MIN_FREE:
            reason = f"disk: {free / 1024**3:.1f} GiB free, need {MIN_FREE // 1024**3}"
            run_id = conn.execute("""INSERT INTO collection_runs (status, reason, finished_at)
                                     VALUES ('missed', %s, now()) RETURNING id""", (reason,)).fetchone()[0]
            return Run(run_id, "missed", Counter())
        run_id = conn.execute("INSERT INTO collection_runs DEFAULT VALUES RETURNING id").fetchone()[0]

    def worker(source) -> Counter:
        counts = Counter()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            while claims := sched.claim_due(conn, 1, LEASE, source):
                for claim in claims:
                    counts[process(conn, claim, run_id)] += 1
        return counts

    sources = [s for s in ADAPTERS for _ in range(PER_HOST)]
    with ThreadPoolExecutor(max_workers=len(sources)) as pool:
        counts = sum(pool.map(worker, sources), Counter())
    status = "ok" if set(counts) <= SUCCESS else "partial"
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("UPDATE collection_runs SET finished_at = now(), status = %s, counts = %s WHERE id = %s",
                     (status, Jsonb(dict(counts)), run_id))
    return Run(run_id, status, counts)
