"""Leased claims and backoff.

Copied from andrefetch-gator @ ebdaff9 (src/gator/poller/sched.py), with feeds renamed to boards.
A success makes the board due at the top of the next interval (date_trunc to the hour, plus
poll_interval), so the next hourly run fetches it however late in the hour this run was.

A claim is a short transaction: lock due, unleased boards with SKIP LOCKED, stamp a lease and a
fresh claim token, commit. The fetch then runs with no transaction open. Completion happens in
the transaction that stores the result, and only if the board still carries our token; a worker
whose lease ran out and was re-claimed gets LeaseLost and its result is rolled back.

All times come from the database clock, so workers on different machines agree.
"""
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

import psycopg

MIN_BACKOFF = timedelta(minutes=1)
MAX_BACKOFF = timedelta(hours=24)
MAX_FAILURES = 10


class LeaseLost(Exception):
    pass


@dataclass(frozen=True)
class Claim:
    board_id: int
    source: str
    slug: str    # the board's token on its ATS
    token: int   # the claim token


def jitter() -> float:
    return random.uniform(0.5, 1.0)


def backoff(failures: int) -> timedelta:
    """Wait after the n-th consecutive failure: doubling from MIN_BACKOFF, jittered down, capped."""
    raw = MIN_BACKOFF * 2 ** min(failures - 1, 30)
    return min(MAX_BACKOFF, raw * jitter())


_CLAIM = """
    UPDATE boards SET claimed_until = now() + %(lease)s, claim_token = nextval('board_claim_token')
    WHERE id IN (
        SELECT id FROM boards
        WHERE status = 'active' AND disabled_at IS NULL
          AND (claimed_until IS NULL OR claimed_until < now())
          AND {where}
          AND (%(source)s::text IS NULL OR source = %(source)s)
        ORDER BY next_fetch_at, id
        LIMIT %(n)s
        FOR UPDATE SKIP LOCKED)
    RETURNING id, source, token, claim_token"""


def claim_due(conn: psycopg.Connection, n: int, lease: timedelta, source: str | None = None) -> list[Claim]:
    """Claim up to n unleased boards that are due (from one source, if given). To fetch everything
    now, make_all_due first."""
    sql = _CLAIM.format(where="next_fetch_at <= now()")
    with conn.transaction():
        return [Claim(*row) for row in conn.execute(sql, {"lease": lease, "n": n, "source": source})]


def claim_board(conn: psycopg.Connection, board_id, lease: timedelta) -> Claim | None:
    """Claim one specific board, due or not, unless another worker holds it."""
    with conn.transaction():
        row = conn.execute(_CLAIM.format(where="id = %(id)s"), {"lease": lease, "n": 1, "id": board_id, "source": None}).fetchone()
    return Claim(*row) if row else None


def make_all_due(conn: psycopg.Connection) -> None:
    conn.execute("UPDATE boards SET next_fetch_at = now() WHERE disabled_at IS NULL")


def finish(conn: psycopg.Connection, claim: Claim, *, ok: bool,
           retry_after: datetime | None = None, error: str | None = None) -> None:
    """Release the claim and schedule the next fetch. Call inside the result's transaction.

    Raises LeaseLost if the board no longer carries this claim's token.
    """
    row = conn.execute("SELECT consecutive_failures FROM boards WHERE id = %s AND claim_token = %s FOR UPDATE",
                       (claim.board_id, claim.token)).fetchone()
    if row is None:
        raise LeaseLost(f"board {claim.board_id}: lease expired and the board was claimed again")
    if ok:
        conn.execute("""
            UPDATE boards SET next_fetch_at = date_trunc('hour', now()) + poll_interval, last_attempt_at = now(),
                             consecutive_failures = 0, claimed_until = NULL, claim_token = NULL
            WHERE id = %s""", (claim.board_id,))
        return
    failures = row[0] + 1
    disabled = f"{failures} consecutive failures, last: {error}" if failures >= MAX_FAILURES else None
    conn.execute("""
        UPDATE boards SET
            next_fetch_at = least(greatest(coalesce(%(retry_after)s, now() + %(wait)s), now() + %(floor)s),
                                  now() + %(cap)s),
            last_attempt_at = now(), consecutive_failures = %(failures)s,
            claimed_until = NULL, claim_token = NULL,
            disabled_at = CASE WHEN %(disabled)s::text IS NULL THEN disabled_at ELSE now() END,
            disabled_reason = coalesce(%(disabled)s, disabled_reason)
        WHERE id = %(id)s""",
        {"retry_after": retry_after, "wait": backoff(failures), "floor": MIN_BACKOFF / 2, "cap": MAX_BACKOFF,
         "failures": failures, "disabled": disabled, "id": claim.board_id})
