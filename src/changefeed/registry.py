"""The board registry: which (source, token) pairs the collector polls.

Candidates are sampled from a published token list (registry/NOTICE names the source), then
`validate` fetches each once and keeps only those that answer 200 with at least one posting.
`export` writes the active set as TSV for the repo; `load` puts it into another database.
"""
import random
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import psycopg
from psycopg.types.json import Jsonb

from changefeed.adapters import ADAPTERS, ShapeError, parse_board
from changefeed.fetch import FetchError, FetchRefused, fetch as http_fetch

PER_HOST = 2  # concurrent requests to any one API


def sample(tokens: list[str], n: int, *, seed: int) -> list[str]:
    """n tokens chosen at random but reproducibly: the same list and seed give the same sample."""
    pool = sorted(set(tokens))
    return sorted(random.Random(seed).sample(pool, min(n, len(pool))))


def import_tokens(conn: psycopg.Connection, source: str, tokens: list[str]) -> tuple[int, int]:
    """Add candidates. Returns (inserted, skipped as malformed); existing boards are left alone."""
    inserted = skipped = 0
    with conn.transaction():
        for token in tokens:
            try:
                ADAPTERS[source].url(token)
            except ValueError:
                skipped += 1
                continue
            inserted += conn.execute("""INSERT INTO boards (source, token) VALUES (%s, %s)
                                        ON CONFLICT (source, token) DO NOTHING""", (source, token)).rowcount
    return inserted, skipped


def _check(source: str, token: str, fetch) -> tuple[str, dict]:
    try:
        resp = fetch(ADAPTERS[source].url(token))
    except (FetchError, FetchRefused) as e:
        return "rejected", {"error": f"fetch: {e}"}
    seen = {"http_status": resp.status, "bytes": len(resp.body)}
    if resp.status != 200:
        return "rejected", {**seen, "error": f"HTTP {resp.status}"}
    try:
        board = parse_board(source, resp.body)
    except ShapeError as e:
        return "rejected", {**seen, "error": f"parse: {e}"}
    if not board.postings:
        return "rejected", {**seen, "postings": 0, "error": "no postings"}
    return "active", {**seen, "postings": len(board.postings)}


def validate(conn: psycopg.Connection, *, fetch=http_fetch, workers: int = 4) -> Counter:
    """Fetch every unvalidated candidate once and mark it active or rejected."""
    boards = conn.execute("SELECT id, source, token FROM boards WHERE status = 'candidate' ORDER BY id").fetchall()
    gates = {s: threading.Semaphore(PER_HOST) for s in ADAPTERS}

    def one(row):
        board_id, source, token = row
        with gates[source]:
            return board_id, *_check(source, token, fetch)

    counts = Counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for board_id, status, seen in pool.map(one, boards):
            conn.execute("UPDATE boards SET status = %s, validation = %s, validated_at = now() WHERE id = %s",
                         (status, Jsonb(seen), board_id))
            counts[status] += 1
    return counts


def export(conn: psycopg.Connection) -> str:
    rows = conn.execute("""SELECT source, token, validation->>'postings', validation->>'bytes' FROM boards
                           WHERE status = 'active' ORDER BY source, token""").fetchall()
    return "\n".join(["source\ttoken\tpostings\tbytes", *("\t".join(map(str, r)) for r in rows)]) + "\n"


def load(conn: psycopg.Connection, tsv: str) -> int:
    """Insert the exported boards as active. Returns how many were new."""
    lines = [l.split("\t") for l in tsv.splitlines()[1:] if l.strip()]
    with conn.transaction():
        return sum(conn.execute("""INSERT INTO boards (source, token, status) VALUES (%s, %s, 'active')
                                   ON CONFLICT (source, token) DO NOTHING""", (src, tok)).rowcount
                   for src, tok, *_ in lines)
