"""M3: fetching boards on a schedule, logging every attempt, keeping raw bodies, refusing stale copies."""
import json
from datetime import datetime
import threading
import time
from collections import Counter

import pytest

from conftest import Reply
from changefeed import adapters, collect, sched
from changefeed.adapters import ADAPTERS, Adapter


@pytest.fixture
def api(server, monkeypatch):
    """Point every adapter at the scripted server: /<source>/<token>."""
    monkeypatch.setenv("CHANGEFEED_EXTRA_HOSTS", "127.0.0.1")
    for source, adapter in list(ADAPTERS.items()):
        monkeypatch.setitem(ADAPTERS, source, Adapter(server.url(f"/{source}/{{}}"), adapter.parse))
    return server


@pytest.fixture
def board(conn):
    def _add(token="acme", source="greenhouse"):
        return conn.execute("INSERT INTO boards (source, token, status) VALUES (%s, %s, 'active') RETURNING id",
                            (source, token)).fetchone()[0]
    return _add


def gh(*jobs):
    """A Greenhouse body. Each job is (id, title, updated_at)."""
    return json.dumps({"jobs": [{"id": i, "title": t, "absolute_url": f"https://e/{i}", "updated_at": u}
                                for i, t, u in jobs], "meta": {"total": len(jobs)}}).encode()


T1, T2, T3 = "2026-10-30T10:00:00Z", "2026-10-31T10:00:00Z", "2026-11-01T10:00:00Z"


def log(conn):
    return conn.execute("SELECT outcome, baseline, error FROM fetch_log ORDER BY id").fetchall()


def poll(conn, board_id):
    return collect.poll_board(conn, board_id)


# --- the basic cycle ------------------------------------------------------------------------

def test_first_fetch_is_a_baseline_and_keeps_the_raw_body(conn, api, board):
    api.routes["/greenhouse/acme"] = Reply(200, gh((1, "A", T1), (2, "B", T2)), {"ETag": 'W/"1"', "CF-Cache-Status": "HIT", "Set-Cookie": "x=1"})
    bid = board()
    assert poll(conn, bid) == "ok"
    assert log(conn) == [("ok", True, None)]
    headers, postings = conn.execute("SELECT headers, postings FROM fetch_log").fetchone()
    assert headers.keys() == {"date", "etag", "cf-cache-status"}, "allowlisted headers only (no Set-Cookie, no Server)"
    assert headers["etag"] == 'W/"1"' and postings == 2
    assert conn.execute("SELECT count(*), sum(bytes) FROM raw_bodies").fetchone() == (1, len(gh((1, "A", T1), (2, "B", T2))))
    assert conn.execute("SELECT external_id FROM postings_current ORDER BY 1").fetchall() == [("1",), ("2",)]


def test_raw_bodies_are_lz4_compressed(conn):
    assert conn.execute("""SELECT attcompression FROM pg_attribute
                           WHERE attrelid = 'raw_bodies'::regclass AND attname = 'body'""").fetchone()[0] == "l"


def test_etag_match_is_not_modified_and_same_bytes_are_unchanged(conn, api, board):
    body = gh((1, "A", T1))
    api.routes["/greenhouse/acme"] = lambda h: Reply(304) if h.get("If-None-Match") == 'W/"1"' else Reply(200, body, {"ETag": 'W/"1"'})
    api.routes["/greenhouse/beta"] = Reply(200, body)  # no validators: the body hash catches it
    a, b = board("acme"), board("beta")
    assert [poll(conn, a), poll(conn, a)] == ["ok", "not_modified"]
    assert [poll(conn, b), poll(conn, b)] == ["ok", "unchanged"]
    assert conn.execute("SELECT count(*) FROM raw_bodies").fetchone()[0] == 1, "one body, seen by two boards"
    assert [r[1] for r in log(conn)] == [True, False, True, False]


def test_a_changed_body_is_stored_again(conn, api, board):
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "A", T1))), Reply(200, gh((1, "A edited", T2)))]
    bid = board()
    assert [poll(conn, bid), poll(conn, bid)] == ["ok", "ok"]
    assert conn.execute("SELECT count(*) FROM raw_bodies").fetchone()[0] == 2
    assert conn.execute("SELECT source_ts FROM postings_current").fetchone()[0] == datetime.fromisoformat(T2)


# --- failures --------------------------------------------------------------------------------

def test_an_error_body_with_a_200_changes_nothing(conn, api, board):
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "A", T1)), {"ETag": '"1"'}),
                                      Reply(200, b'{"status":404,"error":"Job not found"}', {"ETag": '"2"'})]
    bid = board()
    poll(conn, bid)
    assert poll(conn, bid) == "parse_error"
    assert conn.execute("SELECT etag, consecutive_failures FROM boards").fetchone() == ('"1"', 1)
    assert conn.execute("SELECT count(*) FROM postings_current").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM raw_bodies").fetchone()[0] == 2, "the bad body is kept for diagnosis"


def test_a_404_backs_off_and_keeps_the_postings(conn, api, board):
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "A", T1))), Reply(404, b'{"status":404}')]
    bid = board()
    poll(conn, bid)
    assert poll(conn, bid) == "http_error"
    assert conn.execute("SELECT count(*) FROM postings_current").fetchone()[0] == 1, "a 404 is not a closed board"
    wait = conn.execute("SELECT next_fetch_at - now() FROM boards").fetchone()[0]
    assert wait.total_seconds() < 120, "first failure: about a minute, not an hour"


def test_429_retry_after_is_obeyed(conn, api, board):
    api.routes["/greenhouse/acme"] = Reply(429, b"", {"Retry-After": "1800"})
    bid = board()
    assert poll(conn, bid) == "http_error"
    wait = conn.execute("SELECT next_fetch_at - now(), retry_after IS NOT NULL FROM boards, fetch_log").fetchone()
    assert 1790 < wait[0].total_seconds() <= 1800 and wait[1]


# --- stale copies --------------------------------------------------------------------------

def test_a_posting_going_back_in_time_is_a_stale_copy(conn, api, board):
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "A edited", T2), (2, "B", T1))),
                                      Reply(200, gh((1, "A", T1), (2, "B", T1))),        # a lagging replica
                                      Reply(200, gh((1, "A edited again", T3), (2, "B", T1)))]
    bid = board()
    assert [poll(conn, bid) for _ in range(3)] == ["ok", "stale", "ok"]
    assert "went back" in log(conn)[1][2]
    assert conn.execute("SELECT count(*) FROM raw_bodies").fetchone()[0] == 3


def test_closing_the_newest_posting_is_not_stale(conn, api, board):
    # The plan's first rule (newest source time per board went backwards) would call this stale,
    # and keep calling it stale until a newer posting appeared.
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "A", T1), (2, "B", T3))), Reply(200, gh((1, "A", T1)))]
    bid = board()
    assert [poll(conn, bid), poll(conn, bid)] == ["ok", "ok"]


def test_a_rollback_is_accepted_once_it_has_held_for_a_day(conn, api, board):
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "new", T2)))] + [Reply(200, gh((1, "old", T1)))] * 5
    bid = board()
    assert [poll(conn, bid) for _ in range(4)] == ["ok", "stale", "stale", "stale"]
    conn.execute("UPDATE fetch_log SET started_at = started_at - interval '25 hours'")
    assert poll(conn, bid) == "ok"


# --- runs -------------------------------------------------------------------------------------

def test_a_run_fetches_every_due_board_and_records_itself(conn, dbname, api, board):
    api.routes["/greenhouse/a"] = Reply(200, gh((1, "A", T1)))
    api.routes["/greenhouse/b"] = Reply(500, b"oops")
    api.routes["/lever/c"] = Reply(200, b"[]")
    board("a"), board("b"), board("c", "lever")
    run = collect.run(f"dbname={dbname}")
    assert run.counts == {"ok": 2, "http_error": 1}
    row = conn.execute("SELECT status, counts, finished_at IS NOT NULL FROM collection_runs").fetchone()
    assert row == ("partial", {"ok": 2, "http_error": 1}, True)
    assert conn.execute("SELECT count(DISTINCT run_id) FROM fetch_log").fetchone()[0] == 1
    assert collect.run(f"dbname={dbname}").counts == {}, "nothing is due again within the run's hour"


def test_one_board_failing_leaves_the_others_committed(conn, dbname, api, board, monkeypatch):
    api.routes["/greenhouse/a"] = Reply(200, gh((1, "A", T1)))
    api.routes["/greenhouse/b"] = Reply(200, gh((2, "B", T1)))
    board("a"), board("b")
    real = collect.store_postings

    def explode_for_b(conn, board_id, *a, **kw):
        if conn.execute("SELECT token FROM boards WHERE id = %s", (board_id,)).fetchone()[0] == "b":
            raise RuntimeError("disk on fire")
        return real(conn, board_id, *a, **kw)
    monkeypatch.setattr(collect, "store_postings", explode_for_b)
    assert collect.run(f"dbname={dbname}").counts == {"ok": 1, "crashed": 1}
    assert conn.execute("SELECT external_id FROM postings_current").fetchall() == [("1",)]


def test_at_most_two_requests_per_api_at_once(conn, dbname, api, board):
    live, peak, lock = Counter(), Counter(), threading.Lock()

    def slow(source):
        def reply(_headers):
            with lock:
                live[source] += 1
                peak[source] = max(peak[source], live[source])
            time.sleep(0.15)
            with lock:
                live[source] -= 1
            return Reply(200, b"[]" if source == "lever" else gh((1, "A", T1)))
        return reply
    for i in range(6):
        api.routes[f"/greenhouse/g{i}"] = slow("greenhouse")
        api.routes[f"/lever/l{i}"] = slow("lever")
        board(f"g{i}"), board(f"l{i}", "lever")
    collect.run(f"dbname={dbname}")
    assert peak == {"greenhouse": 2, "lever": 2}


def test_low_disk_refuses_the_run_and_says_so(conn, dbname, api, board, monkeypatch):
    api.routes["/greenhouse/a"] = Reply(200, gh((1, "A", T1)))
    board("a")
    monkeypatch.setattr(collect, "free_bytes", lambda conn: 5 * 1024**3)
    run = collect.run(f"dbname={dbname}")
    assert run.status == "missed" and api.requests == []
    assert conn.execute("SELECT status, reason FROM collection_runs").fetchone() == ("missed", "disk: 5.0 GiB free, need 10")


def test_due_boards_are_claimed_once_across_workers(conn, connect, board):
    for i in range(20):
        board(f"t{i}")
    a, b = connect(), connect()
    with a.transaction():
        mine = sched.claim_due(a, 12, collect.LEASE)
        theirs = sched.claim_due(b, 20, collect.LEASE)
    assert len(mine) == 12 and len(theirs) == 8
    assert not {c.board_id for c in mine} & {c.board_id for c in theirs}
