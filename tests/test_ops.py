"""M5b: health, status, raw-body retention, growth reports and backups."""
import subprocess
from datetime import timedelta

import pytest

from changefeed import cli, ops


@pytest.fixture
def bid(conn):
    return conn.execute("INSERT INTO boards (source, token, status) VALUES ('greenhouse', 'acme', 'active') RETURNING id").fetchone()[0]


def fetched(conn, board_id, outcome="ok", ago=timedelta(0), body=None, error=None):
    h = None
    if body is not None:
        h = conn.execute("""INSERT INTO raw_bodies (hash, body, bytes, first_board, first_seen_at, last_seen_at)
                            VALUES (md5(%(b)s), %(b)s, length(%(b)s), %(board)s, now() - %(ago)s, now() - %(ago)s)
                            ON CONFLICT (hash) DO UPDATE SET last_seen_at = excluded.last_seen_at
                            RETURNING hash""", {"b": body, "board": board_id, "ago": ago}).fetchone()[0]
    conn.execute("""INSERT INTO fetch_log (board_id, started_at, recorded_at, duration_ms, outcome, body_hash, error)
                    VALUES (%s, now() - %s, now() - %s, 10, %s, %s, %s)""", (board_id, ago, ago, outcome, h, error))
    return h


# --- health --------------------------------------------------------------------------------------

def test_health_needs_a_recent_success(conn, bid):
    assert ops.health(conn)[0] is False, "nothing fetched yet"
    fetched(conn, bid, "http_error")
    assert ops.health(conn)[0] is False, "failures don't count"
    fetched(conn, bid, "not_modified", ago=timedelta(hours=3))
    ok, message = ops.health(conn)
    assert not ok and "3.0 h ago" in message
    fetched(conn, bid, "unchanged", ago=timedelta(minutes=30))
    assert ops.health(conn)[0] is True


def test_health_command_exit_code(dbname, conn, bid, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
    assert cli.main(["health"]) == 1
    fetched(conn, bid)
    assert cli.main(["health"]) == 0
    assert "last success" in capsys.readouterr().out


# --- status ---------------------------------------------------------------------------------------

def test_status_flags_repeated_stale_dead_tokens_and_missed_hours(conn, bid):
    dead = conn.execute("INSERT INTO boards (source, token, status) VALUES ('lever', 'gone', 'active') RETURNING id").fetchone()[0]
    for i in range(3):
        fetched(conn, bid, "stale", ago=timedelta(hours=i), error="posting 1 went back")
        fetched(conn, dead, "http_error", ago=timedelta(hours=i), error="HTTP 404")
    conn.execute("INSERT INTO collection_runs (hour, status, reason) VALUES (date_trunc('hour', now()) - interval '5 hours', 'missed', 'no run')")
    text = ops.status(conn)
    assert "greenhouse/acme: 3 stale replies in 24 h" in text
    assert "lever/gone: 3 consecutive 404s" in text
    assert "missed hours (7 d): 1" in text


def test_status_flags_flapping_postings(conn, bid):
    pid = conn.execute("""INSERT INTO postings_current (board_id, external_id, fields) VALUES (%s, '9', '{}')
                          RETURNING id""", (bid,)).fetchone()[0]
    for i, present in enumerate([True, False, True, False]):
        conn.execute("""INSERT INTO posting_field_versions (posting_id, field, value, valid_from, valid_from_source, recorded_at)
                        VALUES (%s, 'present', %s, now() - %s, 'observed', now() - %s)""",
                     (pid, "true" if present else "false", timedelta(hours=10 - i), timedelta(hours=10 - i)))
    assert "greenhouse/acme posting 9: closed 2 times in 7 d" in ops.status(conn)


# --- retention and growth -------------------------------------------------------------------------

def test_prune_keeps_each_boards_latest_body_and_the_last_14_days(conn, bid):
    old = fetched(conn, bid, ago=timedelta(days=30), body=b"old body")
    recent = fetched(conn, bid, ago=timedelta(days=3), body=b"recent body")
    other = conn.execute("INSERT INTO boards (source, token, status) VALUES ('ashby', 'quiet', 'active') RETURNING id").fetchone()[0]
    only = fetched(conn, other, ago=timedelta(days=40), body=b"a quiet board's only body")
    assert ops.prune(conn) == (1, len(b"old body"))
    left = {h for (h,) in conn.execute("SELECT hash FROM raw_bodies")}
    assert left == {recent, only}, "the quiet board keeps its latest body, however old"
    assert conn.execute("SELECT count(*) FROM fetch_log WHERE body_hash IS NULL").fetchone()[0] == 1, \
        "the log row stays, without its body"
    assert old not in left


def test_growth_report_is_recorded(conn, bid):
    fetched(conn, bid, body=b"x" * 5000)
    r = ops.report(conn)
    assert r["raw_rows"] == 1 and r["raw_bytes_24h"] == 5000 and r["db_bytes"] > 0
    assert conn.execute("SELECT count(*) FROM storage_reports").fetchone()[0] == 1


# --- backups ------------------------------------------------------------------------------------------

def test_backup_skips_raw_bodies_and_keeps_three(conn, dbname, bid, tmp_path):
    fetched(conn, bid, body=b"big raw body")
    paths = [ops.backup(f"dbname={dbname}", tmp_path, stamp=f"2026-11-0{i}") for i in range(1, 5)]
    assert sorted(p.name for p in tmp_path.iterdir()) == [p.name for p in paths[1:]]
    listing = subprocess.run(["pg_restore", "--list", str(paths[-1])], capture_output=True, text=True, check=True).stdout
    assert "TABLE DATA public fetch_log" in listing
    assert "TABLE DATA public raw_bodies" not in listing and "TABLE public raw_bodies" in listing
