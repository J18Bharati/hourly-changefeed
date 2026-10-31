"""Decisions from the part B /review (2026-11-01): board observations in the feed (format 2),
rebuild refused after emit, drift alerts for emitted hours, and a cheaper hourly load."""
import hashlib
import json
from datetime import timedelta

import psycopg
import pytest

from changefeed import cli, emit, load, ops, reports
from test_emit import top, world  # noqa: F401  (fixtures)
from test_load import consumer, manifests, q  # noqa: F401  (fixtures)


@pytest.fixture
def observed(conn, world, top):
    """Board acme: an error then a success in hour -1. Board beta: only errors, in hours -3 and -1."""
    acme = conn.execute("SELECT id FROM boards WHERE token = 'acme'").fetchone()[0]
    beta = conn.execute("INSERT INTO boards (source, token, status) VALUES ('lever', 'beta', 'active') RETURNING id").fetchone()[0]
    for board, outcome, at in [(acme, "http_error", top - timedelta(minutes=40)), (acme, "ok", top - timedelta(minutes=30)),
                               (beta, "http_error", top - timedelta(hours=3) + timedelta(minutes=5)),
                               (beta, "network_error", top - timedelta(minutes=20))]:
        conn.execute("""INSERT INTO fetch_log (board_id, started_at, recorded_at, duration_ms, outcome)
                        VALUES (%s, %s, %s, 1, %s)""", (board, at, at, outcome))
    # the world fixture's own fetch of acme in hour -3
    conn.execute("UPDATE fetch_log SET recorded_at = started_at WHERE board_id = %s AND outcome = 'ok' AND started_at < %s",
                 (acme, top - timedelta(hours=2)))
    return world


@pytest.fixture
def files2(dbname, observed, tmp_path):
    out = tmp_path / "changes"
    assert emit.emit(f"dbname={dbname}", out) == 3
    return out


# --- D3: what each board's fetches said, hour by hour -----------------------------------------

def test_manifests_say_how_each_board_was_last_fetched_in_the_hour(files2):
    ms = [json.loads(p.read_bytes()) for p in manifests(files2)]
    assert [m["format"] for m in ms] == [2, 2, 2]
    assert [m["boards"] for m in ms] == [
        {"lever/acme": "ok", "lever/beta": "http_error"},
        {},
        {"lever/acme": "ok", "lever/beta": "network_error"}]


def test_the_loader_keeps_board_outcomes(consumer, files2):
    load.load(consumer, files2)
    assert q(consumer, "SELECT source, board, outcome FROM board_observations ORDER BY hour, board") == [
        ("lever", "acme", "ok"), ("lever", "beta", "http_error"),
        ("lever", "acme", "ok"), ("lever", "beta", "network_error")]


def test_the_loader_still_reads_format_1(consumer, files2):
    prev = None
    for path in manifests(files2):  # rewrite the chain as format 1, without board outcomes
        m = json.loads(path.read_bytes())
        del m["boards"]
        raw = emit._dumps({**m, "format": 1, "prev_manifest_sha256": prev})
        path.write_bytes(raw)
        prev = hashlib.sha256(raw).hexdigest()
    assert load.load(consumer, files2) == 3
    assert q(consumer, "SELECT count(*) FROM board_observations") == [(0,)]


def test_reports_leave_out_and_name_boards_not_seen_lately(consumer, files2):
    load.load(consumer, files2)
    with psycopg.connect(consumer, autocommit=True) as c:
        p = c.execute("INSERT INTO postings (source, board, external_id) VALUES ('lever', 'beta', 'b1') RETURNING id").fetchone()[0]
        for field, value in [("present", True), ("title", "B")]:
            c.execute("""INSERT INTO field_versions (posting_id, field, valid_from, value, valid_from_source, recorded_at,
                                                     baseline, hour)
                         SELECT %s, %s, min(hour), %s, 'observed', min(hour), true, min(hour) FROM loaded_files""",
                      (p, field, json.dumps(value)))
        boards = {(s, b) for s, b, *_ in reports.fill_rates(c)}
        assert ("lever", "acme") in boards and ("lever", "beta") not in boards, "beta has only failed for 3 hours"
        assert "no fresh fetch in 24 h: lever/beta" in reports.render(c)


def test_a_day_with_no_fetches_at_all_marks_every_board_unseen(consumer, files2):
    # Once the feed reports boards, an empty window is the strongest signal, not a reason to stop checking.
    load.load(consumer, files2)
    with psycopg.connect(consumer, autocommit=True) as c:
        later = c.execute("SELECT max(hour) + interval '30 hours' FROM loaded_files").fetchone()[0]
        assert reports.unseen_boards(c, at=later) == ["lever/acme"]  # every board with postings


# --- D1, D2: history already published -----------------------------------------------------------

def test_rebuild_is_refused_once_hours_are_emitted(dbname, files2, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
    assert cli.main(["rebuild-versions"]) == 1
    assert "already emitted" in capsys.readouterr().err


def test_status_alerts_when_an_emitted_hour_changes(conn, files2, top):
    assert "changed since emitted" not in ops.status(conn)
    conn.execute("""UPDATE posting_field_versions SET recorded_at = recorded_at - interval '2 hours'
                    WHERE recorded_at >= %s""", (top - timedelta(hours=1),))  # e.g. the clock stepped back
    text = ops.status(conn)
    assert "changed since emitted" in text and "1 rows emitted, 0 now" in text


# --- D4: the hourly load reads only new files ------------------------------------------------------

def test_the_hourly_load_skips_old_files_and_verify_rechecks_them(consumer, files2):
    load.load(consumer, files2)
    first = manifests(files2)[0]
    m = json.loads(first.read_bytes())
    first.write_bytes(emit._dumps({**m, "collection": {"runs": 2, "missed": None}}))
    assert load.load(consumer, files2) == 0, "an hour at or before the newest loaded is not read"
    with pytest.raises(load.LoadError, match="already loaded"):
        load.load(consumer, files2, verify=True)


def test_consumer_versions_are_indexed_by_hour(consumer):
    assert q(consumer, "SELECT count(*) FROM pg_indexes WHERE tablename = 'field_versions' AND indexdef LIKE '%%(hour)%%'") == [(1,)]
