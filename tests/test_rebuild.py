"""M4 through the collector: versions per fetch, rebuild from raw bodies, normaliser bumps."""
import json

import pytest

from conftest import Reply
from changefeed import adapters, collect, versions
from changefeed.adapters import ADAPTERS, Adapter


@pytest.fixture
def api(server, monkeypatch):
    monkeypatch.setenv("CHANGEFEED_EXTRA_HOSTS", "127.0.0.1")
    for source, adapter in list(ADAPTERS.items()):
        monkeypatch.setitem(ADAPTERS, source, Adapter(server.url(f"/{source}/{{}}"), adapter.parse))
    return server


@pytest.fixture
def bid(conn):
    return conn.execute("INSERT INTO boards (source, token, status) VALUES ('greenhouse', 'acme', 'active') RETURNING id").fetchone()[0]


def gh(*jobs):
    return json.dumps({"jobs": [{"id": i, "title": t, "absolute_url": f"https://e/{i}", "updated_at": u}
                                for i, t, u in jobs], "meta": {"total": len(jobs)}}).encode()


T1, T2 = "2026-10-30T10:00:00Z", "2026-10-31T10:00:00Z"


def snapshot(conn):
    return (conn.execute("""SELECT p.external_id, v.field, v.value, v.valid_from, v.valid_from_source, v.recorded_at
                            FROM posting_field_versions v JOIN postings_current p ON p.id = v.posting_id
                            ORDER BY 1, 2, v.valid_from""").fetchall(),
            conn.execute("""SELECT external_id, fields, present, absent_count, absent_since, first_seen_at, last_seen_at
                            FROM postings_current ORDER BY 1""").fetchall())


def script():
    """A fresh list each time: the server consumes the replies it serves."""
    return [Reply(200, gh((1, "A", T1), (2, "B", T1)), {"ETag": '"1"'}),
            Reply(304),
            Reply(200, gh((1, "A2", T2)), {"ETag": '"2"'}),   # 1 edited, 2 gone once
            Reply(304),                                        # 2 gone twice: closed
            Reply(200, gh((1, "A2", T2), (2, "B", T1)), {"ETag": '"3"'})]  # 2 is back


def test_the_poller_versions_and_logs_counts(conn, api, bid):
    api.routes["/greenhouse/acme"] = script()
    assert [collect.poll_board(conn, bid) for _ in range(5)] == ["ok", "not_modified", "ok", "not_modified", "ok"]
    assert [r[0] for r in conn.execute("SELECT versions FROM fetch_log ORDER BY id")] == [10, 0, 1, 1, 1], "5 each: present, title, url, and the empty departments and offices lists"
    recorded = conn.execute("SELECT recorded_at FROM fetch_log ORDER BY id").fetchall()
    assert conn.execute("SELECT DISTINCT recorded_at FROM posting_field_versions ORDER BY 1").fetchall() == \
        [recorded[0], recorded[2], recorded[3], recorded[4]], "versions carry their fetch's recorded time"


def test_rebuild_from_raw_bodies_matches_the_incremental_history(conn, api, bid):
    api.routes["/greenhouse/acme"] = script()
    for _ in range(5):
        collect.poll_board(conn, bid)
    before = snapshot(conn)
    with conn.transaction():
        assert versions.rebuild(conn, bid) == 13
    assert snapshot(conn) == before


def test_rebuild_backfills_bodies_collected_before_versioning(conn, api, bid):
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "A", T1)))]
    collect.poll_board(conn, bid)
    conn.execute("DELETE FROM postings_current")  # as if fetched before M4 existed
    with conn.transaction():
        versions.rebuild(conn, bid)
    assert conn.execute("SELECT count(*) FROM posting_field_versions").fetchone()[0] == 5


def test_a_real_change_right_after_a_normaliser_bump_is_versioned(conn, api, bid, monkeypatch):
    api.routes["/greenhouse/acme"] = [Reply(200, gh((1, "eng", T1), (2, "ops", T1))),
                                      Reply(200, gh((1, "eng", T1), (2, "ops lead", T1)))]
    collect.poll_board(conn, bid)
    real_text = adapters._text
    monkeypatch.setattr(adapters, "_text", lambda v: real_text(v).upper() if isinstance(v, str) else real_text(v))
    monkeypatch.setattr(adapters, "NORMALIZER_VERSION", adapters.NORMALIZER_VERSION + 1)
    assert collect.poll_board(conn, bid) == "ok"
    titles = conn.execute("""SELECT p.external_id, v.value FROM posting_field_versions v
                             JOIN postings_current p ON p.id = v.posting_id WHERE v.field = 'title' ORDER BY v.id""").fetchall()
    assert titles == [("1", "eng"), ("2", "ops"), ("2", "OPS LEAD")], "the re-baseline itself wrote nothing"
    assert conn.execute("SELECT normalizer_version FROM boards").fetchone()[0] == adapters.NORMALIZER_VERSION
