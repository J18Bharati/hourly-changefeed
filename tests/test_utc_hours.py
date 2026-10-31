"""Hours are UTC hours, whatever the server's time zone. Found live on 2026-11-01: in Asia/Kolkata
(+05:30), date_trunc('hour', now()) starts each "hour" at :30 UTC, and 20.jsonl covered 20:30-21:30."""
import json

import pytest

from conftest import Reply
from changefeed import collect, emit
from changefeed.adapters import ADAPTERS, Adapter


@pytest.fixture
def kolkata(dbname, conn):
    conn.execute(f"ALTER DATABASE {dbname} SET timezone = 'Asia/Kolkata'")  # every new connection
    conn.execute("SET timezone = 'Asia/Kolkata'")


def on_the_hour_utc(conn, column, table):
    return conn.execute(f"SELECT bool_and(date_part('minute', {column} AT TIME ZONE 'UTC') = 0) FROM {table}").fetchone()[0]


def test_runs_schedules_and_files_use_utc_hours(conn, dbname, kolkata, server, monkeypatch, tmp_path):
    monkeypatch.setenv("CHANGEFEED_EXTRA_HOSTS", "127.0.0.1")
    monkeypatch.setitem(ADAPTERS, "lever", Adapter(server.url("/lever/{}"), ADAPTERS["lever"].parse))
    server.routes["/lever/acme"] = Reply(200, b'[{"id": "1", "text": "T", "hostedUrl": "https://e/1"}]')
    conn.execute("INSERT INTO boards (source, token, status) VALUES ('lever', 'acme', 'active')")
    conn.execute("""INSERT INTO collection_runs (hour, started_at, finished_at, status)
                    VALUES (date_trunc('hour', now() - interval '3 hours', 'UTC'), now() - interval '3 hours',
                            now() - interval '3 hours', 'ok')""")
    collect.run_hour(f"dbname={dbname}")
    assert on_the_hour_utc(conn, "hour", "collection_runs")
    assert on_the_hour_utc(conn, "next_fetch_at", "boards")
    emit.emit(f"dbname={dbname}", tmp_path)
    hours = [json.loads(p.read_text())["hour"] for p in sorted(tmp_path.rglob("*.manifest.json"))]
    assert hours and all(h.endswith(":00:00+00:00") for h in hours), hours
