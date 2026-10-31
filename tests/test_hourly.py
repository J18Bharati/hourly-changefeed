"""M5a: the hourly entry point: missed hours, one run at a time, the pinned Postgres major."""
import psycopg
import pytest

from changefeed import cli, collect


def hours(conn):
    return conn.execute("""SELECT extract(epoch FROM date_trunc('hour', now(), 'UTC') - hour)::int / 3600, status, reason
                           FROM collection_runs ORDER BY hour""").fetchall()


def test_hours_with_no_run_are_recorded_as_missed(conn):
    conn.execute("INSERT INTO collection_runs (hour, status) VALUES (date_trunc('hour', now(), 'UTC') - interval '4 hours', 'ok')")
    assert collect.record_missed(conn) == 3
    assert hours(conn) == [(4, "ok", None), (3, "missed", "no run"), (2, "missed", "no run"), (1, "missed", "no run")]
    assert collect.record_missed(conn) == 0, "idempotent"


def test_the_first_run_ever_misses_nothing(conn):
    assert collect.record_missed(conn) == 0


def test_a_run_records_its_hour(conn, dbname):
    collect.run(f"dbname={dbname}")
    assert hours(conn) == [(0, "ok", None)]


def test_run_hour_fills_gaps_then_runs(conn, dbname):
    conn.execute("INSERT INTO collection_runs (hour, status) VALUES (date_trunc('hour', now(), 'UTC') - interval '2 hours', 'ok')")
    assert collect.run_hour(f"dbname={dbname}").status == "ok"
    assert [h for h, *_ in hours(conn)] == [2, 1, 0]


def test_a_second_run_hour_waits_for_nobody(conn, dbname, connect):
    holder = connect()
    holder.execute("SELECT pg_advisory_lock(%s)", (collect.RUN_LOCK,))
    assert collect.run_hour(f"dbname={dbname}") is None
    assert hours(conn) == [], "no run row, no missed rows: the holder will write them"


def test_run_hour_command_reports_the_lock(dbname, connect, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
    connect().execute("SELECT pg_advisory_lock(%s)", (collect.RUN_LOCK,))
    assert cli.main(["run-hour"]) == 0
    assert "another run holds the lock" in capsys.readouterr().out


def test_a_different_postgres_major_is_refused(dbname, monkeypatch):
    monkeypatch.setattr(collect, "PG_MAJOR", 16)
    with pytest.raises(collect.WrongPostgres, match="17"):
        collect.run_hour(f"dbname={dbname}")
