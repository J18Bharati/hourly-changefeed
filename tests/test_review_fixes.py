"""Regressions from the part B /review (2026-11-01): each test failed before its fix."""
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
import pytest

from conftest import _admin, _drop
from changefeed import adapters, cli, collect, emit, load, ops, reports
from test_emit import top, world  # noqa: F401  (fixtures)
from test_load import consumer, files, manifests, q  # noqa: F401  (fixtures)

UNITS = Path(__file__).parent.parent / "deploy" / "systemd"


# --- emit -------------------------------------------------------------------------------------

def test_hours_step_in_utc_across_a_dst_change():
    # psycopg hands back timestamps in the session's zone; adding an hour to a zoned datetime is
    # wall-clock arithmetic, which skips 06:00Z at New York's fall-back.
    start = datetime(2025, 11, 2, 0, 0, tzinfo=ZoneInfo("America/New_York"))  # 04:00Z
    got = list(emit._hours(start, datetime(2025, 11, 2, 9, tzinfo=timezone.utc)))
    assert [h.hour for h in got] == [4, 5, 6, 7, 8] and all(h.tzinfo == timezone.utc for h in got)


def test_a_second_emit_chains_onto_the_first(conn, dbname, world, top, tmp_path):
    conn.execute("UPDATE collection_runs SET status = 'running', finished_at = NULL, started_at = %s WHERE id = %s",
                 (top - timedelta(microseconds=1), world[1]))
    assert emit.emit(f"dbname={dbname}", tmp_path) == 2
    conn.execute("UPDATE collection_runs SET status = 'ok', finished_at = started_at WHERE id = %s", (world[1],))
    assert emit.emit(f"dbname={dbname}", tmp_path) == 1
    ms = manifests(tmp_path)
    assert json.loads(ms[2].read_bytes())["prev_manifest_sha256"] == hashlib.sha256(ms[1].read_bytes()).hexdigest()


def test_emit_refuses_to_continue_a_chain_off_the_utc_hour(conn, dbname, world, top, tmp_path):
    # What migration 0008 left behind wherever a :30 hour had been emitted (fixed by hand live).
    conn.execute("INSERT INTO emitted_hours (hour, rows, sha256, manifest_sha256) VALUES (%s, 0, '', '')",
                 (top - timedelta(hours=4, minutes=30),))
    with pytest.raises(emit.EmitError, match="UTC hour"):
        emit.emit(f"dbname={dbname}", tmp_path)


def test_a_manual_run_waits_its_turn_for_the_run_lock(conn, dbname, connect, monkeypatch, capsys):
    other = connect()
    other.execute("SELECT pg_advisory_lock(%s)", (collect.RUN_LOCK,))
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
    assert cli.main(["run"]) == 0
    assert "another run holds the lock" in capsys.readouterr().out
    assert conn.execute("SELECT count(*) FROM collection_runs").fetchone()[0] == 0


# --- units ------------------------------------------------------------------------------------

def test_a_missed_run_still_lets_emit_and_load_run_but_a_crash_does_not():
    # A missed run (low disk) exits 2, which the unit accepts; any other failure still fails it.
    text = (UNITS / "changefeed.service").read_text()
    assert "SuccessExitStatus=2" in text
    assert not any(l.startswith("ExecStart=-") for l in text.splitlines()), "'-' would hide real failures too"


def test_a_missed_run_exits_2(dbname, monkeypatch):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
    monkeypatch.setattr(collect, "free_bytes", lambda conn: 0)
    assert cli.main(["run-hour"]) == 2


def test_the_timer_fires_on_utc_hours():
    # OnCalendar=hourly is local time: :30 UTC in Asia/Kolkata.
    assert "OnCalendar=*-*-* *:00:00 UTC" in (UNITS / "changefeed.timer").read_text()


# --- load -------------------------------------------------------------------------------------

def test_a_missing_data_file_is_refused(consumer, files):
    last = manifests(files)[-1]
    last.with_name(last.name.replace(".manifest.json", ".jsonl")).unlink()
    with pytest.raises(load.LoadError, match="missing|No such"):
        load.load(consumer, files)
    assert q(consumer, "SELECT count(*) FROM loaded_files") == [(2,)]


def test_a_bad_row_rolls_back_its_whole_hour(consumer, files):
    last = manifests(files)[-1]
    data_path = last.with_name(last.name.replace(".manifest.json", ".jsonl"))
    row = json.loads(data_path.read_bytes())
    row["valid_from"] = "2999-01-01T00:00:00+00:00"  # after recorded_at: the table's CHECK refuses it
    data = emit._dumps(row)
    data_path.write_bytes(data)
    m = json.loads(last.read_bytes())
    last.write_bytes(emit._dumps({**m, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}))
    with pytest.raises(load.LoadError, match="refused by the database"):
        load.load(consumer, files)
    assert q(consumer, "SELECT count(*) FROM loaded_files") == [(2,)], "no ledger row without its versions"
    assert q(consumer, "SELECT count(*) FROM field_versions") == [(2,)]


def test_load_and_report_need_the_consumer_database_named(files, monkeypatch, capsys):
    monkeypatch.delenv("CHANGEFEED_CONSUMER_DB", raising=False)
    assert cli.main(["load", "--dir", str(files)]) == 1
    assert cli.main(["report"]) == 1
    assert "CHANGEFEED_CONSUMER_DB" in capsys.readouterr().err


def test_consumer_setup_reports_a_refusal_without_a_traceback(dbname, monkeypatch, capsys):
    role = f"cf_loader_{uuid.uuid4().hex[:8]}"
    try:
        with _admin() as admin:
            admin.execute(f"CREATE ROLE {role} LOGIN SUPERUSER")
        monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
        assert cli.main(["consumer-setup", "--role", role, "--consumer-db", f"cdb_{uuid.uuid4().hex[:8]}"]) == 1
        assert "superuser" in capsys.readouterr().err
    finally:
        with _admin() as admin:
            admin.execute(f"DROP ROLE IF EXISTS {role}")


def test_an_unmigrated_consumer_is_refused_with_a_hint(files, tmp_path):
    from conftest import _scratch_db
    name = _scratch_db("template0")
    try:
        with pytest.raises(load.LoadError, match="consumer-setup"):
            load.load(f"dbname={name}", files)
    finally:
        _drop(name)


def test_provision_refuses_a_loader_role_that_could_reach_the_producer(dbname):
    role, cdb = f"cf_loader_{uuid.uuid4().hex[:8]}", f"changefeed_consumer_{uuid.uuid4().hex[:8]}"
    try:
        with _admin() as admin:
            admin.execute(f"CREATE ROLE {role} LOGIN SUPERUSER")
            with pytest.raises(load.LoadError, match="superuser"):
                load.provision(admin, consumer_db=cdb, producer_db=dbname, role=role)
    finally:
        _drop(cdb)
        with _admin() as admin:
            admin.execute(f"DROP ROLE IF EXISTS {role}")


# --- collection -------------------------------------------------------------------------------

def test_nul_and_lone_surrogates_never_reach_postgres(conn):
    # jsonb refuses both, which would crash the board's store every hour until it was disabled.
    body = json.dumps({"jobs": [{"id": 1, "title": "Eng\u0000ineer \ud800", "absolute_url": "https://e/1",
                                 "updated_at": "2026-10-30T10:00:00Z"}]}).encode()
    (p,) = adapters.parse_board("greenhouse", body).postings
    conn.execute("SELECT %s::jsonb", (json.dumps(p.fields),))
    assert "\x00" not in p.fields["title"] and p.fields["title"].startswith("Engineer")


# --- operations ---------------------------------------------------------------------------------

def test_health_notices_when_emit_falls_behind(conn):
    bid = conn.execute("INSERT INTO boards (source, token, status) VALUES ('lever', 'a', 'active') RETURNING id").fetchone()[0]
    conn.execute("INSERT INTO fetch_log (board_id, started_at, duration_ms, outcome) VALUES (%s, now(), 1, 'ok')", (bid,))
    conn.execute("""INSERT INTO collection_runs (hour, started_at, finished_at, status)
                    VALUES (date_trunc('hour', now(), 'UTC') - interval '5 hours', now() - interval '5 hours', now(), 'ok')""")
    ok, message = ops.health(conn)
    assert not ok and "change files" in message
    conn.execute("""INSERT INTO emitted_hours (hour, rows, sha256, manifest_sha256)
                    VALUES (date_trunc('hour', now(), 'UTC') - interval '1 hour', 0, '', '')""")
    assert ops.health(conn)[0] is True


def test_a_failed_backup_leaves_no_partial_dump(dbname, tmp_path, monkeypatch):
    def fail(cmd, **kw):
        Path(cmd[cmd.index("-f") + 1]).write_bytes(b"half a dump")
        raise ops.subprocess.CalledProcessError(1, cmd)
    monkeypatch.setattr(ops.subprocess, "run", fail)
    with pytest.raises(ops.subprocess.CalledProcessError):
        ops.backup(f"dbname={dbname}", tmp_path, stamp="x")
    assert list(tmp_path.iterdir()) == []


# --- reports ------------------------------------------------------------------------------------

def test_the_report_shows_the_newest_hour_and_escapes_what_it_did_not_write(consumer):
    with psycopg.connect(consumer, autocommit=True) as c:
        c.execute("""INSERT INTO loaded_files (hour, file, rows, sha256, manifest_sha256, runs, missed)
                     VALUES ('2026-10-30 12:00Z', 'x', 0, '', 'm', 0, E'no run\\x1b[2J')""")
        out = reports.render(c)
    assert "newest hour loaded: 2026-10-30 12:00Z" in out
    assert "\x1b" not in out


def test_the_installer_waits_for_a_running_hour_and_always_restarts_the_timer():
    text = (Path(__file__).parent.parent / "scripts" / "install-collector").read_text()
    assert "ActiveState" in text, "a running oneshot is 'activating', which is-active does not count"
    assert "trap" in text and "start changefeed.timer" in text
