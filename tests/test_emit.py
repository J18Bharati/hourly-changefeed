"""Part B M6: one JSONL change file and manifest per hour, deterministic, chained, atomic."""
import hashlib
import json
from datetime import timedelta

import pytest

from changefeed import emit


@pytest.fixture
def top(conn):
    return conn.execute("SELECT date_trunc('hour', now(), 'UTC')").fetchone()[0]


@pytest.fixture
def world(conn, top):
    """Runs at hours -3, -2 (missed) and -1; versions recorded in -3 and -1."""
    bid = conn.execute("INSERT INTO boards (source, token, status) VALUES ('lever', 'acme', 'active') RETURNING id").fetchone()[0]
    pid = conn.execute("INSERT INTO postings_current (board_id, external_id, fields) VALUES (%s, 'p1', '{}') RETURNING id",
                       (bid,)).fetchone()[0]
    runs = {}
    for k, status in [(3, "ok"), (2, "missed"), (1, "ok")]:
        runs[k] = conn.execute("""INSERT INTO collection_runs (hour, started_at, finished_at, status, reason)
                                  VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                               (top - k * timedelta(hours=1), top - k * timedelta(hours=1), top - k * timedelta(hours=1),
                                status, "no run" if status == "missed" else None)).fetchone()[0]
    log = conn.execute("""INSERT INTO fetch_log (board_id, run_id, started_at, duration_ms, outcome, baseline)
                          VALUES (%s, %s, %s, 1, 'ok', true) RETURNING id""", (bid, runs[3], top - 3 * timedelta(hours=1))).fetchone()[0]

    def version(field, value, hours_ago, minutes=5, log_id=None, run=None):
        at = top - hours_ago * timedelta(hours=1) + timedelta(minutes=minutes)
        conn.execute("""INSERT INTO posting_field_versions (posting_id, field, value, valid_from, valid_from_source,
                                                            recorded_at, run_id, fetch_log_id)
                        VALUES (%s, %s, %s, %s, 'observed', %s, %s, %s)""",
                     (pid, field, json.dumps(value), at - timedelta(days=1), at, run, log_id))
    version("present", True, 3, log_id=log, run=runs[3])
    version("title", "Eng", 3, log_id=log, run=runs[3])
    version("title", "Senior Eng", 1, minutes=20, run=runs[1])
    return runs


def files(out):
    return sorted(str(p.relative_to(out)) for p in out.rglob("*") if p.is_file())


def test_every_finished_hour_gets_a_file_and_a_manifest(conn, dbname, world, top, tmp_path):
    assert emit.emit(f"dbname={dbname}", tmp_path) == 3
    stamps = [(top - k * timedelta(hours=1)).astimezone(emit.UTC).strftime("%Y/%m/%d/%H") for k in (3, 2, 1)]
    assert files(tmp_path) == sorted(f"{s}.{ext}" for s in stamps for ext in ("jsonl", "manifest.json"))
    rows = [json.loads(l) for l in (tmp_path / f"{stamps[0]}.jsonl").read_text().splitlines()]
    assert [(r["field"], r["value"], r["baseline"]) for r in rows] == [("present", True, True), ("title", "Eng", True)]
    assert rows[0]["posting"] == {"source": "lever", "board": "acme", "external_id": "p1"}
    assert set(rows[0]) == {"posting", "field", "value", "valid_from", "valid_from_source", "recorded_at", "run_id", "baseline"}
    assert (tmp_path / f"{stamps[1]}.jsonl").read_bytes() == b"", "an hour with nothing recorded is an empty file"


def test_manifests_declare_missed_hours_and_chain(conn, dbname, world, top, tmp_path):
    emit.emit(f"dbname={dbname}", tmp_path)
    manifests = [json.loads(p.read_text()) for p in sorted(tmp_path.rglob("*.manifest.json"))]
    assert [m["rows"] for m in manifests] == [2, 0, 1]
    assert [m["collection"] for m in manifests] == [{"runs": 1, "missed": None},
                                                  {"runs": 0, "missed": "no run"},
                                                  {"runs": 1, "missed": None}]
    raw = [p.read_bytes() for p in sorted(tmp_path.rglob("*.manifest.json"))]
    assert manifests[0]["prev_manifest_sha256"] is None
    assert [m["prev_manifest_sha256"] for m in manifests[1:]] == [hashlib.sha256(r).hexdigest() for r in raw[:-1]]
    for m, data in zip(manifests, sorted(tmp_path.rglob("*.jsonl"))):
        assert m["sha256"] == hashlib.sha256(data.read_bytes()).hexdigest()
    assert not any("emitted" in k or "generated" in k for k in manifests[0]), "no wall-clock fields"


def test_emitting_again_is_a_no_op_and_re_emitting_gives_identical_bytes(conn, dbname, world, tmp_path):
    emit.emit(f"dbname={dbname}", tmp_path)
    first = {f: (tmp_path / f).read_bytes() for f in files(tmp_path)}
    assert emit.emit(f"dbname={dbname}", tmp_path) == 0
    conn.execute("DELETE FROM emitted_hours")
    other = tmp_path.parent / "again"
    emit.emit(f"dbname={dbname}", other)
    assert {f: (other / f).read_bytes() for f in files(other)} == first


def test_an_open_run_holds_back_its_hour(conn, dbname, world, tmp_path):
    # Started 55 minutes ago and still going: it may yet commit versions recorded in hour -1.
    conn.execute("""UPDATE collection_runs SET status = 'running', finished_at = NULL,
                    started_at = now() - interval '55 minutes' WHERE id = %s""", (world[1],))
    assert emit.emit(f"dbname={dbname}", tmp_path) == 2


def test_a_crashed_run_is_closed_rather_than_blocking_forever(conn, dbname, world, tmp_path):
    conn.execute("UPDATE collection_runs SET status = 'running', finished_at = NULL WHERE id = %s", (world[3],))
    assert emit.emit(f"dbname={dbname}", tmp_path) == 3
    assert conn.execute("SELECT status, reason FROM collection_runs WHERE id = %s", (world[3],)).fetchone() == \
        ("partial", "did not finish")


def test_a_crash_between_data_and_manifest_is_redone(conn, dbname, world, tmp_path, monkeypatch):
    real = emit._write_atomic
    calls = []

    def fail_on_first_manifest(path, data):
        calls.append(path.name)
        if path.name.endswith(".manifest.json") and len(calls) == 2:
            raise OSError("power cut")
        real(path, data)
    monkeypatch.setattr(emit, "_write_atomic", fail_on_first_manifest)
    with pytest.raises(OSError):
        emit.emit(f"dbname={dbname}", tmp_path)
    assert conn.execute("SELECT count(*) FROM emitted_hours").fetchone()[0] == 0
    assert not list(tmp_path.rglob("*.manifest.json")) and not list(tmp_path.rglob("*.tmp"))
    monkeypatch.setattr(emit, "_write_atomic", real)
    assert emit.emit(f"dbname={dbname}", tmp_path) == 3
