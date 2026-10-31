"""Part B M9: fill rate per board and field, freshness per board, and missed hours, on the consumer."""
import json
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from changefeed import cli, reports
from test_load import consumer  # noqa: F401  (fixture)

T = datetime(2026, 10, 30, 12, tzinfo=timezone.utc)
H = timedelta(hours=1)
M = timedelta(minutes=1)


@pytest.fixture
def c(consumer):
    with psycopg.connect(consumer, autocommit=True) as conn:
        for i, missed in enumerate([None, "no run", None]):
            conn.execute("""INSERT INTO loaded_files (hour, file, rows, sha256, manifest_sha256, prev_manifest_sha256, runs, missed)
                            VALUES (%s, 'x', 0, '', %s, %s, %s, %s)""",
                         (T + i * H, f"m{i}", f"m{i - 1}" if i else None, 0 if missed else 1, missed))
        yield conn


def posting(c, eid, board="acme", source="greenhouse"):
    return c.execute("INSERT INTO postings (source, board, external_id) VALUES (%s, %s, %s) RETURNING id",
                     (source, board, eid)).fetchone()[0]


def version(c, pid, field, value, recorded, lag=timedelta(0), why="observed", baseline=False):
    c.execute("""INSERT INTO field_versions (posting_id, field, valid_from, value, valid_from_source, recorded_at,
                                             baseline, hour)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
              (pid, field, recorded - lag, json.dumps(value), why, recorded, baseline, T))


def test_fill_rate_counts_open_postings_and_treats_empty_as_missing(c):
    for eid, team in [("1", "Eng"), ("2", ""), ("3", None), ("4", "Ops")]:
        p = posting(c, eid)
        version(c, p, "present", True, T)
        version(c, p, "title", "T", T)
        if team is not None:
            version(c, p, "team", team, T)
    version(c, 4, "present", False, T + 10 * M)  # posting 4 closed: not counted
    assert reports.fill_rates(c, at=T + H) == [
        ("greenhouse", "acme", "team", 1, 3),
        ("greenhouse", "acme", "title", 3, 3)]


def test_freshness_measures_source_lag_on_real_edits_only(c):
    p = posting(c, "1")
    version(c, p, "present", True, T, lag=400 * H, why="source", baseline=True)  # creation time: not lag
    for i, lag in enumerate([10 * M, 30 * M, 2 * H]):
        version(c, p, "title", f"v{i}", T + (i + 1) * 3 * H, lag=lag, why="source")
    version(c, p, "team", "x", T + 20 * H)  # 'observed': lag 0 by construction, so not a measurement
    (row,) = reports.freshness(c)
    assert row[:3] == ("greenhouse", "acme", 3)
    assert row[3] == pytest.approx(30 * 60) and row[4] == pytest.approx(1800 + 0.9 * 5400)
    assert row[5] == T + 20 * H, "last change recorded"


def test_missed_hours_come_from_the_ledger(c):
    assert reports.missed_hours(c) == (3, [(T + H, "no run")])


def test_report_command(c, consumer, monkeypatch, capsys):
    p = posting(c, "1")
    version(c, p, "present", True, T)
    version(c, p, "title", "T", T)
    monkeypatch.setenv("CHANGEFEED_CONSUMER_DB", consumer)
    assert cli.main(["report"]) == 0
    out = capsys.readouterr().out
    assert "greenhouse/acme title 1/1 (100%)" in out
    assert "missed 1 of 3 hours" in out
