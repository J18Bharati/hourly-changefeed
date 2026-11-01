"""M10: a look-ahead backtest. A hiring signal computed from today's records against the same
signal computed from what was known at the time, and the gap between them split into its causes:
postings that closed since (survivorship), closures learned late, and openings learned late."""
import json
import random
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from changefeed import backtest, cli
from test_load import consumer  # noqa: F401  (fixture)

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
D = timedelta(days=1)
H = timedelta(hours=1)


@pytest.fixture
def c(consumer):
    with psycopg.connect(consumer, autocommit=True) as conn:
        conn.execute("""INSERT INTO loaded_files (hour, file, rows, sha256, manifest_sha256, runs)
                        VALUES (%s, 'x', 0, '', 'm0', 1)""", (T0,))
        yield conn


def posting(c, eid, board="acme"):
    return c.execute("INSERT INTO postings (source, board, external_id) VALUES ('greenhouse', %s, %s) RETURNING id",
                     (board, eid)).fetchone()[0]


def put(c, pid, field, value, valid, recorded):
    c.execute("""INSERT INTO field_versions (posting_id, field, valid_from, value, valid_from_source, recorded_at,
                                             baseline, hour)
                 VALUES (%s, %s, %s, %s, 'source', %s, false, %s)""",
              (pid, field, valid, json.dumps(value), recorded, T0))


def opened(c, eid, valid, recorded=None, published=None, board="acme"):
    pid = posting(c, eid, board)
    put(c, pid, "present", True, valid, recorded or valid)
    put(c, pid, "published_at", (published or valid).isoformat(), valid, recorded or valid)
    return pid


def closed(c, pid, valid, recorded=None):
    put(c, pid, "present", False, valid, recorded or valid)


def test_the_gap_is_split_into_its_causes(c):
    a = opened(c, "A", T0)
    closed(c, a, T0 + 3 * D, T0 + 3 * D + 2 * H)         # open at t, closed since: survivorship
    opened(c, "B", T0 + 2 * D)                            # open throughout
    opened(c, "C", T0 + D, recorded=T0 + 5 * D)           # open at t, learned after t
    d = opened(c, "D", T0)
    closed(c, d, T0 + 2 * D, T0 + 3 * D)                  # closed before t, learned after t

    (p,) = backtest.points(c, at=T0 + 2.5 * D, window=2 * D, latest=T0 + 6 * D)
    assert (p.source, p.board, p.at) == ("greenhouse", "acme", T0 + 2.5 * D)
    assert (p.pit_open, p.hindsight_open, p.naive_open) == (3, 3, 2)   # {A,B,D}, {A,B,C}, {B,C}
    assert (p.survivorship, p.late_deletions, p.late_additions) == (1, 1, 1)  # A, D, C
    assert (p.pit_new, p.naive_new) == (1, 2)                           # {B}; {B,C}


def test_a_posting_without_a_published_date_is_counted_apart(c):
    pid = posting(c, "X")
    put(c, pid, "present", True, T0, T0)
    gone = posting(c, "Y")                     # undated too, but closed since: not open now
    put(c, gone, "present", True, T0, T0)
    closed(c, gone, T0 + 1.5 * D)
    (p,) = backtest.points(c, at=T0 + D, window=D, latest=T0 + 2 * D)
    assert (p.pit_open, p.naive_open, p.naive_undated) == (2, 0, 1)


def test_nothing_known_before_the_first_record_is_not_a_point(c):
    opened(c, "A", T0 - 30 * D, recorded=T0)  # a baseline: created long before we watched
    assert backtest.points(c, at=T0 - D, window=D, latest=T0 + D) == []


def _synthetic(c, seed, boards=30, days=42):
    """Seeded history: postings open, some close, some are first seen days after they opened, and
    closures are confirmed hours after the fact. Returns the truth to check against."""
    rng = random.Random(seed)
    truth = []
    for b in range(boards):
        rate = rng.uniform(0.2, 3.0)  # openings per day: boards differ in hiring speed
        for i in range(rng.randint(3, int(rate * days) + 3)):
            open_at = T0 + timedelta(hours=rng.randrange(days * 24))
            seen = open_at + (rng.randint(1, 5) * D if rng.random() < 0.15 else rng.randint(0, 2) * H)
            close_at = open_at + timedelta(hours=rng.expovariate(1 / (12 * 24)))
            pid = opened(c, f"{b}-{i}", open_at, recorded=seen, board=f"b{b}")
            confirmed = None
            if close_at < T0 + days * D:
                close_at = max(close_at, seen)
                confirmed = close_at + 2 * H
                closed(c, pid, close_at, confirmed)
            truth.append((f"b{b}", open_at, seen, close_at, confirmed))
    return truth


def _open(t, valid, known):
    """Whether truth row t was open at `valid` as known at `known`."""
    _, open_at, seen, close_at, confirmed = t
    closed_known = confirmed is not None and close_at <= valid and confirmed <= known
    return open_at <= valid and seen <= known and not closed_known


def test_survivorship_matches_the_synthetic_truth(c):
    truth = _synthetic(c, seed=7)
    at, latest = T0 + 20 * D, T0 + 42 * D
    got = {p.board: p for p in backtest.points(c, at=at, window=7 * D, latest=latest)}
    for board in {t[0] for t in truth}:
        rows = [t for t in truth if t[0] == board]
        open_then = [t for t in rows if _open(t, at, latest)]
        gone_now = [t for t in open_then if not _open(t, latest, latest)]
        known_then = [t for t in rows if _open(t, at, at)]
        p = got.get(board)
        assert (p.hindsight_open if p else 0) == len(open_then), board
        assert (p.survivorship if p else 0) == len(gone_now), board
        assert (p.pit_open if p else 0) == len(known_then), board


def test_the_summary_compares_both_signals_with_what_happened_next(c):
    _synthetic(c, seed=11)
    s = backtest.run(c, start=T0 + 7 * D, end=T0 + 42 * D, step=D, window=7 * D, horizon=7 * D,
                     latest=T0 + 42 * D)
    assert s.points > 0 and s.forward_pairs >= backtest.MIN_PAIRS
    assert s.survivorship > 0 and s.late_additions > 0
    # Today's records drop what closed since, so they undercount what was open.
    assert s.naive_open < s.hindsight_open
    assert s.corr_pit is not None and s.corr_naive is not None


def test_too_little_history_is_said_not_computed(c):
    opened(c, "A", T0)
    s = backtest.run(c, start=T0, end=T0 + D, step=D, window=D, horizon=7 * D, latest=T0 + D)
    assert s.forward_pairs == 0 and s.corr_pit is None and s.corr_naive is None
    assert "not enough history" in backtest.render(s)


def test_backtest_command(c, consumer, monkeypatch, capsys):
    _synthetic(c, seed=3, boards=5, days=21)
    monkeypatch.setenv("CHANGEFEED_CONSUMER_DB", consumer)
    assert cli.main(["backtest", "--window", "3", "--horizon", "3"]) == 0
    out = capsys.readouterr().out
    assert "survivorship" in out and "point-in-time" in out
