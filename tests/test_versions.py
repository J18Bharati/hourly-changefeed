"""M4: the versioning engine. Valid time vs recorded time, presence as a field, as_of."""
import random
from datetime import datetime, timedelta, timezone

import pytest

from changefeed import versions as V
from changefeed.adapters import Board, Posting

T0 = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)


@pytest.fixture
def bid(conn):
    return conn.execute("INSERT INTO boards (source, token, status) VALUES ('greenhouse', 'acme', 'active') RETURNING id").fetchone()[0]


def post(eid, title="T", ts=None, **more):
    return Posting(str(eid), {"title": title, "url": f"https://e/{eid}", **more}, ts)


def body(conn, bid, at, *postings):
    with conn.transaction():
        return V.apply_body(conn, bid, Board(sorted(postings, key=lambda p: p.external_id)), recorded_at=at)


def repeat(conn, bid, at):
    with conn.transaction():
        return V.apply_repeat(conn, bid, recorded_at=at)


def history(conn, field=None):
    return conn.execute("""
        SELECT p.external_id, v.field, v.value, v.valid_from, v.valid_from_source, v.recorded_at
        FROM posting_field_versions v JOIN postings_current p ON p.id = v.posting_id
        WHERE %(f)s::text IS NULL OR v.field = %(f)s ORDER BY v.id""", {"f": field}).fetchall()


def as_of(conn, valid, known):
    return {eid: f for eid, f in conn.execute("""
        SELECT p.external_id, a.fields FROM postings_as_of(%s, %s) a JOIN postings_current p ON p.id = a.posting_id""",
        (valid, known))}


# --- first sight, edits, valid time ---------------------------------------------------------

def test_first_sight_versions_every_field_from_the_source_time(conn, bid):
    created = T0 - 30 * H
    assert body(conn, bid, T0, post(1, "Eng", created, team=None)) == 3  # present, title, url; not the null team
    assert history(conn) == [
        ("1", "present", True, created, "source", T0),
        ("1", "title", "Eng", created, "source", T0),
        ("1", "url", "https://e/1", created, "source", T0)]


def test_an_unchanged_body_writes_nothing(conn, bid):
    body(conn, bid, T0, post(1))
    assert body(conn, bid, T0 + H, post(1)) == 0
    assert conn.execute("SELECT last_seen_at FROM postings_current").fetchone()[0] == T0 + H


def test_an_edit_takes_its_valid_time_from_a_moved_source_stamp(conn, bid):
    body(conn, bid, T0, post(1, "Eng", T0 - 5 * H))
    assert body(conn, bid, T0 + 2 * H, post(1, "Senior Eng", T0 + H)) == 1
    assert history(conn, "title")[-1] == ("1", "title", "Senior Eng", T0 + H, "source", T0 + 2 * H)


def test_an_edit_without_a_moved_stamp_is_valid_from_when_we_saw_it(conn, bid):
    # Lever and Ashby give only a creation time, which does not move on edits.
    body(conn, bid, T0, post(1, "Eng", T0 - 5 * H))
    body(conn, bid, T0 + H, post(1, "Senior Eng", T0 - 5 * H))
    assert history(conn, "title")[-1][3:5] == (T0 + H, "observed")


def test_a_source_stamp_in_the_future_is_clamped(conn, bid):
    body(conn, bid, T0, post(1, "Eng", T0 + 48 * H))
    assert history(conn, "title")[0][3:5] == (T0, "clamped")


def test_a_flapping_field_is_three_versions(conn, bid):
    for i, title in enumerate(["A", "B", "A"]):
        body(conn, bid, T0 + i * H, post(1, title))
    assert [v[2] for v in history(conn, "title")] == ["A", "B", "A"]


# --- presence and tombstones ------------------------------------------------------------------

def test_a_tombstone_needs_two_agreeing_fetches_and_dates_from_the_first(conn, bid):
    body(conn, bid, T0, post(1), post(2))
    assert body(conn, bid, T0 + H, post(1)) == 0, "absent once: could be a bad replica"
    assert body(conn, bid, T0 + 2 * H, post(1)) == 1
    assert history(conn, "present")[-1] == ("2", "present", False, T0 + H, "observed", T0 + 2 * H)


def test_absent_once_then_back_writes_nothing(conn, bid):
    body(conn, bid, T0, post(1), post(2))
    body(conn, bid, T0 + H, post(1))
    assert body(conn, bid, T0 + 2 * H, post(1), post(2)) == 0
    assert conn.execute("SELECT absent_count, absent_since FROM postings_current WHERE external_id = '2'").fetchone() == (0, None)


def test_a_304_or_identical_body_counts_as_agreeing(conn, bid):
    # Otherwise a posting that closes on a quiet board is never tombstoned: nothing else changes.
    body(conn, bid, T0, post(1), post(2))
    body(conn, bid, T0 + H, post(1))
    assert repeat(conn, bid, T0 + 2 * H) == 1
    assert history(conn, "present")[-1][:3] == ("2", "present", False)


def test_a_reappearance_is_a_present_version(conn, bid):
    body(conn, bid, T0, post(1), post(2))
    body(conn, bid, T0 + H, post(1))
    body(conn, bid, T0 + 2 * H, post(1))
    assert body(conn, bid, T0 + 3 * H, post(1), post(2)) == 1
    assert [v[2] for v in history(conn, "present") if v[0] == "2"] == [True, False, True]


def test_a_board_going_empty_needs_three_agreeing_fetches(conn, bid):
    body(conn, bid, T0, post(1))
    assert body(conn, bid, T0 + H) == 0
    assert body(conn, bid, T0 + 2 * H) == 0, "an empty board is the classic error page; wait longer"
    assert repeat(conn, bid, T0 + 3 * H) == 1
    assert history(conn, "present")[-1][2:4] == (False, T0 + H)


# --- as_of ---------------------------------------------------------------------------------------

def test_as_of_separates_what_was_true_from_what_was_known(conn, bid):
    body(conn, bid, T0, post(1, "Eng", T0 - H), post(2, "Ops", T0 - H))
    body(conn, bid, T0 + H, post(1, "Eng", T0 - H))          # 2 absent from T0+1h
    body(conn, bid, T0 + 2 * H, post(1, "Eng", T0 - H))      # confirmed at T0+2h
    assert set(as_of(conn, T0 + 90 * timedelta(minutes=1), T0 + 90 * timedelta(minutes=1))) == {"1", "2"}, \
        "at 01:30 we still believed 2 was open"
    assert set(as_of(conn, T0 + 90 * timedelta(minutes=1), T0 + 3 * H)) == {"1"}, \
        "knowing what we know now, it closed at 01:00: the look-ahead a backtest must not use"


def random_history(rng, n_fetches):
    """A random sequence of board states: postings come and go, fields change, stamps move or don't."""
    open_ = {}
    for i in range(n_fetches):
        at = T0 + i * H + timedelta(minutes=rng.randrange(60))
        kind = rng.random()
        if kind < 0.15:
            yield at, None  # a 304
            continue
        for eid in range(8):
            r = rng.random()
            if eid in open_ and r < 0.15:
                del open_[eid]
            elif eid not in open_ and r < 0.3:
                open_[eid] = post(eid, rng.choice("ABC"), at - timedelta(minutes=rng.randrange(600)))
            elif eid in open_ and r < 0.5:
                old = open_[eid]
                moved = at - timedelta(minutes=rng.randrange(30)) if rng.random() < 0.5 else old.source_ts
                open_[eid] = post(eid, rng.choice("ABC"), moved, team=rng.choice([None, "x", "y"]))
        yield at, list(open_.values())


@pytest.mark.parametrize("seed", range(25))
def test_as_of_then_equals_the_state_we_held_then(conn, bid, seed):
    """Property: as_of(t, t) at each fetch time t is exactly postings_current right after that fetch."""
    rng = random.Random(seed)
    for at, postings in random_history(rng, 12):
        if postings is None:
            repeat(conn, bid, at)
        else:
            body(conn, bid, at, *postings)
        held = {eid: f for eid, f in conn.execute(
            "SELECT external_id, fields FROM postings_current WHERE present AND board_id = %s", (bid,))}
        assert strip(as_of(conn, at, at)) == strip(held), f"seed {seed} at {at}"


def strip(states):
    """A field that is null and a field that is missing say the same thing."""
    return {eid: {k: v for k, v in f.items() if v is not None} for eid, f in states.items()}


@pytest.mark.parametrize("seed", range(25))
def test_valid_time_only_moves_forward_and_never_passes_recorded_time(conn, bid, seed):
    rng = random.Random(1000 + seed)
    for at, postings in random_history(rng, 12):
        repeat(conn, bid, at) if postings is None else body(conn, bid, at, *postings)
    bad = conn.execute("""
        SELECT posting_id, field FROM (
            SELECT posting_id, field, valid_from, recorded_at,
                   lag(valid_from) OVER (PARTITION BY posting_id, field ORDER BY id) AS prev
            FROM posting_field_versions) v
        WHERE valid_from <= prev OR valid_from > recorded_at""").fetchall()
    assert bad == []
