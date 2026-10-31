"""Part B M8: the consumer answers as_of(valid, known) from change files alone, and its answers match
the producer's for every history."""
import random
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from changefeed import emit, load
from test_load import consumer  # noqa: F401  (fixture)
from test_versions import body, random_history, repeat


@pytest.fixture
def bid(conn):
    return conn.execute("INSERT INTO boards (source, token, status) VALUES ('ashby', 'acme', 'active') RETURNING id").fetchone()[0]


def as_of(conninfo_or_conn, valid, known, consumer_side):
    query = ("SELECT p.external_id, a.fields FROM postings_as_of(%s, %s) a JOIN postings p ON p.id = a.posting_id"
             if consumer_side else
             "SELECT p.external_id, a.fields FROM postings_as_of(%s, %s) a JOIN postings_current p ON p.id = a.posting_id")
    if isinstance(conninfo_or_conn, str):
        with psycopg.connect(conninfo_or_conn) as c:
            return dict(c.execute(query, (valid, known)).fetchall())
    return dict(conninfo_or_conn.execute(query, (valid, known)).fetchall())


@pytest.mark.parametrize("seed", range(15))
def test_replaying_the_change_files_gives_the_producers_answers(conn, dbname, consumer, bid, seed, tmp_path):
    rng = random.Random(5000 + seed)
    # A history that ended at least an hour ago, so every hour of it is emitted.
    now = datetime.now(timezone.utc)
    t0 = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=16)
    conn.execute("INSERT INTO collection_runs (hour, started_at, finished_at, status) VALUES (%s, %s, %s, 'ok')",
                 (t0, t0, t0))
    times = []
    with pytest.MonkeyPatch.context() as mp:
        import test_versions
        mp.setattr(test_versions, "T0", t0)
        for at, postings in random_history(rng, 12):
            repeat(conn, bid, at) if postings is None else body(conn, bid, at, *postings)
            times.append(at)
    emit.emit(f"dbname={dbname}", tmp_path)
    assert load.load(consumer, tmp_path) >= 13
    probes = [(t, t) for t in times] + [(t, now) for t in times] + \
             [(t0 + timedelta(minutes=rng.randrange(14 * 60)), t0 + timedelta(minutes=rng.randrange(14 * 60))) for _ in range(20)]
    seen = 0
    for valid, known in probes:
        theirs = as_of(conn, valid, known, False)
        assert as_of(consumer, valid, known, True) == theirs, (seed, valid, known)
        seen += len(theirs)
    assert seen > 0, "the probes saw open postings"
