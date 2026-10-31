"""Part B M7: the consumer loads change files into its own database, checking every hash and the
chain, and never loads an hour twice."""
import json
import shutil
import uuid

import psycopg
import pytest

from conftest import _admin, _drop, _scratch_db
from changefeed import cli, emit, load
from test_emit import top, world  # noqa: F401  (fixtures)


@pytest.fixture
def consumer():
    name = _scratch_db("template0")
    with psycopg.connect(dbname=name, autocommit=True) as c:
        load.migrate(c)
    yield f"dbname={name}"
    _drop(name)


@pytest.fixture
def files(dbname, world, tmp_path):
    out = tmp_path / "changes"
    assert emit.emit(f"dbname={dbname}", out) == 3
    return out


def manifests(out):
    return sorted(out.rglob("*.manifest.json"))


def q(conninfo, sql, *args):
    with psycopg.connect(conninfo) as c:
        return c.execute(sql, args).fetchall()


def test_loading_reproduces_every_version_and_declares_the_missed_hour(consumer, files, conn):
    assert load.load(consumer, files) == 3
    assert q(consumer, "SELECT rows, runs, missed FROM loaded_files ORDER BY hour") == \
        [(2, 1, None), (0, 0, "no run"), (1, 1, None)]
    theirs = conn.execute("""SELECT field, value, valid_from, valid_from_source, recorded_at, run_id
                             FROM posting_field_versions ORDER BY field, valid_from""").fetchall()
    ours = q(consumer, """SELECT field, value, valid_from, valid_from_source, recorded_at, run_id
                          FROM field_versions ORDER BY field, valid_from""")
    assert ours == theirs
    assert q(consumer, "SELECT source, board, external_id FROM postings") == [("lever", "acme", "p1")]
    assert q(consumer, "SELECT baseline FROM field_versions ORDER BY recorded_at, field") == [(True,), (True,), (False,)]


def test_loading_again_is_a_no_op(consumer, files):
    load.load(consumer, files)
    before = q(consumer, "SELECT * FROM field_versions ORDER BY 1, 2, 3")
    assert load.load(consumer, files) == 0
    assert q(consumer, "SELECT * FROM field_versions ORDER BY 1, 2, 3") == before


def test_new_hours_load_on_top_of_old_ones(consumer, files, tmp_path):
    partial = tmp_path / "partial"
    for m in manifests(files)[:2]:
        for p in (m, m.with_name(m.name.replace(".manifest.json", ".jsonl"))):
            dest = partial / p.relative_to(files)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(p, dest)
    assert load.load(consumer, partial) == 2
    assert load.load(consumer, files) == 1


def test_a_changed_data_file_is_refused_and_nothing_of_it_is_loaded(consumer, files):
    last = manifests(files)[-1]
    data = last.with_name(last.name.replace(".manifest.json", ".jsonl"))
    data.write_bytes(data.read_bytes().replace(b"Senior Eng", b"Junior Eng"))
    with pytest.raises(load.LoadError, match="sha256"):
        load.load(consumer, files)
    assert q(consumer, "SELECT count(*) FROM loaded_files") == [(2,)], "the hours before it stay loaded"
    assert q(consumer, "SELECT count(*) FROM field_versions") == [(2,)]


def test_a_missing_hour_breaks_the_chain(consumer, files):
    middle = manifests(files)[1]
    middle.unlink()
    with pytest.raises(load.LoadError, match="chain"):
        load.load(consumer, files)
    assert q(consumer, "SELECT count(*) FROM loaded_files") == [(1,)]


def test_a_manifest_rewritten_to_match_its_data_still_breaks_the_chain(consumer, files):
    # Someone edits a data file and fixes up its manifest's hash: the next manifest's
    # prev_manifest_sha256 no longer matches.
    first = manifests(files)[0]
    m = json.loads(first.read_bytes())
    m["rows"] = 2
    first.write_bytes(emit._dumps({**m, "collection": {"runs": 1, "missed": "edited"}}))
    with pytest.raises(load.LoadError, match="chain"):
        load.load(consumer, files)


def test_an_hour_already_loaded_cannot_be_replaced(consumer, files):
    load.load(consumer, files)
    first = manifests(files)[0]
    m = json.loads(first.read_bytes())
    first.write_bytes(emit._dumps({**m, "collection": {"runs": 2, "missed": None}}))
    with pytest.raises(load.LoadError, match="already loaded"):
        load.load(consumer, files, verify=True)


def test_a_row_count_that_disagrees_with_the_file_is_refused(consumer, files):
    last = manifests(files)[-1]
    m = json.loads(last.read_bytes())
    last.write_bytes(emit._dumps({**m, "rows": 5}))
    with pytest.raises(load.LoadError, match="rows"):
        load.load(consumer, files)


def test_a_manifest_can_only_name_its_own_data_file(consumer, files, tmp_path):
    (tmp_path / "elsewhere.jsonl").write_bytes(b"")
    first = manifests(files)[0]
    m = json.loads(first.read_bytes())
    first.write_bytes(emit._dumps({**m, "file": "../../../../elsewhere.jsonl"}))
    with pytest.raises(load.LoadError, match="names"):
        load.load(consumer, files)


def test_the_loader_role_owns_the_consumer_and_cannot_reach_the_producer(dbname, files):
    role, cdb = f"cf_loader_{uuid.uuid4().hex[:8]}", f"changefeed_consumer_{uuid.uuid4().hex[:8]}"
    try:
        with _admin() as admin:
            load.provision(admin, consumer_db=cdb, producer_db=dbname, role=role)
            load.provision(admin, consumer_db=cdb, producer_db=dbname, role=role)  # idempotent
        with pytest.raises(psycopg.OperationalError, match="permission denied"):
            psycopg.connect(dbname=dbname, user=role).close()
        assert load.load(f"dbname={cdb} user={role}", files) == 3
    finally:
        _drop(cdb)
        with _admin() as admin:
            admin.execute(f"DROP ROLE IF EXISTS {role}")


def test_load_command(consumer, files, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_CONSUMER_DB", consumer)
    assert cli.main(["load", "--dir", str(files)]) == 0
    assert "loaded 3 hours" in capsys.readouterr().out
    files.joinpath(manifests(files)[0].relative_to(files)).write_bytes(b"{}")
    assert cli.main(["load", "--dir", str(files)]) == 0, "the hourly load does not re-read loaded hours"
    assert cli.main(["load", "--dir", str(files), "--verify"]) == 1
    assert "refused" in capsys.readouterr().err
