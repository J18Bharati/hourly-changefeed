"""M2: the board registry: sample tokens from a published list, validate them, export the result."""
import gzip
import json
from pathlib import Path

import httpx
import pytest

from changefeed import cli, registry
from changefeed.fetch import FetchError, Response

FIXTURES = Path(__file__).parent / "fixtures"


def test_sampling_is_deterministic():
    tokens = [f"t{i}" for i in range(1000)]
    a = registry.sample(tokens, 50, seed=7)
    assert a == registry.sample(list(reversed(tokens)), 50, seed=7), "input order does not matter"
    assert a != registry.sample(tokens, 50, seed=8)
    assert len(a) == 50 and a == sorted(a)
    assert registry.sample(tokens[:10], 50, seed=7) == sorted(tokens[:10])


def test_import_is_idempotent_and_skips_bad_tokens(conn):
    assert registry.import_tokens(conn, "lever", ["acme", "beta", "../x", "has space"]) == (2, 2)
    assert registry.import_tokens(conn, "lever", ["acme", "beta"]) == (0, 0)
    assert conn.execute("SELECT count(*), min(status) FROM boards").fetchone() == (2, "candidate")


def fake_fetch(replies):
    def _fetch(url, **_):
        reply = replies[url]
        if isinstance(reply, Exception):
            raise reply
        status, body = reply
        return Response(status, httpx.Headers(), body, url, None)
    return _fetch


def gz(name):
    return gzip.decompress((FIXTURES / name).read_bytes())


def test_validate_keeps_boards_with_postings(conn):
    registry.import_tokens(conn, "greenhouse", ["discord", "gone", "broken"])
    registry.import_tokens(conn, "lever", ["empty", "down"])
    gh = "https://boards-api.greenhouse.io/v1/boards/{}/jobs?content=true"
    lv = "https://api.lever.co/v0/postings/{}?mode=json"
    fetch = fake_fetch({
        gh.format("discord"): (200, gz("greenhouse-discord.json.gz")),
        gh.format("gone"): (404, (FIXTURES / "greenhouse-404.json").read_bytes()),
        gh.format("broken"): (200, b'{"status":404,"error":"Job not found"}'),
        lv.format("empty"): (200, b"[]"),
        lv.format("down"): FetchError("ConnectError: boom"),
    })
    counts = registry.validate(conn, fetch=fetch, workers=2)
    assert counts == {"active": 1, "rejected": 4}
    rows = {t: (s, v) for t, s, v in conn.execute("SELECT token, status, validation FROM boards")}
    assert rows["discord"][0] == "active" and rows["discord"][1]["postings"] == 41
    assert rows["discord"][1]["bytes"] == len(gz("greenhouse-discord.json.gz"))
    assert rows["gone"][1]["error"] == "HTTP 404"
    assert rows["broken"][1]["error"].startswith("parse:")
    assert rows["empty"][1]["error"] == "no postings"
    assert rows["down"][1]["error"].startswith("fetch:")
    assert conn.execute("SELECT count(*) FROM boards WHERE validated_at IS NULL").fetchone()[0] == 0
    assert registry.validate(conn, fetch=fake_fetch({})) == {}, "validated boards are not fetched again"


def test_export_and_load_round_trip(conn, connect):
    registry.import_tokens(conn, "ashby", ["linear"])
    conn.execute("""UPDATE boards SET status = 'active', validated_at = now(),
                    validation = '{"postings": 31, "bytes": 412718}'""")
    tsv = registry.export(conn)
    assert tsv.splitlines() == ["source\ttoken\tpostings\tbytes", "ashby\tlinear\t31\t412718"]
    other = connect()
    other.execute("DELETE FROM boards")
    assert registry.load(other, tsv) == 1
    assert registry.load(other, tsv) == 0
    assert other.execute("SELECT source, token, status FROM boards").fetchall() == [("ashby", "linear", "active")]


def test_seed_import_command_reads_the_published_list(dbname, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
    tokens = tmp_path / "lever_companies.json"
    tokens.write_text(json.dumps([f"co{i}" for i in range(100)]))
    assert cli.main(["seed", "import", "lever", str(tokens), "--sample", "10", "--seed", "1"]) == 0
    assert "imported 10" in capsys.readouterr().out
    assert cli.main(["seed", "import", "lever", str(tokens), "--sample", "10", "--seed", "1"]) == 0
    assert "imported 0" in capsys.readouterr().out
