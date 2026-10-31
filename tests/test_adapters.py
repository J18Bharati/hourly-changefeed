"""M1: contract tests for the three board adapters, against bodies recorded on 2026-10-31."""
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from changefeed.adapters import ADAPTERS, ShapeError, parse_board

FIXTURES = Path(__file__).parent / "fixtures"


def body(name: str) -> bytes:
    path = FIXTURES / name
    return gzip.decompress(path.read_bytes()) if name.endswith(".gz") else path.read_bytes()


RECORDED = [("greenhouse", "greenhouse-discord.json.gz", 41),
            ("lever", "lever-outreach-limit3.json.gz", 3),
            ("ashby", "ashby-linear.json.gz", 31)]


@pytest.mark.parametrize("source, fixture, count", RECORDED)
def test_recorded_boards_parse(source, fixture, count):
    board = parse_board(source, body(fixture))
    ids = [p.external_id for p in board.postings]
    assert len(ids) == count and ids == sorted(ids) and len(set(ids)) == count
    for p in board.postings:
        assert p.fields["title"] and p.fields["url"].startswith("https://")
        assert "updated_at" not in p.fields, "the source's own update stamp is never a versioned field"
        json.dumps(p.fields)  # plain JSON values only
        assert p.source_ts is None or p.source_ts.tzinfo is not None


@pytest.mark.parametrize("source, fixture, count", RECORDED)
def test_parsing_is_deterministic_and_order_free(source, fixture, count):
    raw = json.loads(body(fixture))
    jobs = raw["jobs"] if isinstance(raw, dict) else raw
    jobs.reverse()
    assert parse_board(source, json.dumps(raw).encode()) == parse_board(source, body(fixture))


def test_greenhouse_content_is_unescaped_and_squashed():
    p = parse_board("greenhouse", body("greenhouse-discord.json.gz")).postings[0]
    assert "&lt;" not in p.fields["description"] and "<" in p.fields["description"]
    assert "  " not in p.fields["description"] and "\n" not in p.fields["description"]


def test_greenhouse_source_time_is_updated_at_in_utc():
    board = parse_board("greenhouse", body("greenhouse-discord.json.gz"))
    raw = {str(j["id"]): j["updated_at"] for j in json.loads(body("greenhouse-discord.json.gz"))["jobs"]}
    p = board.postings[0]
    assert p.source_ts == datetime.fromisoformat(raw[p.external_id]).astimezone(timezone.utc)
    assert board.newest_source_ts == max(q.source_ts for q in board.postings)


def test_lever_created_at_milliseconds():
    p = parse_board("lever", body("lever-outreach-limit3.json.gz")).postings[0]
    raw = {j["id"]: j["createdAt"] for j in json.loads(body("lever-outreach-limit3.json.gz"))}
    assert p.source_ts == datetime.fromtimestamp(raw[p.external_id] / 1000, timezone.utc)


def test_an_explicit_empty_list_is_an_empty_board():
    assert parse_board("lever", body("lever-mistral-empty.json.gz")).postings == []
    assert parse_board("greenhouse", b'{"jobs": [], "meta": {"total": 0}}').postings == []
    assert parse_board("ashby", b'{"jobs": [], "apiVersion": "1"}').postings == []


@pytest.mark.parametrize("source, raw", [
    ("greenhouse", body("greenhouse-404.json")),             # an error body, as if served with a 200
    ("lever", body("lever-404.json")),
    ("ashby", body("ashby-404.txt")),                        # not JSON at all
    ("greenhouse", body("greenhouse-discord.json.gz")[:5000]),  # truncated
    ("ashby", body("ashby-linear.json.gz")[:-3]),
    ("greenhouse", b'{"meta": {"total": 0}}'),               # the list is missing, not empty
    ("lever", b'{"data": []}'),
    ("ashby", b'{"jobs": null}'),
    ("greenhouse", b'{"jobs": [{"id": 1, "title": "x", "absolute_url": "https://e"}], "meta": {"total": 2}}'),
    ("lever", b'[{"text": "no id", "hostedUrl": "https://e"}]'),
    ("ashby", b'{"jobs": [{"id": "a", "jobUrl": "https://e"}]}'),  # no title
])
def test_wrong_shapes_are_parse_errors(source, raw):
    with pytest.raises(ShapeError):
        parse_board(source, raw)


def test_a_repeated_id_keeps_the_first_copy():
    raw = (b'{"jobs": [{"id": 1, "title": "first", "absolute_url": "https://e/1"},'
           b' {"id": 1, "title": "second", "absolute_url": "https://e/1"}], "meta": {"total": 2}}')
    board = parse_board("greenhouse", raw)
    assert [p.fields["title"] for p in board.postings] == ["first"]
    assert board.duplicate_ids == 1


def test_unlisted_ashby_jobs_are_left_out():
    raw = (b'{"jobs": [{"id": "a", "title": "A", "jobUrl": "https://e/a", "isListed": true},'
           b' {"id": "b", "title": "B", "jobUrl": "https://e/b", "isListed": false}]}')
    assert [p.external_id for p in parse_board("ashby", raw).postings] == ["a"]


def test_every_source_has_a_board_url():
    assert ADAPTERS["greenhouse"].url("acme") == "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true"
    assert ADAPTERS["lever"].url("acme") == "https://api.lever.co/v0/postings/acme?mode=json"
    assert ADAPTERS["ashby"].url("acme") == "https://api.ashbyhq.com/posting-api/job-board/acme"
    with pytest.raises(ValueError):
        ADAPTERS["lever"].url("../admin")
