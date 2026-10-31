"""The fetcher copied from gator, with the public-address check swapped for a host allowlist."""
import gzip

import pytest

from conftest import Reply
from changefeed import fetch as fetch_mod
from changefeed.fetch import FetchRefused, fetch


def test_only_the_three_apis_are_allowed(monkeypatch):
    monkeypatch.delenv("CHANGEFEED_EXTRA_HOSTS", raising=False)
    for host in ("boards-api.greenhouse.io", "api.lever.co", "API.ASHBYHQ.COM"):
        assert fetch_mod.host_allowed(host)
    for host in ("example.com", "127.0.0.1", "api.lever.co.evil.example", "localhost"):
        assert not fetch_mod.host_allowed(host)


def test_a_test_server_needs_an_explicit_extra_host(server, monkeypatch):
    server.routes["/b"] = Reply(200, b"[]")
    monkeypatch.delenv("CHANGEFEED_EXTRA_HOSTS", raising=False)
    with pytest.raises(FetchRefused, match="allowlist"):
        fetch(server.url("/b"))
    monkeypatch.setenv("CHANGEFEED_EXTRA_HOSTS", "127.0.0.1")
    assert fetch(server.url("/b")).body == b"[]"


def test_a_redirect_off_the_allowlist_is_refused(server, monkeypatch):
    monkeypatch.setenv("CHANGEFEED_EXTRA_HOSTS", "127.0.0.1")
    server.routes["/b"] = Reply(302, b"", {"Location": "https://example.com/elsewhere"})
    with pytest.raises(FetchRefused, match="allowlist"):
        fetch(server.url("/b"))


def test_decoded_size_is_capped(server, monkeypatch):
    monkeypatch.setenv("CHANGEFEED_EXTRA_HOSTS", "127.0.0.1")
    monkeypatch.setattr(fetch_mod, "MAX_BYTES", 10_000)
    server.routes["/b"] = Reply(200, gzip.compress(b" " * 1_000_000), {"Content-Encoding": "gzip"})
    with pytest.raises(FetchRefused, match="larger than"):
        fetch(server.url("/b"))
