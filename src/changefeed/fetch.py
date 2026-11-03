"""HTTP fetching for board APIs.

Adapted from my Python rebuild of andrefetch/gator (https://github.com/andrefetch/gator, a Go
project). Changes: the public-address
check is replaced by a fixed host allowlist (we only ever talk to three APIs), MAX_BYTES is
64 MiB (the largest boards are several MB), and the User-Agent names this project.
"""
import os
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit

import httpx

USER_AGENT = "hourly-changefeed/0.1 (public job-board research; one request per board per hour)"
ALLOWED_HOSTS = {"boards-api.greenhouse.io", "api.lever.co", "api.ashbyhq.com"}
TIMEOUT = httpx.Timeout(10.0, read=60.0)
MAX_BYTES = 64 * 1024 * 1024
MAX_REDIRECTS = 5
DEADLINE = 120.0  # seconds for the whole fetch, redirects included; must stay below the claim lease
MAX_RETRY_AFTER = timedelta(hours=24)
REDIRECTS = {301, 302, 303, 307, 308}
PERMANENT = {301, 308}


class FetchError(Exception):
    """The request never produced an HTTP response."""


class FetchRefused(Exception):
    """We chose not to make the request or keep the response."""


@dataclass(frozen=True)
class Response:
    status: int
    headers: httpx.Headers
    body: bytes
    url: str                 # where the final response came from
    moved_to: str | None     # set when every redirect on the way was permanent


def host_allowed(host: str) -> bool:
    """Only the three board APIs. CHANGEFEED_EXTRA_HOSTS (a comma list) adds hosts, for tests."""
    extra = {h.strip().lower() for h in os.environ.get("CHANGEFEED_EXTRA_HOSTS", "").split(",") if h.strip()}
    return host.lower() in ALLOWED_HOSTS | extra


def _check(url: str) -> None:
    parts = urlsplit(url)
    parts.port  # raises ValueError for a non-numeric or out-of-range port
    if parts.scheme not in ("http", "https"):
        raise FetchRefused(f"refusing non-http(s) URL {url!r}")
    if not parts.hostname or not host_allowed(parts.hostname):
        raise FetchRefused(f"refusing host outside the allowlist in {url!r}")


class _Inflate:
    """One Content-Encoding layer, decoded with a hard limit on the output of every call.

    httpx's own decoder inflates a whole network chunk at once, so a 200 KB gzip of spaces
    became 80 MB in memory before a size cap could look at it (tests/test_security_fixes.py).
    """

    def __init__(self, encoding: str):
        codings = [c.strip().lower() for c in encoding.split(",") if c.strip()]
        codings = [c for c in codings if c != "identity"]
        if len(codings) > 1:
            raise FetchRefused(f"refusing stacked Content-Encoding {encoding!r}")
        self.coding = codings[0] if codings else None
        if self.coding not in (None, "gzip", "x-gzip", "deflate"):
            raise FetchRefused(f"refusing unsupported Content-Encoding {encoding!r}")
        # deflate is meant to be zlib-wrapped, but some servers send it raw: decide on the first bytes.
        self.z = zlib.decompressobj(16 + zlib.MAX_WBITS) if self.coding in ("gzip", "x-gzip") else None

    def feed(self, data: bytes, room: int) -> bytes:
        """Decode `data`, returning at most `room` bytes plus one, which is enough to tell it overflowed."""
        if self.coding is None:
            return data[:room + 1]
        if self.z is None:
            raw = len(data) < 2 or (data[0] & 0x0F) != 8 or ((data[0] << 8) | data[1]) % 31
            self.z = zlib.decompressobj(-zlib.MAX_WBITS if raw else zlib.MAX_WBITS)
        out = bytearray()
        try:
            while data and len(out) <= room:
                out += self.z.decompress(data, room + 1 - len(out))
                data = self.z.unconsumed_tail
        except zlib.error as e:
            raise FetchRefused(f"cannot decode {self.coding} body: {e}") from None
        return bytes(out)


def _read_capped(r: httpx.Response, deadline: float) -> bytes:
    inflate = _Inflate(r.headers.get("content-encoding", ""))
    body = bytearray()
    for raw in r.iter_raw():
        if time.monotonic() > deadline:
            raise FetchRefused(f"fetch took longer than {DEADLINE:g}s")
        body += inflate.feed(raw, MAX_BYTES - len(body))
        if len(body) > MAX_BYTES:
            raise FetchRefused(f"body larger than {MAX_BYTES} bytes after decoding")
    if body and inflate.z is not None and not inflate.z.eof:  # an empty body (a 304) has no stream to end
        raise FetchRefused(f"cannot decode {inflate.coding} body: truncated stream")
    return bytes(body)


def _timeout(deadline: float) -> httpx.Timeout:
    left = deadline - time.monotonic()
    if left <= 0:
        raise FetchRefused(f"fetch took longer than {DEADLINE:g}s")
    return httpx.Timeout(min(TIMEOUT.connect, left), read=min(TIMEOUT.read, left))


def fetch(url: str, *, etag: str | None = None, last_modified: str | None = None) -> Response:
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    current, all_permanent = url, True
    deadline = time.monotonic() + DEADLINE
    try:
        with httpx.Client(timeout=TIMEOUT, follow_redirects=False, trust_env=False) as client:
            for hop in range(MAX_REDIRECTS + 1):
                _check(current)
                with client.stream("GET", current, headers=headers, timeout=_timeout(deadline)) as r:
                    location = r.headers.get("location")
                    if r.status_code in REDIRECTS and location:
                        all_permanent &= r.status_code in PERMANENT
                        current = urljoin(current, location)
                        continue
                    body = _read_capped(r, deadline)
                    moved = current if hop and all_permanent else None
                    return Response(r.status_code, r.headers, body, current, moved)
    except httpx.HTTPError as e:
        raise FetchError(f"{type(e).__name__}: {e}") from None
    except (httpx.InvalidURL, ValueError) as e:  # malformed redirect target, port, or IDNA label
        raise FetchRefused(f"refusing malformed URL {current!r}: {type(e).__name__}: {e}") from None
    raise FetchRefused(f"more than {MAX_REDIRECTS} redirects")


def parse_retry_after(value: str | None, *, now: datetime) -> datetime | None:
    """Retry-After as an absolute time: delta-seconds or an HTTP-date, clamped to [now, now + 24h]."""
    if not value:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():  # isdigit alone accepts digits like "\u00b2"
        return now + min(timedelta(seconds=int(value[:12])), MAX_RETRY_AFTER)
    when = http_date(value)
    return min(max(when, now), now + MAX_RETRY_AFTER) if when else None


def http_date(value: str | None) -> datetime | None:
    """An HTTP-date as an aware datetime (UTC if it names no zone), or None if it doesn't parse."""
    try:
        when = parsedate_to_datetime(value) if value else None
    except (TypeError, ValueError, OverflowError):
        return None
    if when and when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when
