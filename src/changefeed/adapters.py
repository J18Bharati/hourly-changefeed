"""Board adapters: one public ATS API response in, a list of normalised postings out.

Each adapter checks the top-level shape it expects and raises ShapeError when it is missing, so an
error page served with a 200, or a truncated body, can never read as "every posting was closed".
Only an explicitly present empty list is an empty board.

Field values are plain JSON (str, int, bool, list, dict, None) after normalisation v1: strings have
whitespace runs collapsed, HTML is unescaped where the source double-escapes it, times are UTC ISO
strings, and postings are sorted by external id. The source's own "updated" stamp is never a field:
it moves without the posting changing. It is kept as `source_ts`, for valid time and staleness only.
"""
import html
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import quote

NORMALIZER_VERSION = 1


class ShapeError(ValueError):
    """The body is not the shape this source's API returns for a board."""


@dataclass(frozen=True)
class Posting:
    external_id: str
    fields: dict
    source_ts: datetime | None = None  # newest time the source says this posting changed, if it says


@dataclass(frozen=True)
class Board:
    postings: list[Posting]
    duplicate_ids: int = 0

    @property
    def newest_source_ts(self) -> datetime | None:
        return max((p.source_ts for p in self.postings if p.source_ts), default=None)


@dataclass(frozen=True)
class Adapter:
    url_template: str
    parse: Callable[[object], list[Posting]] = field(repr=False)

    def url(self, token: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", token) or ".." in token:
            raise ValueError(f"bad board token {token!r}")
        return self.url_template.format(quote(token, safe=""))


# --- value normalisation -----------------------------------------------------------

def _text(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ShapeError(f"expected a string, got {type(value).__name__}")
    return " ".join(value.split()) or None


def _names(items, key="name") -> list[str]:
    return sorted({n for item in items or [] if (n := _text(item.get(key) if isinstance(item, dict) else item))})


def _iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        when = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise ShapeError(f"unparseable timestamp {value!r}") from None
    return (when if when.tzinfo else when.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def _millis(value) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ShapeError(f"expected epoch milliseconds, got {value!r}")
    return datetime.fromtimestamp(value / 1000, timezone.utc)


def _stamp(when: datetime | None) -> str | None:
    return when.isoformat() if when else None


def _require(job: dict, *keys) -> None:
    missing = [k for k in keys if job.get(k) in (None, "")]
    if missing:
        raise ShapeError(f"posting without {', '.join(missing)}: {str(job)[:120]}")


def _jobs(data, key: str | None) -> list:
    jobs = data if key is None else (data.get(key) if isinstance(data, dict) else None)
    if not isinstance(jobs, list) or not all(isinstance(j, dict) for j in jobs):
        where = "a top-level list" if key is None else f'a "{key}" list'
        raise ShapeError(f"expected {where} of postings, got {str(data)[:120]}")
    return jobs


# --- the three sources -----------------------------------------------------------------

def _greenhouse(data) -> list[Posting]:
    jobs = _jobs(data, "jobs")
    total = (data.get("meta") or {}).get("total")
    if total is not None and total != len(jobs):
        raise ShapeError(f"meta.total is {total} but the body lists {len(jobs)} jobs")
    out = []
    for j in jobs:
        _require(j, "id", "title", "absolute_url")
        content = j.get("content")
        out.append(Posting(str(j["id"]), {
            "title": _text(j["title"]),
            "url": _text(j["absolute_url"]),
            "location": _text((j.get("location") or {}).get("name")),
            "departments": _names(j.get("departments")),
            "offices": _names(j.get("offices")),
            "requisition_id": _text(j.get("requisition_id")),
            "published_at": _stamp(_iso(j.get("first_published"))),
            "description": _text(html.unescape(content)) if isinstance(content, str) else _text(content),
        }, _iso(j.get("updated_at"))))
    return out


def _lever(data) -> list[Posting]:
    out = []
    for j in _jobs(data, None):
        _require(j, "id", "text", "hostedUrl")
        cats = j.get("categories") or {}
        lists = "".join(f"<h3>{x.get('text', '')}</h3><ul>{x.get('content', '')}</ul>" for x in j.get("lists") or [])
        created = _millis(j.get("createdAt"))
        out.append(Posting(str(j["id"]), {
            "title": _text(j["text"]),
            "url": _text(j["hostedUrl"]),
            "location": _text(cats.get("location")),
            "locations": _names(cats.get("allLocations")),
            "department": _text(cats.get("department")),
            "team": _text(cats.get("team")),
            "employment_type": _text(cats.get("commitment")),
            "workplace_type": _text(j.get("workplaceType")),
            "salary": j.get("salaryRange"),
            "published_at": _stamp(created),
            "description": _text((j.get("description") or "") + lists + (j.get("additional") or "")),
        }, created))
    return out


def _ashby(data) -> list[Posting]:
    out = []
    for j in _jobs(data, "jobs"):
        _require(j, "id", "title", "jobUrl")
        if j.get("isListed") is False:
            continue  # not on the public board page; the API may still return it
        published = _iso(j.get("publishedAt"))
        out.append(Posting(str(j["id"]), {
            "title": _text(j["title"]),
            "url": _text(j["jobUrl"]),
            "location": _text(j.get("location")),
            "locations": _names(j.get("secondaryLocations"), key="location"),
            "department": _text(j.get("department")),
            "team": _text(j.get("team")),
            "employment_type": _text(j.get("employmentType")),
            "workplace_type": _text(j.get("workplaceType")),
            "remote": j.get("isRemote"),
            "published_at": _stamp(published),
            "description": _text(j.get("descriptionHtml")),
        }, published))
    return out


ADAPTERS = {
    "greenhouse": Adapter("https://boards-api.greenhouse.io/v1/boards/{}/jobs?content=true", _greenhouse),
    "lever": Adapter("https://api.lever.co/v0/postings/{}?mode=json", _lever),
    "ashby": Adapter("https://api.ashbyhq.com/posting-api/job-board/{}", _ashby),
}


def parse_board(source: str, body: bytes) -> Board:
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError) as e:  # JSONDecodeError is a ValueError
        raise ShapeError(f"not JSON: {e}") from None
    seen, postings, dupes = set(), [], 0
    for p in ADAPTERS[source].parse(data):
        if p.external_id in seen:
            dupes += 1  # keep the first copy of a repeated id
            continue
        seen.add(p.external_id)
        postings.append(p)
    return Board(sorted(postings, key=lambda p: p.external_id), duplicate_ids=dupes)
