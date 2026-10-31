"""Hourly change files: everything recorded in hour H, as JSONL, plus a manifest.

Layout: <out>/YYYY/MM/DD/HH.jsonl and HH.manifest.json, hours in UTC. Every finished hour gets
both, even with nothing in it (an empty file) or no run at all (declared in the manifest), so a
consumer can tell "nothing changed" from "we weren't looking". Each manifest carries the sha256
of the previous one, so the files form a chain a loader can check.

The bytes depend only on the stored versions: rows are sorted by content, not by row id, and the
manifest has no wall-clock fields. Emitting an hour twice gives the same bytes.

The watermark: hour H is written only under the run lock, once H+1 has begun and no run that
started before H+1 is still open. A run still 'running' an hour after it started has crashed
(the unit's TimeoutStartSec is 50 minutes) and is closed as 'partial'.
"""
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

from changefeed.collect import RUN_LOCK

UTC = timezone.utc
HOUR = timedelta(hours=1)
FORMAT = 1


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)  # the rename itself is durable only once the directory is
    finally:
        os.close(fd)


def _stamp(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def _dumps(obj) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def _rows(conn, hour: datetime) -> bytes:
    rows = conn.execute("""
        SELECT b.source, b.token, p.external_id, v.field, v.value, v.valid_from, v.valid_from_source,
               v.recorded_at, v.run_id, coalesce(l.baseline, false)
        FROM posting_field_versions v
        JOIN postings_current p ON p.id = v.posting_id
        JOIN boards b ON b.id = p.board_id
        LEFT JOIN fetch_log l ON l.id = v.fetch_log_id
        WHERE v.recorded_at >= %s AND v.recorded_at < %s
        ORDER BY v.recorded_at, b.source, b.token, p.external_id, v.field""", (hour, hour + HOUR)).fetchall()
    return b"".join(_dumps({
        "posting": {"source": src, "board": token, "external_id": eid},
        "field": field, "value": value, "valid_from": _stamp(vf), "valid_from_source": why,
        "recorded_at": _stamp(rec), "run_id": run_id, "baseline": baseline,
    }) for src, token, eid, field, value, vf, why, rec, run_id, baseline in rows), len(rows)


def _collection(conn, hour: datetime) -> dict:
    runs, missed = conn.execute("""
        SELECT count(*) FILTER (WHERE status <> 'missed'), min(reason) FILTER (WHERE status = 'missed')
        FROM collection_runs WHERE hour = %s""", (hour,)).fetchone()
    return {"runs": runs, "missed": None if runs else (missed or "no record")}


def emit(conninfo: str, out: Path) -> int:
    """Write every finished hour not yet written. Returns how many hours were written."""
    out = Path(out)
    written = 0
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (RUN_LOCK,))
        conn.execute("""UPDATE collection_runs SET status = 'partial', reason = 'did not finish', finished_at = now()
                        WHERE status = 'running' AND started_at < now() - interval '1 hour'""")
        limit, start, prev = conn.execute("""
            SELECT least(date_trunc('hour', now(), 'UTC'),
                         (SELECT date_trunc('hour', min(started_at), 'UTC') FROM collection_runs WHERE status = 'running')),
                   coalesce((SELECT max(hour) + interval '1 hour' FROM emitted_hours),
                            (SELECT min(hour) FROM collection_runs)),
                   (SELECT manifest_sha256 FROM emitted_hours ORDER BY hour DESC LIMIT 1)""").fetchone()
        hour = start
        while hour is not None and hour < limit:
            data, n = _rows(conn, hour)
            base = out / hour.astimezone(UTC).strftime("%Y/%m/%d/%H")
            manifest = _dumps({
                "format": FORMAT, "hour": _stamp(hour), "file": base.name + ".jsonl", "rows": n,
                "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                "collection": _collection(conn, hour), "prev_manifest_sha256": prev,
            })
            _write_atomic(base.with_name(base.name + ".jsonl"), data)
            _write_atomic(base.with_name(base.name + ".manifest.json"), manifest)  # present = complete
            prev = hashlib.sha256(manifest).hexdigest()
            conn.execute("INSERT INTO emitted_hours (hour, rows, sha256, manifest_sha256) VALUES (%s, %s, %s, %s)",
                         (hour, n, hashlib.sha256(data).hexdigest(), prev))
            written += 1
            hour += HOUR
    return written
