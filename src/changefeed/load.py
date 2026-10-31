"""The consumer side: load hourly change files into a separate database.

Each file is checked before anything is written, and loaded in one transaction:
- the manifest names its own data file (HH.jsonl next to HH.manifest.json) and nothing else;
- the data file's size, sha256 and line count match the manifest;
- the manifest's prev_manifest_sha256 is the last manifest loaded (None only for the first), so a
  missing, reordered or rewritten hour breaks the chain;
- an hour already in the ledger is skipped if its manifest is byte-identical, refused if not.
  The hourly load reads only manifests after the newest hour loaded; `verify` reads them all again.

Missed hours load as ledger rows with no versions, so they show up as gaps rather than quiet hours.
The loader needs nothing from the producer database: `provision` gives it a role that owns the
consumer database and cannot connect to the producer.
"""
import hashlib
import json
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql

from changefeed import db, emit

PACKAGE = "changefeed.consumer_migrations"
LOAD_LOCK = 0x6366_6C64  # "cfld"
FORMATS = (1, 2)  # format 2 adds per-board fetch outcomes


class LoadError(RuntimeError):
    pass


def migrate(conn: psycopg.Connection) -> list[str]:
    return db.migrate(conn, PACKAGE)


def provision(admin: psycopg.Connection, *, consumer_db: str, producer_db: str, role: str) -> None:
    """Create the loader's role and database (idempotent). Needs a superuser connection."""
    ident = sql.Identifier
    if not admin.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone():
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE").format(ident(role)))
    if admin.execute("SELECT rolsuper FROM pg_roles WHERE rolname = %s", (role,)).fetchone()[0]:
        raise LoadError(f"role {role} is a superuser, so it could reach the producer; use another role")
    if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (consumer_db,)).fetchone():
        admin.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(ident(consumer_db), ident(role)))
    admin.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(ident(consumer_db)))
    admin.execute(sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC").format(ident(producer_db)))
    if admin.execute("SELECT has_database_privilege(%s, %s, 'CONNECT')", (role, producer_db)).fetchone()[0]:
        raise LoadError(f"role {role} can still connect to {producer_db}; check its grants and role memberships")
    with psycopg.connect(dbname=consumer_db, user=role, host=admin.info.host, port=admin.info.port) as c:
        migrate(c)


def _check(path: Path, raw: bytes, expected_prev: str | None, loaded_hours) -> tuple[dict, datetime, bytes]:
    try:
        m = json.loads(raw)
        hour = datetime.fromisoformat(m["hour"])
        name, rows, size, digest, prev = m["file"], m["rows"], m["bytes"], m["sha256"], m["prev_manifest_sha256"]
        runs, missed = m["collection"]["runs"], m["collection"]["missed"]
        fmt = m["format"]
        boards = m["boards"] if fmt == 2 else {}
        if not all(isinstance(k, str) and "/" in k and isinstance(v, str) for k, v in boards.items()):
            raise TypeError("boards must map 'source/token' to an outcome")
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        raise LoadError(f"{path}: not a manifest ({e!r})") from None
    if fmt not in FORMATS:
        raise LoadError(f"{path}: format {fmt}, this loader reads {FORMATS}")
    stem = path.name.removesuffix(".manifest.json")
    if name != stem + ".jsonl" or hour.astimezone(emit.UTC).strftime("%Y/%m/%d/%H") != \
            "/".join([*path.parent.parts[-3:], stem]):
        raise LoadError(f"{path}: names {name!r} for hour {m['hour']}, not its own file")
    if hour in loaded_hours:
        raise LoadError(f"{path}: hour {m['hour']} already loaded from a different manifest")
    if prev != expected_prev:
        raise LoadError(f"{path}: chain broken: prev_manifest_sha256 is {prev}, the last loaded manifest is {expected_prev}")
    try:
        data = path.with_name(name).read_bytes()
    except OSError as e:
        raise LoadError(f"{path}: data file missing or unreadable ({e.strerror})") from None
    if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
        raise LoadError(f"{path}: {name} does not match the manifest's size or sha256")
    lines = data.count(b"\n")
    if lines != rows or (data and not data.endswith(b"\n")):
        raise LoadError(f"{path}: manifest says {rows} rows, {name} has {lines}")
    return {"hour": hour, "file": name, "rows": rows, "sha256": digest, "prev": prev,
            "runs": runs, "missed": missed, "boards": boards}, hour, data


def _merge(conn, entry: dict, manifest_sha: str, data: bytes) -> None:
    conn.execute("""INSERT INTO loaded_files (hour, file, rows, sha256, manifest_sha256, prev_manifest_sha256, runs, missed)
                    VALUES (%(hour)s, %(file)s, %(rows)s, %(sha256)s, %(m)s, %(prev)s, %(runs)s, %(missed)s)""",
                 {**entry, "m": manifest_sha})
    with conn.cursor().copy("COPY board_observations (hour, source, board, outcome) FROM STDIN") as copy:
        for key, outcome in sorted(entry["boards"].items()):
            copy.write_row((entry["hour"], *key.split("/", 1), outcome))
    conn.execute("CREATE TEMP TABLE stage (line jsonb NOT NULL) ON COMMIT DROP")
    with conn.cursor().copy("COPY stage (line) FROM STDIN") as copy:
        for line in data.split(b"\n")[:-1]:  # not splitlines(), which also splits on \r
            copy.write_row((line.decode(),))
    conn.execute("ANALYZE stage")  # a temp table has no statistics until asked
    conn.execute("""INSERT INTO postings (source, board, external_id)
                    SELECT DISTINCT line #>> '{posting,source}', line #>> '{posting,board}', line #>> '{posting,external_id}'
                    FROM stage ORDER BY 1, 2, 3
                    ON CONFLICT DO NOTHING""")
    conn.execute("""INSERT INTO field_versions (posting_id, field, valid_from, value, valid_from_source,
                                                recorded_at, run_id, baseline, hour)
                    SELECT p.id, s.line ->> 'field', (s.line ->> 'valid_from')::timestamptz, s.line -> 'value',
                           s.line ->> 'valid_from_source', (s.line ->> 'recorded_at')::timestamptz,
                           (s.line ->> 'run_id')::bigint, (s.line ->> 'baseline')::boolean, %s
                    FROM stage s JOIN postings p ON (p.source, p.board, p.external_id) =
                        (s.line #>> '{posting,source}', s.line #>> '{posting,board}', s.line #>> '{posting,external_id}')""",
                 (entry["hour"],))


def _path_hour(path: Path) -> str:
    return "/".join([*path.parent.parts[-3:], path.name.removesuffix(".manifest.json")])


def load(conninfo: str, src: Path, verify: bool = False) -> int:
    """Load every change file under `src` not yet loaded, oldest first. Returns how many hours were
    loaded. Stops at the first file that fails a check, raising LoadError; earlier files stay loaded.
    With verify, also re-read every manifest already loaded and refuse any that changed."""
    loaded = 0
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (LOAD_LOCK,))
        applied = set()
        if conn.execute("SELECT to_regclass('schema_migrations') IS NOT NULL").fetchone()[0]:
            applied = {v for (v,) in conn.execute("SELECT version FROM schema_migrations")}
        pending = [m.version for m in db.load_migrations(PACKAGE) if m.version not in applied]
        if pending:
            raise LoadError(f"consumer schema lacks {', '.join(pending)}: run `changefeed consumer-setup`")
        done = dict(conn.execute("SELECT manifest_sha256, hour FROM loaded_files").fetchall())
        hours = set(done.values())
        last, newest = conn.execute("SELECT manifest_sha256, hour FROM loaded_files ORDER BY hour DESC LIMIT 1").fetchone() \
            or (None, None)
        skip_to = newest.astimezone(emit.UTC).strftime("%Y/%m/%d/%H") if newest and not verify else ""
        for path in sorted(Path(src).glob("[0-9]*/[0-9]*/[0-9]*/[0-9]*.manifest.json")):  # zero-padded: path order is hour order
            if _path_hour(path) <= skip_to:
                continue
            try:
                raw = path.read_bytes()
            except OSError as e:
                raise LoadError(f"{path}: unreadable ({e.strerror})") from None
            manifest_sha = hashlib.sha256(raw).hexdigest()
            if manifest_sha in done:
                continue
            entry, hour, data = _check(path, raw, last, hours)
            try:
                with conn.transaction():
                    _merge(conn, entry, manifest_sha, data)
            except (psycopg.DataError, psycopg.IntegrityError, UnicodeDecodeError) as e:
                raise LoadError(f"{path}: refused by the database ({str(e).splitlines()[0]})") from None
            done[manifest_sha], last = hour, manifest_sha
            hours.add(hour)
            loaded += 1
    return loaded
