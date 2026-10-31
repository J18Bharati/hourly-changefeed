"""changefeed command line. The database comes from CHANGEFEED_DB (a libpq conninfo); empty means
libpq's own defaults (PGHOST, PGDATABASE, ...). `load` uses CHANGEFEED_CONSUMER_DB instead and never
connects to the producer."""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from changefeed import collect, db, emit, load, ops, registry, reports, versions
from changefeed.adapters import ADAPTERS


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="changefeed")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply pending schema migrations")
    sub.add_parser("run", help="fetch every due board once")
    sub.add_parser("run-hour", help="what the timer runs: record missed hours, then run, under a lock")
    em = sub.add_parser("emit", help="write the change file and manifest for every finished hour")
    em.add_argument("--out", type=Path, default=Path.home() / ".local/share/hourly-changefeed/changes")
    ld = sub.add_parser("load", help="consumer: load new change files into CHANGEFEED_CONSUMER_DB")
    ld.add_argument("--dir", type=Path, default=Path.home() / ".local/share/hourly-changefeed/changes")
    sub.add_parser("report", help="consumer: fill rates, freshness and missed hours")
    cs = sub.add_parser("consumer-setup", help="create the loader's role and database (run as a superuser)")
    cs.add_argument("--consumer-db", default="changefeed_consumer")
    cs.add_argument("--role", default="changefeed_loader")
    sub.add_parser("health", help="exit 1 unless some board was fetched successfully in the last 2 hours")
    sub.add_parser("status", help="runs, outcomes, churn, and boards or postings that need a look")
    sub.add_parser("maintain", help="prune raw bodies older than 14 days and record today's sizes")
    bk = sub.add_parser("backup", help="pg_dump without raw bodies, keeping the newest 3")
    bk.add_argument("--dir", type=Path, default=Path.home() / ".local/share/hourly-changefeed/backups")
    rb = sub.add_parser("rebuild-versions", help="replay every board's history from its raw bodies")
    rb.add_argument("--board", type=int, help="only this board id")
    seed = sub.add_parser("seed", help="manage the board registry").add_subparsers(dest="seed_cmd", required=True)
    imp = seed.add_parser("import", help="add candidate boards from a JSON list of tokens")
    imp.add_argument("source", choices=sorted(ADAPTERS))
    imp.add_argument("file", type=Path)
    imp.add_argument("--sample", type=int, help="take this many, chosen reproducibly")
    imp.add_argument("--seed", type=int, default=0)
    val = seed.add_parser("validate", help="fetch each candidate once; keep boards with postings")
    val.add_argument("--workers", type=int, default=4)
    seed.add_parser("export", help="print the active boards as TSV")
    load = seed.add_parser("load", help="add boards from an exported TSV as active")
    load.add_argument("file", type=Path)
    return p


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(sys.argv[1:] if argv is None else argv)
    except SystemExit as e:  # argparse has printed usage (or --help) already
        return 1 if e.code else 0
    if args.cmd == "load":
        try:
            print(f"loaded {load.load(os.environ.get('CHANGEFEED_CONSUMER_DB', ''), args.dir)} hours")
        except load.LoadError as e:
            print(f"refused: {e}", file=sys.stderr)
            return 1
        return 0
    if args.cmd == "report":
        with psycopg.connect(os.environ.get("CHANGEFEED_CONSUMER_DB", ""), options="-c timezone=UTC") as conn:
            print(reports.render(conn))
        return 0
    with psycopg.connect(os.environ.get("CHANGEFEED_DB", ""), autocommit=True) as conn:
        if args.cmd == "consumer-setup":
            load.provision(conn, consumer_db=args.consumer_db, producer_db=conn.info.dbname, role=args.role)
            print(f"{args.consumer_db} owned by {args.role}, which cannot connect to {conn.info.dbname}")
            return 0
        if args.cmd == "migrate":
            applied = db.migrate(conn)
            print("\n".join(f"applied {v}" for v in applied) or "schema up to date")
            return 0
        db.migrate(conn)
    if args.cmd in ("run", "run-hour"):
        conninfo = os.environ.get("CHANGEFEED_DB", "")
        result = collect.run(conninfo) if args.cmd == "run" else collect.run_hour(conninfo)
        if result is None:
            print("another run holds the lock; nothing to do")
            return 0
        counts = ", ".join(f"{k}: {v}" for k, v in sorted(result.counts.items())) or "nothing due"
        print(f"run {result.id}: {result.status} ({counts})")
        return 1 if result.status == "missed" else 0
    conninfo = os.environ.get("CHANGEFEED_DB", "")
    if args.cmd in ("health", "status", "maintain"):
        with psycopg.connect(conninfo, autocommit=True) as conn:
            if args.cmd == "health":
                ok, message = ops.health(conn)
                print(("ok: " if ok else "UNHEALTHY: ") + message)
                return 0 if ok else 1
            if args.cmd == "status":
                print(ops.status(conn))
                return 0
            r = ops.report(conn, ops.prune(conn))
            print(", ".join(f"{k} {v}" for k, v in r.items()))
            return 0
    if args.cmd == "emit":
        print(f"emitted {emit.emit(conninfo, args.out)} hours")
        return 0
    if args.cmd == "backup":
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%MZ")
        print(ops.backup(conninfo, args.dir, stamp=stamp))
        return 0
    if args.cmd == "rebuild-versions":
        with psycopg.connect(os.environ.get("CHANGEFEED_DB", ""), autocommit=True) as conn:
            conn.execute("SELECT pg_advisory_lock(%s)", (collect.RUN_LOCK,))  # never during a run
            boards = [args.board] if args.board else [b for (b,) in conn.execute("SELECT id FROM boards ORDER BY id")]
            total = 0
            for board_id in boards:
                with conn.transaction():
                    total += versions.rebuild(conn, board_id)
        print(f"rebuilt {len(boards)} boards: {total} versions")
        return 0
    with psycopg.connect(os.environ.get("CHANGEFEED_DB", ""), autocommit=True) as conn:
        match args.seed_cmd:
            case "import":
                tokens = json.loads(args.file.read_text())
                if args.sample:
                    tokens = registry.sample(tokens, args.sample, seed=args.seed)
                inserted, skipped = registry.import_tokens(conn, args.source, tokens)
                print(f"imported {inserted} {args.source} boards ({skipped} malformed tokens skipped)")
            case "validate":
                counts = registry.validate(conn, workers=args.workers)
                print(", ".join(f"{k}: {v}" for k, v in sorted(counts.items())) or "nothing to validate")
            case "export":
                sys.stdout.write(registry.export(conn))
            case "load":
                print(f"loaded {registry.load(conn, args.file.read_text())} boards")
    return 0
