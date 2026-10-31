"""changefeed command line. The database comes from CHANGEFEED_DB (a libpq conninfo); empty means
libpq's own defaults (PGHOST, PGDATABASE, ...)."""
import argparse
import json
import os
import sys
from pathlib import Path

import psycopg

from changefeed import collect, db, registry
from changefeed.adapters import ADAPTERS


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="changefeed")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply pending schema migrations")
    sub.add_parser("run", help="fetch every due board once")
    sub.add_parser("run-hour", help="what the timer runs: record missed hours, then run, under a lock")
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
    with psycopg.connect(os.environ.get("CHANGEFEED_DB", ""), autocommit=True) as conn:
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
