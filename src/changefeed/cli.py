"""changefeed command line. The database comes from CHANGEFEED_DB (a libpq conninfo); empty means
libpq's own defaults (PGHOST, PGDATABASE, ...)."""
import os
import sys

import psycopg

from changefeed import db

USAGE = """usage: changefeed <command>
  migrate    apply pending schema migrations"""


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] != ["migrate"] or len(argv) != 1:
        print(USAGE, file=sys.stderr)
        return 1
    with psycopg.connect(os.environ.get("CHANGEFEED_DB", ""), autocommit=True) as conn:
        applied = db.migrate(conn)
    print("\n".join(f"applied {v}" for v in applied) or "schema up to date")
    return 0
