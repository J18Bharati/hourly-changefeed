# Copied from andrefetch-gator @ ebdaff9 (src/gator/db.py); the lock key and package name differ.
"""Schema migrations: numbered SQL files applied once each, in order, under an advisory lock.

A migration that was applied and later edited is refused rather than silently skipped.
"""
import hashlib
from dataclasses import dataclass
from importlib import resources

import psycopg

MIGRATION_LOCK = 0x6366_6D67  # "cfmg": arbitrary advisory-lock key for this app


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()


def load_migrations() -> list[Migration]:
    files = resources.files("changefeed.migrations")
    return sorted(
        (Migration(f.name.removesuffix(".sql"), f.read_text()) for f in files.iterdir() if f.name.endswith(".sql")),
        key=lambda m: m.version,
    )


def migrate(conn: psycopg.Connection) -> list[str]:
    """Apply pending migrations. Returns the versions applied by this call."""
    applied_now = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK,))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    text PRIMARY KEY,
                checksum   text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )""")
        done = dict(conn.execute("SELECT version, checksum FROM schema_migrations").fetchall())
        for m in load_migrations():
            if m.version in done:
                if done[m.version] != m.checksum:
                    raise MigrationError(f"migration {m.version} was edited after it was applied")
                continue
            conn.execute(m.sql)
            conn.execute("INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)", (m.version, m.checksum))
            applied_now.append(m.version)
    return applied_now
