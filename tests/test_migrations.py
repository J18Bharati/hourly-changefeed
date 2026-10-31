import psycopg
import pytest

from changefeed import db


def _tables(conn):
    return {r[0] for r in conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}


def test_migrate_creates_schema(empty_db):
    with psycopg.connect(dbname=empty_db) as conn:
        applied = db.migrate(conn)
        assert applied, "first run applies every migration"
        assert {"schema_migrations", "boards"} <= _tables(conn)


def test_migrate_twice_is_a_noop(empty_db):
    with psycopg.connect(dbname=empty_db) as conn:
        db.migrate(conn)
        before = conn.execute("SELECT version, checksum, applied_at FROM schema_migrations ORDER BY version").fetchall()
        assert db.migrate(conn) == []
        after = conn.execute("SELECT version, checksum, applied_at FROM schema_migrations ORDER BY version").fetchall()
        assert before == after


def test_edited_migration_is_refused(empty_db, monkeypatch):
    with psycopg.connect(dbname=empty_db) as conn:
        db.migrate(conn)
        real = db.load_migrations()
        first = real[0]
        tampered = [db.Migration(first.version, first.sql + "\n-- edited", )] + real[1:]
        monkeypatch.setattr(db, "load_migrations", lambda package=None: tampered)
        with pytest.raises(db.MigrationError, match=first.version):
            db.migrate(conn)


def test_all_timestamps_have_time_zones(conn):
    naive = conn.execute("""
        SELECT table_name || '.' || column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND data_type = 'timestamp without time zone'
    """).fetchall()
    assert naive == []
