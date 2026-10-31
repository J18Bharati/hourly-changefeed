# DB fixtures copied from andrefetch-gator @ ebdaff9 (tests/conftest.py).
import os
import uuid

import psycopg
import pytest

from changefeed import db

TEMPLATE = "changefeed_test_template"


def _admin():
    return psycopg.connect(dbname="postgres", autocommit=True)


@pytest.fixture(scope="session")
def template_db():
    """A migrated database that every test clones, so each test starts from the same schema."""
    with _admin() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {TEMPLATE}")
        conn.execute(f"CREATE DATABASE {TEMPLATE}")
    with psycopg.connect(dbname=TEMPLATE) as conn:
        db.migrate(conn)
    yield TEMPLATE
    with _admin() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {TEMPLATE}")


def _scratch_db(template):
    name = f"changefeed_test_{uuid.uuid4().hex[:12]}"
    with _admin() as conn:
        conn.execute(f"CREATE DATABASE {name} TEMPLATE {template}")
    return name


def _drop(name):
    with _admin() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def empty_db():
    """A database with no schema at all."""
    name = _scratch_db("template0")
    yield name
    _drop(name)


@pytest.fixture
def dbname(template_db):
    """A migrated database private to one test."""
    name = _scratch_db(template_db)
    yield name
    _drop(name)


@pytest.fixture
def conn(dbname):
    """Autocommit, like the CLI: each statement or `conn.transaction()` block commits on its own,
    so other connections (workers, claimers) see the data."""
    with psycopg.connect(dbname=dbname, autocommit=True) as c:
        yield c


@pytest.fixture
def connect(dbname):
    """Open more connections to the same test database."""
    opened = []

    def _connect():
        c = psycopg.connect(dbname=dbname, autocommit=True)
        opened.append(c)
        return c
    yield _connect
    for c in opened:
        c.close()


