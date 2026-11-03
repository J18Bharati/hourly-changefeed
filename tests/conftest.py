# DB fixtures adapted from my Python rebuild of andrefetch/gator (https://github.com/andrefetch/gator,
# a Go project).
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



# --- scripted HTTP server (adapted from the same source) ---

import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@dataclass
class Reply:
    status: int = 200
    body: bytes = b""
    headers: dict = field(default_factory=dict)


class FeedServer:
    """Serves scripted replies per path and records every request it gets.

    `routes[path]` is a Reply, a list of Replies (served in turn, the last one repeating),
    or a callable taking the request headers and returning a Reply.
    """

    def __init__(self):
        self.routes = {}
        self.requests = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                server.requests.append((self.path, dict(self.headers)))
                reply = server._reply(self.path, self.headers)
                self.send_response(reply.status)
                for k, v in reply.headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(reply.body)))
                self.end_headers()
                self.wfile.write(reply.body)

            def log_message(self, *args):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True).start()

    def _reply(self, path, headers):
        route = self.routes.get(path)
        if route is None:
            return Reply(404, b"not found")
        if callable(route):
            return route(headers)
        if isinstance(route, list):
            return route.pop(0) if len(route) > 1 else route[0]
        return route

    def url(self, path):
        return self.base + path

    def hits(self, path):
        return [h for p, h in self.requests if p == path]


@pytest.fixture
def server():
    s = FeedServer()
    yield s
    s.httpd.shutdown()
