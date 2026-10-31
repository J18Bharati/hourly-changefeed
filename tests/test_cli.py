import psycopg

from changefeed import cli


def test_migrate_command_is_idempotent(empty_db, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={empty_db}")
    assert cli.main(["migrate"]) == 0
    assert "applied 0001_boards" in capsys.readouterr().out
    assert cli.main(["migrate"]) == 0
    assert "up to date" in capsys.readouterr().out


def test_unknown_command_fails_with_usage(empty_db, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={empty_db}")
    assert cli.main(["nope"]) == 1
    assert "usage" in capsys.readouterr().err


def test_boards_are_unique_per_source_and_token(conn):
    conn.execute("INSERT INTO boards (source, token) VALUES ('greenhouse', 'acme')")
    try:
        conn.execute("INSERT INTO boards (source, token) VALUES ('greenhouse', 'acme')")
        raise AssertionError("duplicate board accepted")
    except psycopg.errors.UniqueViolation:
        pass


def test_run_with_nothing_due(dbname, monkeypatch, capsys):
    monkeypatch.setenv("CHANGEFEED_DB", f"dbname={dbname}")
    assert cli.main(["run"]) == 0
    assert capsys.readouterr().out.startswith("run 1: ok (nothing due)")
