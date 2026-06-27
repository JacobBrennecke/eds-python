"""PARITY: internal/util/dbschema.go + sql.go helpers — over a fake DB-API connection (no Docker)."""

from __future__ import annotations

import pytest

from eds.util.db import (
    build_db_schema_from_info_schema,
    drop_table,
    query_single_value,
    sql_executer,
)


class FakeCursor:
    def __init__(self, rows=None, row="__unset__") -> None:
        self.rows = rows if rows is not None else []
        self._row = row
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self):
        return None if self._row == "__unset__" else self._row

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, cur: FakeCursor) -> None:
        self._cur = cur

    def cursor(self) -> FakeCursor:
        return self._cur


class CapturingLogger:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def _log(self, level, msg, args):
        self.messages.append((level, msg % args if args else msg))

    def trace(self, m, *a): self._log("trace", m, a)
    def debug(self, m, *a): self._log("debug", m, a)
    def info(self, m, *a): self._log("info", m, a)
    def warn(self, m, *a): self._log("warn", m, a)
    def error(self, m, *a): self._log("error", m, a)
    def fatal(self, m, *a): self._log("fatal", m, a)
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def test_query_single_value() -> None:
    cur = FakeCursor(row=("testdb",))
    assert query_single_value(FakeConn(cur), "current_database()") == "testdb"
    assert cur.executed == ["SELECT current_database()"]


def test_query_single_value_no_row_raises() -> None:
    with pytest.raises(ValueError, match="no rows returned"):
        query_single_value(FakeConn(FakeCursor()), "current_database()")


def test_build_db_schema() -> None:
    cur = FakeCursor(rows=[("t1", "c1", "TEXT"), ("t1", "c2", "BIGINT"), ("t2", "c3", "JSONB")])
    schema = build_db_schema_from_info_schema(None, FakeConn(cur), "table_catalog", "mydb", False)
    assert schema == {"t1": {"c1": "TEXT", "c2": "BIGINT"}, "t2": {"c3": "JSONB"}}
    assert cur.executed[0] == (
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog = 'mydb'"
    )


def test_build_db_schema_with_conditions() -> None:
    cur = FakeCursor(rows=[("t1", "c1", "TEXT")])
    build_db_schema_from_info_schema(None, FakeConn(cur), "table_schema", "public", False, [("table_name", "t1")])
    assert cur.executed[0].endswith("WHERE table_schema = 'public' AND table_name = 't1'")


def test_build_db_schema_fail_if_empty() -> None:
    with pytest.raises(ValueError, match="no tables found using table_catalog = mydb"):
        build_db_schema_from_info_schema(None, FakeConn(FakeCursor(rows=[])), "table_catalog", "mydb", True)


def test_sql_executer_dry_run() -> None:
    cur = FakeCursor()
    log = CapturingLogger()
    sql_executer(log, FakeConn(cur), dry_run=True)("CREATE TABLE x;")
    assert cur.executed == []  # not executed
    assert ("info", "[dry-run] CREATE TABLE x;") in log.messages


def test_sql_executer_executes_and_trims_log() -> None:
    cur = FakeCursor()
    log = CapturingLogger()
    sql_executer(log, FakeConn(cur), dry_run=False)("INSERT INTO x;\n\n")
    assert cur.executed == ["INSERT INTO x;\n\n"]  # exec uses the original sql
    assert ("debug", "executing: INSERT INTO x;") in log.messages  # log is right-trimmed


def test_drop_table() -> None:
    cur = FakeCursor()
    drop_table(FakeConn(cur), '"user"')
    assert cur.executed == ['DROP TABLE IF EXISTS "user"']
