"""PARITY: SqlDriverBase orchestration — the C# SqlDriverBaseTests parity locks, against a fake DB seam."""

from __future__ import annotations

import pytest

from eds.dbchange import DBChangeEvent
from eds.drivers.postgresql.driver import PostgresqlDriver
from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util.gojson import RawJson


class CapturingLogger:
    """Records every log call (level, formatted-message) and is its own prefix/fields chain."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def _log(self, level: str, msg: str, args: tuple) -> None:
        self.messages.append((level, msg % args if args else msg))

    def trace(self, msg, *args): self._log("trace", msg, args)
    def debug(self, msg, *args): self._log("debug", msg, args)
    def info(self, msg, *args): self._log("info", msg, args)
    def warn(self, msg, *args): self._log("warn", msg, args)
    def error(self, msg, *args): self._log("error", msg, args)
    def fatal(self, msg, *args): self._log("fatal", msg, args)
    def with_prefix(self, prefix): return self
    def with_fields(self, fields): return self

    def has(self, substr: str) -> bool:
        return any(substr in m for _, m in self.messages)


class FakeSqlDb:
    """In-memory SqlDb seam — records commits/execs/drops; can fail the transactional exec."""

    def __init__(self, fail_exec: bool = False, schema: DatabaseSchema | None = None) -> None:
        self.fail_exec = fail_exec
        self.committed: list[str] = []
        self.executed: list[str] = []
        self.dropped: list[str] = []
        self.closed = False
        self._schema = schema if schema is not None else DatabaseSchema()

    def query_single_value(self, fn: str) -> str:
        return "testdb"

    def build_schema(self, logger, column, value, fail_if_empty, conditions) -> DatabaseSchema:
        return self._schema

    def execute_in_transaction(self, sql: str, logger) -> None:
        if self.fail_exec:
            logger.error("offending sql: %s", sql)  # PARITY: offending sql logged only on the exec failure
            raise RuntimeError("exec failed")
        self.committed.append(sql)

    def exec(self, sql: str) -> None:
        self.executed.append(sql)

    def drop_table(self, quoted_table: str) -> None:
        self.dropped.append(quoted_table)

    def create_import_executor(self, dry_run: bool):
        return self.committed.append

    def close(self) -> None:
        self.closed = True


class FakeRegistry:
    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    def get_table_version(self, table: str) -> tuple[bool, str]:
        return True, self._schema.model_version or "v1"

    def get_schema(self, table: str, version: str) -> Schema:
        return self._schema

    def get_latest_schema(self) -> dict[str, Schema]:
        return {self._schema.table: self._schema}


def _user_schema() -> Schema:
    return Schema(
        table="user", model_version="v1", primary_keys=["id"], required=["id"],
        properties={
            "id": SchemaProperty(type="string"), "name": SchemaProperty(type="string"),
            "age": SchemaProperty(type="integer"), "meta": SchemaProperty(type="object"),
        },
    )


def _insert_event(uid: str) -> DBChangeEvent:
    return DBChangeEvent(
        operation="INSERT", table="user", key=[uid],
        after=RawJson(f'{{"id":"{uid}","name":"Bob","age":42,"meta":{{"k":"v"}}}}'),
    )


def _driver(fail_exec: bool = False) -> PostgresqlDriver:
    drv = PostgresqlDriver()
    drv._db = FakeSqlDb(fail_exec=fail_exec)
    drv._registry = FakeRegistry(_user_schema())
    drv._logger = CapturingLogger()
    drv._dbname = "testdb"
    return drv


def test_flush_preserves_buffer_on_error_then_resends() -> None:
    drv = _driver(fail_exec=True)
    log = CapturingLogger()
    drv.process(log, _insert_event("u1"))
    drv.process(log, _insert_event("u2"))
    assert drv._count == 2

    with pytest.raises(RuntimeError):
        drv.flush(log)
    assert drv._count == 2  # buffer preserved — no reset on error
    assert drv._db.committed == []  # nothing committed
    assert log.has("offending sql")

    drv._db.fail_exec = False
    drv.flush(log)
    assert drv._count == 0
    assert len(drv._db.committed) == 1  # one batch, both events resent
    assert "u1" in drv._db.committed[0]
    assert "u2" in drv._db.committed[0]


def test_flush_clears_buffer_on_success() -> None:
    drv = _driver()
    log = CapturingLogger()
    drv.process(log, _insert_event("u1"))
    drv.flush(log)
    assert len(drv._db.committed) == 1
    drv.flush(log)  # no-op (count == 0)
    assert len(drv._db.committed) == 1


def test_postgres_logs_flushed_count() -> None:
    drv = _driver()
    log = CapturingLogger()
    drv.process(log, _insert_event("u1"))
    drv.flush(log)
    assert log.has("flushed 1 records")


def test_other_drivers_do_not_log_flushed_count() -> None:
    class _NoFlushLogDriver(PostgresqlDriver):  # represents MySQL/MSSQL (no OnFlushed override)
        def on_flushed(self, logger, count) -> None:
            pass

    drv = _NoFlushLogDriver()
    drv._db = FakeSqlDb()
    drv._registry = FakeRegistry(_user_schema())
    drv._dbname = "testdb"
    log = CapturingLogger()
    drv.process(log, _insert_event("u1"))
    drv.flush(log)
    assert not log.has("flushed")


def test_migrate_drops_and_recreates_when_table_exists() -> None:
    drv = _driver()
    drv._dbschema = DatabaseSchema({"user": {"id": "TEXT"}})
    log = CapturingLogger()
    drv.migrate_new_table(None, log, _user_schema())
    assert drv._db.dropped == ['"user"']
    assert any("CREATE TABLE" in s for s in drv._db.executed)
    assert log.has("table already exists")


def test_migrate_creates_without_drop_when_table_absent() -> None:
    drv = _driver()  # empty dbschema
    log = CapturingLogger()
    drv.migrate_new_table(None, log, _user_schema())
    assert drv._db.dropped == []
    assert any("CREATE TABLE" in s for s in drv._db.executed)
