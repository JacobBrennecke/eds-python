"""PARITY: update_destination_schema — startup reconcile of the driver's destination schema vs the registry."""

from __future__ import annotations

import pytest

from eds.consumer.consumer import update_destination_schema
from eds.schema import Schema, SchemaProperty


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def _schema(table: str, cols: list[str]) -> Schema:
    return Schema(
        table=table, model_version="v1", primary_keys=[cols[0]],
        properties={c: SchemaProperty(type="string") for c in cols},
    )


class _Reg:
    def __init__(self, schemas: dict[str, Schema], found: bool = True) -> None:
        self._schemas = schemas
        self._found = found

    def get_latest_schema(self) -> dict[str, Schema]:
        return self._schemas

    def get_table_version(self, table: str) -> tuple[bool, str]:
        return (self._found, self._schemas[table].model_version)


class _Driver:
    def __init__(self, dest: dict[str, dict[str, str]]) -> None:
        self._dest = dest
        self.new_tables: list = []
        self.new_cols: list = []

    def get_destination_schema(self, ctx, logger) -> dict[str, dict[str, str]]:
        return self._dest

    def migrate_new_table(self, ctx, logger, schema) -> None:
        self.new_tables.append(schema.table)

    def migrate_new_columns(self, ctx, logger, schema, cols) -> None:
        self.new_cols.append((schema.table, list(cols)))


def test_migrates_new_table() -> None:
    d = _Driver(dest={})  # table absent from destination
    update_destination_schema(_QuietLogger(), _Reg({"user": _schema("user", ["id", "name"])}), d)
    assert d.new_tables == ["user"] and d.new_cols == []


def test_migrates_new_columns() -> None:
    d = _Driver(dest={"user": {"id": "text", "name": "text"}})  # missing email
    update_destination_schema(_QuietLogger(), _Reg({"user": _schema("user", ["id", "name", "email"])}), d)
    assert d.new_tables == [] and d.new_cols == [("user", ["email"])]


def test_no_new_columns() -> None:
    d = _Driver(dest={"user": {"id": "text", "name": "text"}})
    update_destination_schema(_QuietLogger(), _Reg({"user": _schema("user", ["id", "name"])}), d)
    assert d.new_tables == [] and d.new_cols == []


def test_table_version_not_found_raises() -> None:
    d = _Driver(dest={})
    with pytest.raises(RuntimeError, match="table version"):
        update_destination_schema(_QuietLogger(), _Reg({"user": _schema("user", ["id"])}, found=False), d)
