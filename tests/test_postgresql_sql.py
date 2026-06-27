"""PARITY: internal/drivers/postgresql/sql.go — golden SQL (C# PostgresqlSqlTests + Go sql_test.go vectors)."""

from __future__ import annotations

import pytest

from eds.dbchange import DBChangeEvent
from eds.drivers.postgresql.sql import (
    add_new_columns_sql,
    create_sql,
    get_connection_string_from_url,
    quote_identifier,
    quote_string,
    quote_value,
    to_sql,
)
from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util.gojson import RawJson


def _user_schema() -> Schema:
    return Schema(
        table="user",
        model_version="v1",
        primary_keys=["id"],
        required=["id"],
        properties={
            "id": SchemaProperty(type="string"),
            "name": SchemaProperty(type="string"),
            "age": SchemaProperty(type="integer"),
            "meta": SchemaProperty(type="object"),
        },
    )


@pytest.mark.parametrize(
    ("s", "expected"),
    [
        ("hello world", "'hello world'"),
        ("a\nb", "$_H_$a\nb$_H_$"),
        ("abc\n", "$_H_$abc\n$_H_$"),  # trailing newline -> dollar-quoted (RE2 \Z)
        ("a\0b", "'ab'"),  # NUL stripped -> safe
        ("", "''"),
        ("it's", "$_H_$it's$_H_$"),  # single quote not in safe set -> dollar-quoted
    ],
)
def test_quote_string(s: str, expected: str) -> None:
    assert quote_string(s) == expected


def test_quote_value_types() -> None:
    assert quote_value(None) == "null"
    assert quote_value(True) == "true"
    assert quote_value(False) == "false"
    assert quote_value(30.0) == "30"
    assert quote_value(9.5) == "9.5"
    assert quote_value("x") == "'x'"


@pytest.mark.parametrize("name", ["test", "order", "id", "name", "updatedDate", "select", "number"])
def test_quote_identifier(name: str) -> None:
    assert quote_identifier(name) == f'"{name}"'


def test_create_sql_golden() -> None:
    assert create_sql(_user_schema()) == (
        'DROP TABLE IF EXISTS "user";\n'
        'CREATE TABLE "user" (\n'
        '\t"id" TEXT NOT NULL,\n'
        '\t"age" BIGINT,\n'
        '\t"meta" JSONB,\n'
        '\t"name" TEXT,\n'
        '\tPRIMARY KEY ("id")\n'
        ");\n"
    )


def test_insert_golden() -> None:
    evt = DBChangeEvent(
        operation="INSERT", table="user", key=["u1"],
        after=RawJson('{"id":"u1","name":"Bob","age":42,"meta":{"k":"v"}}'),
    )
    assert to_sql(evt, _user_schema()) == (
        'INSERT INTO "user" ("id","age","meta","name") VALUES '
        '(\'u1\',42,\'{"k":"v"}\',\'Bob\') ON CONFLICT (id) DO UPDATE SET '
        '"age"=42,"meta"=\'{"k":"v"}\',"name"=\'Bob\';\n'
    )


def test_update_diff_only_golden() -> None:
    evt = DBChangeEvent(
        operation="UPDATE", table="user", key=["u1"], diff=["name"],
        after=RawJson('{"id":"u1","name":"Bob","age":42}'),  # meta missing -> '{}'
    )
    assert to_sql(evt, _user_schema()) == (
        'INSERT INTO "user" ("id","age","meta","name") VALUES '
        '(\'u1\',42,\'{}\',\'Bob\') ON CONFLICT (id) DO UPDATE SET "name"=\'Bob\';\n'
    )


def test_delete_golden() -> None:
    evt = DBChangeEvent(operation="DELETE", table="user", key=["u1"])
    assert to_sql(evt, _user_schema()) == 'DELETE FROM "user" WHERE "id"=\'u1\';\n'


def _order_schema() -> Schema:
    return Schema(
        table="order",
        primary_keys=["id"],
        properties={
            "id": SchemaProperty(type="string"),
            "name": SchemaProperty(type="string"),
            "number": SchemaProperty(type="string"),
            "externalNumber": SchemaProperty(type="string", nullable=True),
        },
    )


def test_add_new_columns_sql() -> None:
    cols = ["number", "internalNumber", "externalNumber"]  # internalNumber not in schema -> zero -> TEXT
    out = add_new_columns_sql(None, cols, _order_schema(), DatabaseSchema())
    assert out == [
        'ALTER TABLE "order" ADD COLUMN "number" TEXT;',
        'ALTER TABLE "order" ADD COLUMN "internalNumber" TEXT;',
        'ALTER TABLE "order" ADD COLUMN "externalNumber" TEXT;',
    ]


def test_add_new_columns_sql_skips_existing() -> None:
    cols = ["number", "internalNumber", "externalNumber"]
    db = DatabaseSchema({"order": {"number": "TEXT"}})
    out = add_new_columns_sql(None, cols, _order_schema(), db)
    assert out == [
        'ALTER TABLE "order" ADD COLUMN "internalNumber" TEXT;',
        'ALTER TABLE "order" ADD COLUMN "externalNumber" TEXT;',
    ]


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgres://localhost", "postgresql://localhost:5432?application_name=eds&sslmode=disable"),
        ("postgres://localhost:15432", "postgresql://localhost:15432?application_name=eds&sslmode=disable"),
        ("postgres://localhost:15432?application_name=foo&sslmode=disable",
         "postgresql://localhost:15432?application_name=foo&sslmode=disable"),
        ("postgres://127.0.0.1:15432?application_name=foo&sslmode=disable",
         "postgresql://127.0.0.1:15432?application_name=foo&sslmode=disable"),
        ("postgres://127.0.0.1:15432?application_name=foo&sslmode=require",
         "postgresql://127.0.0.1:15432?application_name=foo&sslmode=require"),
        ("postgres://foo.aws.com:15432?application_name=foo",
         "postgresql://foo.aws.com:15432?application_name=foo"),  # remote → no sslmode
        # derived (url.String reassembly + reencode gate)
        ("postgres://localhost/db", "postgresql://localhost:5432/db?application_name=eds&sslmode=disable"),
        ("postgres://hostname:5432/db", "postgresql://hostname:5432/db?application_name=eds"),
    ],
)
def test_get_connection_string_from_url(url, expected) -> None:
    assert get_connection_string_from_url(url) == expected
