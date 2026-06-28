"""PARITY: internal/drivers/sqlserver/{sql.go, escape.go} — golden vectors (Go + C# MssqlSql)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from eds.dbchange import DBChangeEvent
from eds.drivers.sqlserver.sql import (
    add_new_columns_sql,
    create_sql,
    escape_string,
    handle_schema_property,
    parse_url_to_dsn,
    quote_identifier,
    quote_value,
    to_sql,
    to_sql_from_object,
)
from eds.schema import DatabaseSchema, Schema, SchemaProperty


@pytest.mark.parametrize("name", ["test", "order", "current", "select", "updatedDate", "companyId"])
def test_quote_identifier(name) -> None:
    assert quote_identifier(name) == f"[{name}]"


@pytest.mark.parametrize(
    ("s", "expected"),
    [("a'b", "a''b"), ('a"b', r'a\"b'), ("a\nb", r"a\nb"), ("a\\b", r"a\\b"), ("a\x00b", r"a\0b")],
)
def test_escape_string_hybrid(s, expected) -> None:
    assert escape_string(s) == expected


@pytest.mark.parametrize(
    ("arg", "expected"),
    [
        ("test", "'test'"),
        ("test with a 'hi'", "'test with a ''hi'''"),  # ' doubled
        (1, "1"),
        (1.1, "1.1"),
        (True, "1"),
        (False, "0"),
        (None, "NULL"),
        ({"a": "b"}, "'" + r'{\"a\":\"b\"}' + "'"),
        ([{"a": "b"}], "'" + r'[{\"a\":\"b\"}]' + "'"),
        (datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "'2021-01-01 00:00:00'"),
        ("2024-07-09T18:28:03.69708Z", "'2024-07-09 18:28:03.69708'"),
        ("2024-07-09T18:28:03Z", "'2024-07-09 18:28:03'"),
    ],
)
def test_quote_value(arg, expected) -> None:
    assert quote_value(arg) == expected


@pytest.mark.parametrize(
    ("prop", "v", "expected"),
    [
        (SchemaProperty(type="boolean"), "NULL", "0"),
        (SchemaProperty(type="boolean"), "true", "1"),
        (SchemaProperty(type="boolean"), "1", "1"),
        (SchemaProperty(type="boolean"), "false", "0"),
        (SchemaProperty(type="boolean", nullable=False), "", "0"),
        (SchemaProperty(type="integer"), "NULL", "0"),
        (SchemaProperty(type="integer"), "5", "5"),
        (SchemaProperty(type="array", nullable=False), "NULL", "''"),
        (SchemaProperty(type="array", nullable=True), "NULL", "NULL"),
        (SchemaProperty(type="string"), "NULL", "NULL"),
        (SchemaProperty(type="object", additional_properties=True), "keepme", "keepme"),
        (SchemaProperty(type="object"), "NULL", "NULL"),
    ],
)
def test_handle_schema_property(prop, v, expected) -> None:
    assert handle_schema_property(prop, v) == expected


def _merge_schema() -> Schema:
    return Schema(
        table="order", model_version="v1", primary_keys=["id"], required=[],
        properties={
            "id": SchemaProperty(type="string"), "archived": SchemaProperty(type="boolean"),
            "count": SchemaProperty(type="integer"), "name": SchemaProperty(type="string"),
        },
    )


def test_merge_all_values() -> None:
    o = {"id": "1", "name": "Widget", "archived": True, "count": 5.0}
    out = to_sql_from_object(_merge_schema(), "order", o, None)
    assert out == (
        "MERGE [order] AS target USING (VALUES('1')) AS source (id) ON target.id=source.id "
        "WHEN MATCHED THEN UPDATE SET [archived]=1,[count]=5,[name]='Widget' "
        "WHEN NOT MATCHED THEN INSERT ([id],[archived],[count],[name]) VALUES ('1',1,5,'Widget');"
    )


def test_merge_missing_values_and_diff() -> None:
    out = to_sql_from_object(_merge_schema(), "order", {"id": "1"}, ["name"])
    assert out == (
        "MERGE [order] AS target USING (VALUES('1')) AS source (id) ON target.id=source.id "
        "WHEN MATCHED THEN UPDATE SET [name]=NULL "
        "WHEN NOT MATCHED THEN INSERT ([id],[archived],[count],[name]) VALUES ('1',0,0,NULL);"
    )


def test_to_sql_delete() -> None:
    evt = DBChangeEvent(operation="DELETE", table="order", key=["abc"])
    assert to_sql(evt, _merge_schema()) == "DELETE FROM [order] WHERE [id]='abc';\n"


def _create_schema() -> Schema:
    return Schema(
        table="order", primary_keys=["id"], required=[],
        properties={"id": SchemaProperty(type="string"), "name": SchemaProperty(type="string")},
    )


def test_create_sql() -> None:
    assert create_sql(_create_schema()) == (
        "DROP TABLE IF EXISTS [order];\n"
        "CREATE TABLE [order] (\n"
        "\t[id] VARCHAR(64),\n"
        "\t[name] NVARCHAR(MAX),\n"
        "\tPRIMARY KEY ([id])\n"
        ")"
    )


def _cols_schema() -> Schema:
    return Schema(
        table="order", primary_keys=["id"],
        properties={
            "id": SchemaProperty(type="string"), "number": SchemaProperty(type="string"),
            "externalNumber": SchemaProperty(type="string", nullable=True),
        },
    )


def test_add_new_columns_sql() -> None:
    out = add_new_columns_sql(None, ["number", "internalNumber", "externalNumber"], _cols_schema(), DatabaseSchema())
    assert out == [
        "ALTER TABLE [order] ADD [number] NVARCHAR(MAX);",
        "ALTER TABLE [order] ADD [internalNumber] NVARCHAR(MAX);",
        "ALTER TABLE [order] ADD [externalNumber] NVARCHAR(MAX);",
    ]


def test_add_new_columns_sql_skips_existing() -> None:
    db = DatabaseSchema({"order": {"number": "X"}})
    out = add_new_columns_sql(None, ["number", "externalNumber"], _cols_schema(), db)
    assert out == ["ALTER TABLE [order] ADD [externalNumber] NVARCHAR(MAX);"]


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("sqlserver://root:password@localhost:3306/eds",
         "sqlserver://root:password@localhost:3306?app+name=eds&database=eds&encrypt=disable"),
        ("sqlserver://root:password@localhost:3306/eds?encrypt=enable",
         "sqlserver://root:password@localhost:3306?app+name=eds&database=eds&encrypt=enable"),
        ("sqlserver://root:password@foo.microsoft.com:3306/eds",
         "sqlserver://root:password@foo.microsoft.com:3306?app+name=eds&database=eds"),
        # explicit EMPTY values are overwritten (Go gates on Get()=="")
        ("sqlserver://sa:pw@localhost:1433/eds?encrypt=",
         "sqlserver://sa:pw@localhost:1433?app+name=eds&database=eds&encrypt=disable"),
    ],
)
def test_parse_url_to_dsn(url, expected) -> None:
    assert parse_url_to_dsn(url) == expected


def test_quote_value_pre_1970_timestamp_floored() -> None:
    assert quote_value("0000-01-01T00:00:00Z") == "'1970-01-01 00:00:01'"
    assert quote_value("1969-12-31T23:59:59Z") == "'1970-01-01 00:00:01'"
