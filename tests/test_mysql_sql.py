"""PARITY: internal/drivers/mysql/{sql.go, escape.go} + gofloat.format_g — golden vectors (Go + C# MysqlSql)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from eds.dbchange import DBChangeEvent
from eds.drivers.mysql.sql import (
    add_new_columns_sql,
    create_sql,
    quote_identifier,
    quote_value,
    to_sql,
)
from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util.gofloat import format_g
from eds.util.gojson import RawJson


@pytest.mark.parametrize(
    ("f", "expected"),
    [
        (1.0, "1"), (1.1, "1.1"), (1000.0, "1000"), (46700.0, "46700"), (1004.0, "1004"),
        (1234.5, "1234.5"), (1e21, "1e+21"), (1e-5, "1e-05"), (0.0001, "0.0001"), (1e-7, "1e-07"),
        (-0.0, "-0"), (123456789012345680000.0, "123456789012345680000"),
    ],
)
def test_format_g(f, expected) -> None:
    assert format_g(f) == expected


@pytest.mark.parametrize("name", ["test", "order", "current", "select", "id", "updatedDate", "number"])
def test_quote_identifier(name) -> None:
    assert quote_identifier(name) == f"`{name}`"


@pytest.mark.parametrize(
    ("arg", "expected"),
    [
        ("test", "'test'"),
        ("test with a 'hi'", "'" + r"test with a \'hi\'" + "'"),
        (1, "1"),
        (1.1, "1.1"),
        (1.0, "1"),
        (True, "1"),
        (False, "0"),
        (None, "NULL"),
        ({"a": "b"}, "'" + r'{\"a\":\"b\"}' + "'"),
        ([{"a": "b"}], "'" + r'[{\"a\":\"b\"}]' + "'"),
        (datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "'2021-01-01 00:00:00'"),
        ("2024-07-09T18:28:03.69708Z", "'2024-07-09 18:28:03.69708'"),
        ("2024-07-09T18:28:03Z", "'2024-07-09 18:28:03'"),
        ("2024-07-09T18:28:03Z\n", "'" + r"2024-07-09T18:28:03Z\n" + "'"),  # trailing NL -> not coerced
        ("２０２４-07-09T18:28:03Z", "'２０２４-07-09T18:28:03Z'"),  # fullwidth -> not coerced
        (RawJson('{"x":1}'), "'" + r'{\"x\":1}' + "'"),
    ],
)
def test_quote_value(arg, expected) -> None:
    assert quote_value(arg) == expected


def test_quote_value_bad_timestamp_raises() -> None:
    with pytest.raises(ValueError, match="error parsing"):
        quote_value("9999-99-99T99:99:99Z")


def test_quote_value_pre_1970_timestamp_floored() -> None:
    # Go's proleptic Gregorian accepts year 0; both floor to the MySQL TIMESTAMP minimum.
    assert quote_value("0000-01-01T00:00:00Z") == "'1970-01-01 00:00:01'"
    assert quote_value("1969-12-31T23:59:59Z") == "'1970-01-01 00:00:01'"


def _order_schema() -> Schema:
    return Schema(
        table="order", model_version="v1", primary_keys=["id"], required=[],
        properties={"id": SchemaProperty(type="string"), "name": SchemaProperty(type="string")},
    )


def test_to_sql_delete() -> None:
    evt = DBChangeEvent(operation="DELETE", table="order", key=["abc"])
    assert to_sql(evt, _order_schema()) == "DELETE FROM `order` WHERE `id`='abc';\n"


def test_to_sql_replace_into() -> None:
    evt = DBChangeEvent(operation="INSERT", table="order", key=["o1"], after=RawJson('{"id":"o1","name":"Widget"}'))
    assert to_sql(evt, _order_schema()) == "REPLACE INTO `order` (`id`,`name`) VALUES ('o1','Widget');\n"


def test_to_sql_update_is_full_replace() -> None:
    # §8.2: an UPDATE still emits a full-column REPLACE — diff is ignored.
    evt = DBChangeEvent(
        operation="UPDATE", table="order", key=["o1"], diff=["name"], after=RawJson('{"id":"o1","name":"New"}')
    )
    assert to_sql(evt, _order_schema()) == "REPLACE INTO `order` (`id`,`name`) VALUES ('o1','New');\n"


def test_create_sql() -> None:
    assert create_sql(_order_schema()) == (
        "DROP TABLE IF EXISTS `order`;\n"
        "CREATE TABLE `order` (\n"
        "\t`id` VARCHAR(64),\n"
        "\t`name` TEXT,\n"
        "\tPRIMARY KEY (`id`)\n"
        ") CHARACTER SET=utf8mb4;\n"
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
    cols = ["number", "internalNumber", "externalNumber"]
    out = add_new_columns_sql(None, cols, _cols_schema(), DatabaseSchema())
    assert out == [
        "ALTER TABLE `order` ADD COLUMN `number` TEXT;",
        "ALTER TABLE `order` ADD COLUMN `internalNumber` TEXT;",
        "ALTER TABLE `order` ADD COLUMN `externalNumber` TEXT;",
    ]


def test_add_new_columns_sql_skips_existing() -> None:
    db = DatabaseSchema({"order": {"number": "TEXT"}})
    out = add_new_columns_sql(None, ["number", "externalNumber"], _cols_schema(), db)
    assert out == ["ALTER TABLE `order` ADD COLUMN `externalNumber` TEXT;"]
