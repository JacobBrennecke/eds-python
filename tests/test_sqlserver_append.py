"""FEATURE(audit-mode): SQL Server append/audit-trail golden vectors — byte-for-byte vs audit-mode.md §3.3.

Each statement is its own Exec (no GO); CREATE TABLE / CREATE INDEX / CREATE VIEW carry no trailing ';'.
"""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.drivers.sqlserver import sql
from eds.schema import Schema, SchemaProperty
from eds.util.gojson import RawJson


def _order_schema() -> Schema:
    return Schema(
        table="order",
        primary_keys=["id"],
        properties={
            "id": SchemaProperty(type="string"),
            "name": SchemaProperty(type="string"),
            "total": SchemaProperty(type="number"),
            "meta": SchemaProperty(type="object"),
        },
    )


def test_create_append_sql_golden() -> None:
    assert sql.create_append_sql(_order_schema()) == (
        "DROP VIEW IF EXISTS [order_timeline];\n"
        "DROP VIEW IF EXISTS [order_current];\n"
        "DROP TABLE IF EXISTS [order];\n"
        "CREATE TABLE [order] (\n"
        "\t[id] VARCHAR(64) NOT NULL,\n"
        "\t[meta] NVARCHAR(MAX),\n"
        "\t[name] NVARCHAR(MAX),\n"
        "\t[total] FLOAT,\n"
        "\t[_eds_seq] BIGINT IDENTITY(1,1) PRIMARY KEY,\n"
        "\t[_eds_operation] NVARCHAR(16) NOT NULL,\n"
        "\t[_eds_mvcc_timestamp] DECIMAL(38,10),\n"
        "\t[_eds_timestamp] BIGINT,\n"
        "\t[_eds_appended_at] DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()\n"
        ")\n"
        "CREATE INDEX [ix_order_id_mvcc] ON [order] "
        "([id], [_eds_mvcc_timestamp] DESC, [_eds_timestamp] DESC, [_eds_seq] DESC)"
    )


def test_append_inserts_golden() -> None:
    ins = DBChangeEvent(
        operation="INSERT", table="order", key=["1234"],
        mvcc_timestamp="1719158400000000000.0000000000", timestamp=1719158400000,
        after=RawJson('{"id":"1234","meta":{"color":"red"},"name":"Widget","total":19.99}'),
    )
    assert sql.to_append_sql(ins, _order_schema()) == (
        "INSERT INTO [order] ([id],[meta],[name],[total],[_eds_operation],[_eds_mvcc_timestamp],"
        "[_eds_timestamp]) VALUES ('1234','{\\\"color\\\":\\\"red\\\"}','Widget',19.99,'INSERT',"
        "1719158400000000000.0000000000,1719158400000);\n"
    )

    upd = DBChangeEvent(
        operation="UPDATE", table="order", key=["1234"],
        mvcc_timestamp="1719162000000000000.0000000000", timestamp=1719162000000,
        after=RawJson('{"id":"1234","meta":{"color":"blue"},"name":"Widget","total":24.5}'),
    )
    assert sql.to_append_sql(upd, _order_schema()) == (
        "INSERT INTO [order] ([id],[meta],[name],[total],[_eds_operation],[_eds_mvcc_timestamp],"
        "[_eds_timestamp]) VALUES ('1234','{\\\"color\\\":\\\"blue\\\"}','Widget',24.5,'UPDATE',"
        "1719162000000000000.0000000000,1719162000000);\n"
    )

    dele = DBChangeEvent(
        operation="DELETE", table="order", key=["1234"],
        mvcc_timestamp="1719165600000000000.0000000000", timestamp=1719165600000,
    )
    assert sql.to_append_sql(dele, _order_schema()) == (
        "INSERT INTO [order] ([id],[meta],[name],[total],[_eds_operation],[_eds_mvcc_timestamp],"
        "[_eds_timestamp]) VALUES ('1234',NULL,NULL,NULL,'DELETE',1719165600000000000.0000000000,"
        "1719165600000);\n"
    )


def test_current_view_golden() -> None:
    assert sql.create_current_view_sql(_order_schema()) == (
        "CREATE VIEW [order_current] AS\n"
        "SELECT [id],[meta],[name],[total]\n"
        "FROM (\n"
        "\tSELECT [id],[meta],[name],[total],[_eds_operation],\n"
        "\t\tROW_NUMBER() OVER (\n"
        "\t\t\tPARTITION BY [id]\n"
        "\t\t\tORDER BY [_eds_mvcc_timestamp] DESC, [_eds_timestamp] DESC, [_eds_seq] DESC\n"
        "\t\t) AS _eds_rn\n"
        "\tFROM [order]\n"
        ") AS ranked\n"
        "WHERE _eds_rn = 1 AND [_eds_operation] <> 'DELETE'"
    )


def test_timeline_view_golden() -> None:
    assert sql.create_timeline_view_sql(_order_schema()) == (
        "CREATE VIEW [order_timeline] AS\n"
        "SELECT [id],[meta],[name],[total],[_eds_operation],\n"
        "\t[_eds_mvcc_timestamp] AS valid_from,\n"
        "\tLEAD([_eds_mvcc_timestamp]) OVER (\n"
        "\t\tPARTITION BY [id]\n"
        "\t\tORDER BY [_eds_mvcc_timestamp] ASC, [_eds_timestamp] ASC, [_eds_seq] ASC\n"
        "\t) AS valid_to\n"
        "FROM [order]"
    )


def test_drop_views_sql_golden() -> None:
    assert sql.drop_views_sql(_order_schema()) == [
        "DROP VIEW IF EXISTS [order_timeline];",
        "DROP VIEW IF EXISTS [order_current];",
    ]


# ---- H3: present-null int/bool → NULL (NO handle_schema_property in append) ----

def _typed_schema() -> Schema:
    return Schema(
        table="order",
        primary_keys=["id"],
        properties={
            "id": SchemaProperty(type="string"),
            "active": SchemaProperty(type="boolean"),
            "count": SchemaProperty(type="integer"),
        },
    )


def test_append_present_null_int_and_bool_emit_null() -> None:
    # columns() = ["id","active","count"]. A present JSON null for int/bool must emit NULL (not coerced to 0).
    evt = DBChangeEvent(
        operation="INSERT", table="order", key=["1234"],
        mvcc_timestamp="1.0000000000", timestamp=7,
        after=RawJson('{"id":"1234","active":null,"count":null}'),
    )
    assert sql.to_append_sql(evt, _typed_schema()) == (
        "INSERT INTO [order] ([id],[active],[count],[_eds_operation],[_eds_mvcc_timestamp],[_eds_timestamp]) "
        "VALUES ('1234',NULL,NULL,'INSERT',1.0000000000,7);\n"
    )


def test_append_present_nonnull_int_and_bool_unchanged() -> None:
    # non-null int/bool still render byte-identically to the pre-fix behavior (bool→1, int as-is).
    evt = DBChangeEvent(
        operation="INSERT", table="order", key=["1234"],
        mvcc_timestamp="1.0000000000", timestamp=7,
        after=RawJson('{"id":"1234","active":true,"count":5}'),
    )
    assert sql.to_append_sql(evt, _typed_schema()) == (
        "INSERT INTO [order] ([id],[active],[count],[_eds_operation],[_eds_mvcc_timestamp],[_eds_timestamp]) "
        "VALUES ('1234',1,5,'INSERT',1.0000000000,7);\n"
    )


# ---- item 7: composite-PK (no-space PK join) ----

def _composite_schema() -> Schema:
    return Schema(
        table="order",
        primary_keys=["company_id", "id"],
        properties={
            "company_id": SchemaProperty(type="string"),
            "id": SchemaProperty(type="string"),
            "name": SchemaProperty(type="string"),
            "total": SchemaProperty(type="number"),
            "meta": SchemaProperty(type="object"),
        },
    )


def test_composite_create_append_sql_golden() -> None:
    out = sql.create_append_sql(_composite_schema())
    assert "\t[company_id] VARCHAR(64) NOT NULL,\n\t[id] VARCHAR(64) NOT NULL,\n" in out  # both keys NOT NULL
    assert ("CREATE INDEX [ix_order_id_mvcc] ON [order] "
            "([company_id],[id], [_eds_mvcc_timestamp] DESC, [_eds_timestamp] DESC, [_eds_seq] DESC)") in out


def test_composite_current_view_partition_no_space() -> None:
    assert "\t\t\tPARTITION BY [company_id],[id]\n" in sql.create_current_view_sql(_composite_schema())


def test_composite_timeline_view_partition_no_space() -> None:
    assert "\t\tPARTITION BY [company_id],[id]\n" in sql.create_timeline_view_sql(_composite_schema())
