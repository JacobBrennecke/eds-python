"""FEATURE(audit-mode): MySQL append/audit-trail golden vectors — byte-for-byte vs audit-mode.md §3.2."""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.drivers.mysql import sql
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
        "DROP VIEW IF EXISTS `order_timeline`;\n"
        "DROP VIEW IF EXISTS `order_current`;\n"
        "DROP TABLE IF EXISTS `order`;\n"
        "CREATE TABLE `order` (\n"
        "\t`id` VARCHAR(64) NOT NULL,\n"
        "\t`meta` JSON,\n"
        "\t`name` TEXT,\n"
        "\t`total` FLOAT,\n"
        "\t`_eds_seq` BIGINT NOT NULL AUTO_INCREMENT,\n"
        "\t`_eds_operation` VARCHAR(16) NOT NULL,\n"
        "\t`_eds_mvcc_timestamp` DECIMAL(38,10),\n"
        "\t`_eds_timestamp` BIGINT,\n"
        "\t`_eds_appended_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
        "\tPRIMARY KEY (`_eds_seq`),\n"
        "\tKEY `order_eds_history_idx` (`id`, `_eds_mvcc_timestamp` DESC, `_eds_timestamp` DESC, `_eds_seq` DESC)\n"
        ") CHARACTER SET=utf8mb4;\n"
    )


def test_append_inserts_golden() -> None:
    ins = DBChangeEvent(
        operation="INSERT", table="order", key=["ord_1"],
        mvcc_timestamp="1735689600000123000.0000000000", timestamp=1735689600000123,
        after=RawJson('{"id":"ord_1","meta":{"currency":"USD"},"name":"Widget","total":19.99}'),
    )
    assert sql.to_append_sql(ins, _order_schema()) == (
        "INSERT INTO `order` (`id`,`meta`,`name`,`total`,`_eds_operation`,`_eds_mvcc_timestamp`,"
        "`_eds_timestamp`) VALUES ('ord_1','{\\\"currency\\\":\\\"USD\\\"}','Widget',19.99,'INSERT',"
        "1735689600000123000.0000000000,1735689600000123);\n"
    )

    upd = DBChangeEvent(
        operation="UPDATE", table="order", key=["ord_1"],
        mvcc_timestamp="1735689700000456000.0000000000", timestamp=1735689700000456,
        after=RawJson('{"id":"ord_1","meta":{"currency":"USD","tier":"pro"},"name":"Widget Pro","total":29.5}'),
    )
    assert sql.to_append_sql(upd, _order_schema()) == (
        "INSERT INTO `order` (`id`,`meta`,`name`,`total`,`_eds_operation`,`_eds_mvcc_timestamp`,"
        "`_eds_timestamp`) VALUES ('ord_1','{\\\"currency\\\":\\\"USD\\\",\\\"tier\\\":\\\"pro\\\"}',"
        "'Widget Pro',29.5,'UPDATE',1735689700000456000.0000000000,1735689700000456);\n"
    )

    dele = DBChangeEvent(
        operation="DELETE", table="order", key=["ord_1"],
        mvcc_timestamp="1735689800000789000.0000000000", timestamp=1735689800000789,
    )
    assert sql.to_append_sql(dele, _order_schema()) == (
        "INSERT INTO `order` (`id`,`meta`,`name`,`total`,`_eds_operation`,`_eds_mvcc_timestamp`,"
        "`_eds_timestamp`) VALUES ('ord_1',NULL,NULL,NULL,'DELETE',1735689800000789000.0000000000,"
        "1735689800000789);\n"
    )


def test_current_view_golden() -> None:
    assert sql.create_current_view_sql(_order_schema()) == (
        "CREATE VIEW `order_current` AS\n"
        "SELECT `id`, `meta`, `name`, `total`\n"
        "FROM (\n"
        "\tSELECT `id`, `meta`, `name`, `total`, `_eds_operation`,\n"
        "\t\tROW_NUMBER() OVER (\n"
        "\t\t\tPARTITION BY `id`\n"
        "\t\t\tORDER BY `_eds_mvcc_timestamp` DESC, `_eds_timestamp` DESC, `_eds_seq` DESC\n"
        "\t\t) AS `_eds_rn`\n"
        "\tFROM `order`\n"
        ") AS `_eds_ranked`\n"
        "WHERE `_eds_rn` = 1 AND `_eds_operation` <> 'DELETE';\n"
    )


def test_timeline_view_golden() -> None:
    assert sql.create_timeline_view_sql(_order_schema()) == (
        "CREATE VIEW `order_timeline` AS\n"
        "SELECT `id`, `meta`, `name`, `total`, `_eds_operation`,\n"
        "\t`_eds_mvcc_timestamp` AS `valid_from`,\n"
        "\tLEAD(`_eds_mvcc_timestamp`) OVER (\n"
        "\t\tPARTITION BY `id`\n"
        "\t\tORDER BY `_eds_mvcc_timestamp` ASC, `_eds_timestamp` ASC, `_eds_seq` ASC\n"
        "\t) AS `valid_to`\n"
        "FROM `order`;\n"
    )


def test_drop_views_sql_golden() -> None:
    assert sql.drop_views_sql(_order_schema()) == [
        "DROP VIEW IF EXISTS `order_timeline`;",
        "DROP VIEW IF EXISTS `order_current`;",
    ]


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
    assert "\t`company_id` VARCHAR(64) NOT NULL,\n\t`id` VARCHAR(64) NOT NULL,\n" in out  # both keys NOT NULL
    assert ("\tKEY `order_eds_history_idx` "
            "(`company_id`,`id`, `_eds_mvcc_timestamp` DESC, `_eds_timestamp` DESC, `_eds_seq` DESC)") in out


def test_composite_current_view_partition_no_space() -> None:
    assert "\t\t\tPARTITION BY `company_id`,`id`\n" in sql.create_current_view_sql(_composite_schema())


def test_composite_timeline_view_partition_no_space() -> None:
    assert "\t\tPARTITION BY `company_id`,`id`\n" in sql.create_timeline_view_sql(_composite_schema())
