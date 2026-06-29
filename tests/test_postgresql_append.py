"""FEATURE(audit-mode): PostgreSQL append/audit-trail golden vectors — byte-for-byte vs audit-mode.md §3.1.

These exact strings are the cross-port oracle (the eds-dotnet twin must emit the identical bytes).
"""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.drivers.postgresql import sql
from eds.schema import Schema, SchemaProperty
from eds.util.gojson import RawJson


def _order_schema() -> Schema:
    # §3: table `order`, primaryKeys=["id"], id(string,pk), name(string), total(number), meta(object).
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


def test_columns_order() -> None:
    assert _order_schema().columns() == ["id", "meta", "name", "total"]


def test_create_append_sql_golden() -> None:
    assert sql.create_append_sql(_order_schema()) == (
        'DROP TABLE IF EXISTS "order" CASCADE;\n'
        'CREATE TABLE "order" (\n'
        '\t"id" TEXT NOT NULL,\n'
        '\t"meta" JSONB,\n'
        '\t"name" TEXT,\n'
        '\t"total" DOUBLE PRECISION,\n'
        '\t"_eds_seq" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n'
        '\t"_eds_operation" TEXT NOT NULL,\n'
        '\t"_eds_mvcc_timestamp" NUMERIC(38,10),\n'
        '\t"_eds_timestamp" BIGINT NOT NULL,\n'
        '\t"_eds_appended_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()\n'
        ');\n'
        'CREATE INDEX "order__eds_history_idx" ON "order" '
        '("id", "_eds_mvcc_timestamp" DESC, "_eds_timestamp" DESC, "_eds_seq" DESC);\n'
    )


def test_append_insert_golden() -> None:
    evt = DBChangeEvent(
        operation="INSERT", table="order", key=["o_123"],
        mvcc_timestamp="1717009183239076000.0000000000", timestamp=1717009183239,
        after=RawJson('{"id":"o_123","meta":{"k":"v"},"name":"Widget","total":99.5}'),
    )
    assert sql.to_append_sql(evt, _order_schema()) == (
        'INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp",'
        '"_eds_timestamp") VALUES (\'o_123\',\'{"k":"v"}\',\'Widget\',99.5,\'INSERT\','
        '1717009183239076000.0000000000,1717009183239);\n'
    )


def test_append_update_golden() -> None:
    evt = DBChangeEvent(
        operation="UPDATE", table="order", key=["o_123"],
        mvcc_timestamp="1717009190000000000.0000000000", timestamp=1717009190000,
        after=RawJson('{"id":"o_123","meta":{"k":"v"},"name":"Widget","total":129}'),
    )
    assert sql.to_append_sql(evt, _order_schema()) == (
        'INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp",'
        '"_eds_timestamp") VALUES (\'o_123\',\'{"k":"v"}\',\'Widget\',129,\'UPDATE\','
        '1717009190000000000.0000000000,1717009190000);\n'
    )


def test_append_delete_golden() -> None:
    evt = DBChangeEvent(
        operation="DELETE", table="order", key=["o_123"],
        mvcc_timestamp="1717009200000000000.0000000000", timestamp=1717009200000,
    )
    assert sql.to_append_sql(evt, _order_schema()) == (
        'INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp",'
        '"_eds_timestamp") VALUES (\'o_123\',NULL,NULL,NULL,\'DELETE\','
        '1717009200000000000.0000000000,1717009200000);\n'
    )


def test_append_mvcc_falsy_guard_emits_null() -> None:
    # L1: "" or a JSON-null (None) mvccTimestamp → bare NULL (never None into ",".join).
    for mv in ("", None):
        evt = DBChangeEvent(
            operation="INSERT", table="order", key=["o1"], mvcc_timestamp=mv, timestamp=1,  # type: ignore[arg-type]
            after=RawJson('{"id":"o1"}'),
        )
        assert sql.to_append_sql(evt, _order_schema()).endswith(
            "VALUES ('o1',NULL,NULL,NULL,'INSERT',NULL,1);\n"
        )


def test_current_view_golden() -> None:
    assert sql.create_current_view_sql(_order_schema()) == (
        'CREATE OR REPLACE VIEW "order_current" AS\n'
        'SELECT "id","meta","name","total"\n'
        'FROM (\n'
        '\tSELECT DISTINCT ON ("id")\n'
        '\t\t"id","meta","name","total","_eds_operation"\n'
        '\tFROM "order"\n'
        '\tORDER BY "id", "_eds_mvcc_timestamp" DESC NULLS LAST, "_eds_timestamp" DESC, "_eds_seq" DESC\n'
        ') "latest"\n'
        'WHERE "_eds_operation" <> \'DELETE\';\n'
    )


def test_timeline_view_golden() -> None:
    assert sql.create_timeline_view_sql(_order_schema()) == (
        'CREATE OR REPLACE VIEW "order_timeline" AS\n'
        'SELECT\n'
        '\t"id","meta","name","total",\n'
        '\t"_eds_operation",\n'
        '\t"_eds_mvcc_timestamp" AS "valid_from",\n'
        '\tLEAD("_eds_mvcc_timestamp") OVER (\n'
        '\t\tPARTITION BY "id"\n'
        '\t\tORDER BY "_eds_mvcc_timestamp" ASC, "_eds_timestamp" ASC, "_eds_seq" ASC\n'
        '\t) AS "valid_to"\n'
        'FROM "order";\n'
    )


# ---- §3.5 composite-PK variant ----

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


def test_composite_columns_order() -> None:
    # PK columns first (declared order), then the rest lexicographically.
    assert _composite_schema().columns() == ["company_id", "id", "meta", "name", "total"]


def test_composite_create_append_sql_golden() -> None:
    out = sql.create_append_sql(_composite_schema())
    assert '\t"company_id" TEXT NOT NULL,\n\t"id" TEXT NOT NULL,\n' in out  # both keys NOT NULL
    assert '\t"meta" JSONB,\n' in out  # object cols nullable
    # index leads with all PK columns in declared order, joined no-space (cross-port w/ C#)
    assert ('CREATE INDEX "order__eds_history_idx" ON "order" '
            '("company_id","id", "_eds_mvcc_timestamp" DESC, "_eds_timestamp" DESC, "_eds_seq" DESC);\n') in out


def test_composite_current_view_golden() -> None:
    out = sql.create_current_view_sql(_composite_schema())
    assert '\tSELECT DISTINCT ON ("company_id","id")\n' in out
    assert ('\tORDER BY "company_id","id", "_eds_mvcc_timestamp" DESC NULLS LAST, '
            '"_eds_timestamp" DESC, "_eds_seq" DESC\n') in out


def test_composite_timeline_view_golden() -> None:
    out = sql.create_timeline_view_sql(_composite_schema())
    assert '\t\tPARTITION BY "company_id","id"\n' in out


def test_composite_delete_tombstone_fills_all_pks() -> None:
    evt = DBChangeEvent(
        operation="DELETE", table="order", key=["c_9", "o_123"],
        mvcc_timestamp="1.0000000000", timestamp=7,
    )
    assert sql.to_append_sql(evt, _composite_schema()) == (
        'INSERT INTO "order" ("company_id","id","meta","name","total","_eds_operation",'
        '"_eds_mvcc_timestamp","_eds_timestamp") VALUES '
        '(\'c_9\',\'o_123\',NULL,NULL,NULL,\'DELETE\',1.0000000000,7);\n'
    )


def test_drop_views_sql_golden() -> None:
    assert sql.drop_views_sql(_order_schema()) == [
        'DROP VIEW IF EXISTS "order_timeline";',
        'DROP VIEW IF EXISTS "order_current";',
    ]


# ---- SqlDriverBase append-branch wiring (fake DB seam) ----

class _FakeSqlDb:
    def __init__(self, schema=None) -> None:
        self.committed: list[str] = []
        self.executed: list[str] = []
        self.dropped: list[str] = []
        from eds.schema import DatabaseSchema
        self._schema = schema if schema is not None else DatabaseSchema()

    def query_single_value(self, fn): return "testdb"
    def build_schema(self, logger, column, value, fail_if_empty, conditions): return self._schema
    def execute_in_transaction(self, sql_text, logger): self.committed.append(sql_text)
    def exec(self, sql_text): self.executed.append(sql_text)
    def drop_table(self, quoted_table): self.dropped.append(quoted_table)
    def create_import_executor(self, dry_run): return self.committed.append
    def close(self): ...


class _FakeRegistry:
    def __init__(self, schema: Schema) -> None: self._schema = schema
    def get_table_version(self, table): return True, "v1"
    def get_schema(self, table, version): return self._schema
    def get_latest_schema(self): return {self._schema.table: self._schema}


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def _append_driver():
    from eds.driver import IngestMode
    from eds.drivers.postgresql.driver import PostgresqlDriver
    drv = PostgresqlDriver()
    drv._db = _FakeSqlDb()
    drv._registry = _FakeRegistry(_order_schema())
    drv._logger = _QuietLogger()
    drv._dbname = "testdb"
    drv._mode = IngestMode.APPEND  # FEATURE(audit-mode)
    return drv


def test_process_buffers_append_insert_not_upsert() -> None:
    drv = _append_driver()
    drv.process(_QuietLogger(), DBChangeEvent(
        operation="INSERT", table="order", key=["o1"], mvcc_timestamp="1.0000000000", timestamp=2,
        after=RawJson('{"id":"o1","meta":{"k":"v"},"name":"W","total":1.0}')))
    assert drv._count == 1
    buffered = drv._pending[0]
    assert buffered.startswith('INSERT INTO "order"')
    assert "ON CONFLICT" not in buffered  # append never upserts
    assert "_eds_operation" in buffered


def test_migrate_new_table_append_emits_table_and_two_views() -> None:
    drv = _append_driver()
    drv.migrate_new_table(None, _QuietLogger(), _order_schema())
    assert len(drv._db.executed) == 3
    assert drv._db.executed[0].startswith('DROP TABLE IF EXISTS "order" CASCADE;')
    assert drv._db.executed[1].startswith('CREATE OR REPLACE VIEW "order_current"')
    assert drv._db.executed[2].startswith('CREATE OR REPLACE VIEW "order_timeline"')
    assert drv._db.dropped == []  # the DDL block self-drops via CASCADE; no separate drop_table call


def test_migrate_new_columns_append_drops_then_recreates_views() -> None:
    drv = _append_driver()
    drv.migrate_new_columns(None, _QuietLogger(), _order_schema(), ["newcol"])
    ex = drv._db.executed
    assert ex[0] == 'DROP VIEW IF EXISTS "order_timeline";'
    assert ex[1] == 'DROP VIEW IF EXISTS "order_current";'
    assert any(s.startswith('ALTER TABLE "order" ADD COLUMN "newcol"') for s in ex)
    assert ex[-2].startswith('CREATE OR REPLACE VIEW "order_current"')
    assert ex[-1].startswith('CREATE OR REPLACE VIEW "order_timeline"')
