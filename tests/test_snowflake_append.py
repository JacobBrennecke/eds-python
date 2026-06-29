"""FEATURE(audit-mode): Snowflake append/audit-trail golden vectors — byte-for-byte vs audit-mode.md §3.4.

DEVIATION from the literal §3.4b string: the UPDATE total is 24.5 (not "24.50"). format_f strips trailing
zeros (locked by quote_value(30.0)=="30"), and a parsed JSON float 24.50 == 24.5, so "24.50" is unreachable
by the (mandated) reuse of quote_value. Both ports produce 24.5; the §3.4b "24.50" is a doc typo.
"""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.drivers.snowflake import sql
from eds.drivers.snowflake.snowflake import SnowflakeDriver, plan_flush_append
from eds.schema import Schema, SchemaProperty
from eds.util.batcher import Record
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


def _rec(op, obj, mvcc, ts, key) -> Record:
    return Record(
        table="order", id=key[-1], operation=op, diff=None, object=obj,
        event=DBChangeEvent(operation=op, table="order", key=key, mvcc_timestamp=mvcc, timestamp=ts),
    )


def test_create_append_sql_golden() -> None:
    assert sql.create_append_sql(_order_schema()) == (
        'CREATE OR REPLACE TABLE "order" (\n'
        '\t"id" STRING NOT NULL,\n'
        # object → STRING: reuses prop_type_to_sql_type verbatim, matching the upsert table (NOT VARIANT).
        '\t"meta" STRING,\n'
        '\t"name" STRING,\n'
        '\t"total" FLOAT,\n'
        '\t"_eds_seq" NUMBER AUTOINCREMENT,\n'
        '\t"_eds_operation" STRING NOT NULL,\n'
        '\t"_eds_mvcc_timestamp" NUMBER(38,10),\n'
        '\t"_eds_timestamp" NUMBER,\n'
        '\t"_eds_appended_at" TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),\n'
        '\tPRIMARY KEY ("_eds_seq")\n'
        ');\n'
    )


def test_append_insert_golden() -> None:
    rec = _rec("INSERT", {"id": "o-1001", "meta": {"region": "emea"}, "name": "Widget", "total": 19.99},
               "1719500000.0000000000", 1719500000123, ["o-1001"])
    assert sql.to_append_sql(rec, _order_schema()) == (
        'INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp",'
        '"_eds_timestamp")\n'
        'SELECT \'o-1001\',PARSE_JSON(\'{"region":"emea"}\'),\'Widget\',19.99,\'INSERT\','
        '1719500000.0000000000,1719500000123;\n'
    )


def test_append_update_golden() -> None:
    # NOTE: total 24.5 (not §3.4b's "24.50" — trailing zero unreachable via the reused float formatter).
    rec = _rec("UPDATE", {"id": "o-1001", "meta": {"region": "emea"}, "name": "Widget", "total": 24.5},
               "1719500900.0000000000", 1719500900456, ["o-1001"])
    assert sql.to_append_sql(rec, _order_schema()) == (
        'INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp",'
        '"_eds_timestamp")\n'
        'SELECT \'o-1001\',PARSE_JSON(\'{"region":"emea"}\'),\'Widget\',24.5,\'UPDATE\','
        '1719500900.0000000000,1719500900456;\n'
    )


def test_append_delete_golden() -> None:
    rec = _rec("DELETE", {}, "1719501000.0000000000", 1719501000789, ["o-1001"])
    assert sql.to_append_sql(rec, _order_schema()) == (
        'INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp",'
        '"_eds_timestamp")\n'
        'SELECT \'o-1001\',NULL,NULL,NULL,\'DELETE\',1719501000.0000000000,1719501000789;\n'
    )


def test_append_delete_key_absent_falls_back_to_record_id() -> None:
    # M2: event.key empty → fall back to record.id for the PK value (no IndexError).
    rec = Record(
        table="order", id="o-1001", operation="DELETE", object=None,
        event=DBChangeEvent(operation="DELETE", table="order", key=None,
                            mvcc_timestamp="1719501000.0000000000", timestamp=1719501000789),
    )
    assert sql.to_append_sql(rec, _order_schema()) == (
        'INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp",'
        '"_eds_timestamp")\n'
        'SELECT \'o-1001\',NULL,NULL,NULL,\'DELETE\',1719501000.0000000000,1719501000789;\n'
    )


def test_current_view_golden() -> None:
    assert sql.create_current_view_sql(_order_schema()) == (
        'CREATE OR REPLACE VIEW "order_current" AS\n'
        'SELECT\n'
        '\t"id",\n'
        '\t"meta",\n'
        '\t"name",\n'
        '\t"total"\n'
        'FROM "order"\n'
        'QUALIFY ROW_NUMBER() OVER (\n'
        '\tPARTITION BY "id"\n'
        '\tORDER BY "_eds_mvcc_timestamp" DESC NULLS LAST, "_eds_timestamp" DESC, "_eds_seq" DESC\n'
        ') = 1\n'
        '\tAND "_eds_operation" <> \'DELETE\';\n'
    )


def test_timeline_view_golden() -> None:
    assert sql.create_timeline_view_sql(_order_schema()) == (
        'CREATE OR REPLACE VIEW "order_timeline" AS\n'
        'SELECT\n'
        '\t"id",\n'
        '\t"meta",\n'
        '\t"name",\n'
        '\t"total",\n'
        '\t"_eds_operation",\n'
        '\t"_eds_mvcc_timestamp" AS "valid_from",\n'
        '\tLEAD("_eds_mvcc_timestamp") OVER (\n'
        '\t\tPARTITION BY "id"\n'
        '\t\tORDER BY "_eds_mvcc_timestamp" ASC, "_eds_timestamp" ASC, "_eds_seq" ASC\n'
        '\t) AS "valid_to"\n'
        'FROM "order";\n'
    )


# ---- plan_flush_append: one INSERT...SELECT per record, statement_count == len(records), no dedup ----

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


def test_plan_flush_append_no_dedup_count_equals_records() -> None:
    obj = {"id": "o1", "meta": {"k": "v"}, "name": "W", "total": 1.0}
    # three changes for the SAME key — append keeps all three (upsert would combine to one)
    records = [
        _rec("INSERT", obj, "1.0000000000", 1, ["o1"]),
        _rec("UPDATE", obj, "2.0000000000", 2, ["o1"]),
        _rec("DELETE", {}, "3.0000000000", 3, ["o1"]),
    ]
    plan = plan_flush_append(records, _FakeRegistry(_order_schema()), _QuietLogger())
    assert plan.statement_count == 3  # == len(records), no combine
    assert plan.cache_keys == [] and plan.delete_keys == []  # no tracker bookkeeping in append
    assert plan.query.count("INSERT INTO") == 3
    assert "MERGE" not in plan.query and "DELETE FROM" not in plan.query  # plain INSERT...SELECT only


# ---- flush wiring with append mode ----

class _FakeSnowflakeDb:
    def __init__(self) -> None:
        self.last_sql = ""
        self.last_count = -1

    def query_single_value(self, fn): return "db"
    def build_schema(self, catalog, schema, fail_if_empty):
        from eds.schema import DatabaseSchema
        return DatabaseSchema()
    def exec_multi_statement(self, sql_text, statement_count):
        self.last_sql = sql_text
        self.last_count = statement_count
        return statement_count
    def exec(self, sql_text): ...
    def close(self): ...


class _FakeTracker:
    def __init__(self) -> None:
        self.set_keys_called = False
        self.delete_called = False

    def get_key(self, key): return False, ""
    def set_keys(self, keys, value, expires=0.0): self.set_keys_called = True
    def delete_key(self, *keys): self.delete_called = True
    def delete_keys_with_prefix(self, prefix): return 0


def test_flush_append_wiring() -> None:
    from eds.driver import IngestMode
    drv = SnowflakeDriver()
    drv._db = _FakeSnowflakeDb()
    drv._registry = _FakeRegistry(_order_schema())
    drv._tracker = _FakeTracker()
    drv._logger = _QuietLogger()
    drv._mode = IngestMode.APPEND  # FEATURE(audit-mode)
    log = _QuietLogger()
    drv.process(log, DBChangeEvent(
        operation="INSERT", table="order", key=["o1"], mvcc_timestamp="1.0000000000", timestamp=2,
        after=RawJson('{"id":"o1","meta":{"k":"v"},"name":"W","total":1.0}')))
    drv.process(log, DBChangeEvent(
        operation="UPDATE", table="order", key=["o1"], mvcc_timestamp="2.0000000000", timestamp=3,
        after=RawJson('{"id":"o1","meta":{"k":"v"},"name":"W2","total":2.0}')))
    drv.flush(log)
    assert drv._db.last_count == 2  # both rows kept (no combine)
    assert drv._db.last_sql.count("INSERT INTO") == 2
    assert "MERGE" not in drv._db.last_sql
    assert not drv._tracker.set_keys_called and not drv._tracker.delete_called  # no tracker writes in append


def test_migrate_new_table_append_emits_table_and_two_views() -> None:
    from eds.driver import IngestMode
    drv = SnowflakeDriver()
    execed: list[str] = []

    class _DB:
        def exec(self, s): execed.append(s)
        def build_schema(self, c, s, f):
            from eds.schema import DatabaseSchema
            return DatabaseSchema()
        def query_single_value(self, fn): return "db"
        def close(self): ...

    drv._db = _DB()
    drv._dbname = "db"
    drv._schema_name = "PUBLIC"
    drv._mode = IngestMode.APPEND
    drv.migrate_new_table(None, _QuietLogger(), _order_schema())
    assert len(execed) == 3
    assert execed[0].startswith('CREATE OR REPLACE TABLE "order"')
    assert execed[1].startswith('CREATE OR REPLACE VIEW "order_current"')
    assert execed[2].startswith('CREATE OR REPLACE VIEW "order_timeline"')
