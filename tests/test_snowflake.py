"""PARITY: internal/drivers/snowflake — SQL gen, RecordOptimize, plan_flush, key-pair (Go + C# vectors)."""

from __future__ import annotations

import pytest

from eds.dbchange import DBChangeEvent
from eds.drivers.snowflake import sql
from eds.drivers.snowflake.snowflake import SnowflakeDriver, plan_flush
from eds.drivers.snowflake.snowflake_keypair import SnowflakeKeyPairDriver, parse_key_pair_url
from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util.batcher import Record
from eds.util.gojson import RawJson
from eds.util.optimize import combine_records_with_same_primary_key, sort_records_by_mvcc_timestamp


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


# ---- SQL generation ----

@pytest.mark.parametrize(
    ("arg", "fn", "expected"),
    [
        ("simple string", "", "'simple string'"),
        ("Hi Mike,\nThis is a friendly reminder.", "", "'Hi Mike,\nThis is a friendly reminder.'"),
        ("Hello 'world'!", "", "'Hello ''world''!'"),
        ("'" * 4 + "simple string" + "'" * 4, "", "'" + "'" * 8 + "simple string" + "'" * 8 + "'"),
        ("Line1\tLine2\rLine3", "", "'Line1\tLine2\rLine3'"),
        (None, "", "NULL"),
        ("test string", "UPPER", "UPPER('test string')"),
        (True, "", "true"),
        (False, "", "false"),
        (1.1, "", "1.1"),
        ({"a": "b"}, "", "'" + '{"a":"b"}' + "'"),  # only \ and ' escaped, NOT "
        ("2024-07-11T21:16:51.70856Z", "", "'2024-07-11T21:16:51.70856Z'"),  # NO timestamp coercion
    ],
)
def test_quote_value(arg, fn, expected) -> None:
    assert sql.quote_value(arg, fn) == expected


@pytest.mark.parametrize(
    ("prop", "expected"),
    [
        (SchemaProperty(type="object"), "PARSE_JSON"),
        (SchemaProperty(type="array", items=None), "TO_VARIANT"),
        (SchemaProperty(type="string"), ""),
    ],
)
def test_generate_insert_function(prop, expected) -> None:
    assert sql.generate_insert_function(prop) == expected


def test_generate_insert_function_array_items() -> None:
    from eds.schema import ItemsType
    assert sql.generate_insert_function(SchemaProperty(type="array", items=ItemsType(type="object"))) == "PARSE_JSON"
    assert sql.generate_insert_function(SchemaProperty(type="array", items=ItemsType(type="string"))) == "PARSE_JSON"
    assert sql.generate_insert_function(SchemaProperty(type="array", items=ItemsType(type="number"))) == "TO_VARIANT"


@pytest.mark.parametrize(
    ("prop", "expected"),
    [
        (SchemaProperty(type="string", nullable=True), "NULL"),
        (SchemaProperty(type="object"), "PARSE_JSON('{}')"),
        (SchemaProperty(type="array"), "PARSE_JSON('[]')"),
        (SchemaProperty(type="integer"), "0"),
        (SchemaProperty(type="boolean"), "false"),
        (SchemaProperty(type="string"), "''"),
    ],
)
def test_nullable_value(prop, expected) -> None:
    assert sql.nullable_value(prop, True) == expected


def _merge_schema() -> Schema:
    return Schema(
        table="order", model_version="v1", primary_keys=[],
        properties={
            "id": SchemaProperty(type="string"), "updatedDate": SchemaProperty(type="string"),
            "firstName": SchemaProperty(type="string"),
        },
    )


def test_to_merge_sql_golden() -> None:
    after = '{"id":"1","updatedDate":"2024-07-11T21:16:51.70856Z","firstName":"Jim"}'
    record = Record(
        table="order", id="1", operation="UPDATE", diff=None,
        object={"id": "1", "updatedDate": "2024-07-11T21:16:51.70856Z", "firstName": "Jim"},
        event=DBChangeEvent(operation="UPDATE", table="order", after=RawJson(after)),
    )
    assert sql.to_merge_sql(record, _merge_schema()) == (
        'MERGE INTO "order" AS target USING (SELECT \'1\' AS "id", '
        '\'2024-07-11T21:16:51.70856Z\' AS "updatedDate") AS source ON target."id" = source."id" '
        'WHEN MATCHED AND source."updatedDate" > target."updatedDate" THEN UPDATE SET '
        '"firstName"=\'Jim\',"id"=\'1\',"updatedDate"=\'2024-07-11T21:16:51.70856Z\' '
        'WHEN NOT MATCHED THEN INSERT ("firstName","id","updatedDate") '
        'VALUES (\'Jim\',\'1\',\'2024-07-11T21:16:51.70856Z\');\n'
    )


def test_to_delete_sql() -> None:
    rec = Record(table="order", id="abc", operation="DELETE")
    assert sql.to_delete_sql(rec) == 'DELETE FROM "order" WHERE "id"=\'abc\';\n'


def test_to_sql_count_and_delete_before_insert() -> None:
    rec = Record(
        table="order", id="1", operation="INSERT", object={"id": "1", "updatedDate": "x", "firstName": "J"},
        event=DBChangeEvent(operation="INSERT", table="order",
                            after=RawJson('{"id":"1","updatedDate":"x","firstName":"J"}')),
    )
    s, c = sql.to_sql(rec, _merge_schema(), exists=False)
    assert c == 1 and 'MERGE INTO "order"' in s and "DELETE FROM" not in s
    s, c = sql.to_sql(rec, _merge_schema(), exists=True)
    assert c == 2 and s.startswith('DELETE FROM "order" WHERE "id"=\'1\';') and 'MERGE INTO "order"' in s
    d = Record(table="order", id="1", operation="DELETE")
    s, c = sql.to_sql(d, _merge_schema(), exists=False)
    assert c == 1 and s == 'DELETE FROM "order" WHERE "id"=\'1\';\n'


def test_create_sql() -> None:
    s = Schema(table="order", primary_keys=["id"], required=["id"],
               properties={"id": SchemaProperty(type="string"), "name": SchemaProperty(type="string")})
    assert sql.create_sql(s) == (
        'CREATE OR REPLACE TABLE "order" (\n'
        '\t"id" STRING NOT NULL,\n'
        '\t"name" STRING,\n'
        '\tPRIMARY KEY ("id")\n'
        ");\n"
    )


def test_add_new_columns_sql() -> None:
    s = Schema(table="order", primary_keys=["id"],
               properties={"id": SchemaProperty(type="string"), "number": SchemaProperty(type="string")})
    out = sql.add_new_columns_sql(None, ["number", "internalNumber"], s, DatabaseSchema())
    assert out == [
        'ALTER TABLE "order" ADD COLUMN "number" STRING;',
        'ALTER TABLE "order" ADD COLUMN "internalNumber" STRING;',
    ]


def test_get_connection_string_from_url() -> None:
    assert sql.get_connection_string_from_url("snowflake://user:password@account/db?foo=bar") == (
        "user:password@account/db?application=eds&client_session_keep_alive=true&foo=bar"
    )


# ---- RecordOptimize ----

def _rec(rid, op, mvcc="0", diff=None, obj=None) -> Record:
    return Record(
        table="order", id=rid, operation=op, diff=diff, object=obj or {},
        event=DBChangeEvent(operation=op, table="order", mvcc_timestamp=mvcc),
    )


def test_sort_by_mvcc() -> None:
    recs = [_rec("a", "INSERT", "3"), _rec("b", "INSERT", "1"), _rec("c", "INSERT", "2")]
    assert [r.id for r in sort_records_by_mvcc_timestamp(recs)] == ["b", "c", "a"]


def test_combine_delete_wins() -> None:
    recs = [
        _rec("1", "UPDATE", diff=["a"], obj={"a": 1}),
        _rec("1", "UPDATE", diff=["b"], obj={"b": 2}),
        _rec("1", "DELETE"),
        _rec("2", "INSERT", obj={"x": 1}),
    ]
    out = combine_records_with_same_primary_key(recs)
    assert [(r.id, r.operation) for r in out] == [("1", "DELETE"), ("2", "INSERT")]


def test_combine_updates_merge() -> None:
    recs = [_rec("1", "UPDATE", diff=["a"], obj={"a": 1}), _rec("1", "UPDATE", diff=["b"], obj={"b": 2})]
    out = combine_records_with_same_primary_key(recs)
    assert len(out) == 1
    assert out[0].diff == ["a", "b"]
    assert out[0].object == {"a": 1, "b": 2}


# ---- plan_flush ----

class _FakeRegistry:
    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    def get_table_version(self, table):
        return True, "v1"

    def get_schema(self, table, version):
        return self._schema

    def get_latest_schema(self):
        return {self._schema.table: self._schema}


def _pf_schema() -> Schema:
    return Schema(table="order", primary_keys=[],
                  properties={"id": SchemaProperty(type="string"), "updatedDate": SchemaProperty(type="string")})


def _pf_rec(rid, op, diff=None) -> Record:
    obj = {"id": rid, "updatedDate": "2024-01-01T00:00:00Z"}
    return Record(table="order", id=rid, operation=op, diff=diff, object=obj,
                  event=DBChangeEvent(operation=op, table="order",
                                      after=RawJson(f'{{"id":"{rid}","updatedDate":"2024-01-01T00:00:00Z"}}')))


def test_plan_flush() -> None:
    seen = {"snowflake:order:2"}
    records = [
        _pf_rec("1", "INSERT"),
        _pf_rec("2", "INSERT"),
        _pf_rec("3", "UPDATE", ["updatedDate"]),
        _pf_rec("3", "UPDATE", ["updatedDate", "meta"]),
        _pf_rec("3", "UPDATE", []),
        _pf_rec("4", "UPDATE", ["name"]),
        _pf_rec("5", "DELETE"),
    ]
    plan = plan_flush(records, _FakeRegistry(_pf_schema()), lambda k: k in seen, _QuietLogger())
    assert plan.statement_count == 5  # 1 + 2 + 0 + 0 + 0 + 1 + 1
    assert plan.cache_keys == ["snowflake:order:1", "snowflake:order:2", "snowflake:order:5"]
    assert plan.delete_keys == ["snowflake:order:5"]
    assert 'DELETE FROM "order" WHERE "id"=\'2\';' in plan.query  # forced delete-before-insert
    assert 'DELETE FROM "order" WHERE "id"=\'5\';' in plan.query


# ---- key pair ----

def test_parse_key_pair_url() -> None:
    assert parse_key_pair_url("snowflake-keypair://bob@org-acct/MYDB/PUBLIC?secret-key=MY_SECRET_VAR") == (
        "bob", "org-acct", "MYDB", "PUBLIC", "MY_SECRET_VAR"
    )


def test_parse_key_pair_url_missing_schema_raises() -> None:
    with pytest.raises(ValueError, match="invalid URL path"):
        parse_key_pair_url("snowflake-keypair://bob@org-acct/MYDB?secret-key=X")


def test_keypair_validate() -> None:
    url, errors = SnowflakeKeyPairDriver().validate(
        {"Account": "org-acct", "Database": "MYDB/PUBLIC", "Username": "bob", "Secret": "MY_SECRET_VAR"}
    )
    assert errors == []
    assert url == "snowflake-keypair://bob@org-acct/MYDB/PUBLIC?secret-key=MY_SECRET_VAR"


def test_keypair_validate_missing_required() -> None:
    url, errors = SnowflakeKeyPairDriver().validate({"Account": "org-acct"})
    assert url == ""
    assert errors  # Database + Username missing


# ---- flush wiring (fake seam + tracker) ----

class _FakeSnowflakeDb:
    def __init__(self) -> None:
        self.last_sql = ""
        self.last_count = -1

    def query_single_value(self, fn):
        return "db"

    def build_schema(self, catalog, schema, fail_if_empty):
        return DatabaseSchema()

    def exec_multi_statement(self, sql_text, statement_count):
        self.last_sql = sql_text
        self.last_count = statement_count
        return statement_count

    def exec(self, sql_text): ...
    def close(self): ...


class _FakeTracker:
    def __init__(self) -> None:
        self.set_keys_args = None
        self.deleted = None

    def get_key(self, key):
        return False, ""

    def set_keys(self, keys, value, expires=0.0):
        self.set_keys_args = (list(keys), value, expires)

    def delete_key(self, *keys):
        self.deleted = list(keys)

    def delete_keys_with_prefix(self, prefix):
        return 0


def test_flush_wiring() -> None:
    driver = SnowflakeDriver()
    driver._db = _FakeSnowflakeDb()
    driver._registry = _FakeRegistry(_pf_schema())
    driver._tracker = _FakeTracker()
    driver._logger = _QuietLogger()
    log = _QuietLogger()
    driver.process(log, DBChangeEvent(
        operation="INSERT", table="order", key=["o1"], mvcc_timestamp="1",
        after=RawJson('{"id":"o1","updatedDate":"2024-01-01T00:00:00Z"}')))
    driver.process(log, DBChangeEvent(
        operation="DELETE", table="order", key=["o9"], mvcc_timestamp="2",
        after=RawJson('{"id":"o9"}')))
    driver.flush(log)

    assert driver._db.last_count == 2
    assert 'MERGE INTO "order"' in driver._db.last_sql
    assert 'DELETE FROM "order" WHERE "id"=\'o9\';' in driver._db.last_sql
    keys, _value, expires = driver._tracker.set_keys_args
    assert keys == ["snowflake:order:o1", "snowflake:order:o9"]
    assert expires == 24 * 3600.0
    assert driver._tracker.deleted == ["snowflake:order:o9"]
