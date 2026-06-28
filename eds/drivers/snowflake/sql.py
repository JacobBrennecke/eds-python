"""PARITY: internal/drivers/snowflake/sql.go — byte-critical Snowflake SQL generation (pure, golden).

Snowflake-distinct: double-quote identifiers; quote_string escapes ONLY '\\' and "'" (no control-char, no
double-quote escaping) and passes "NULL" through; floats use the 'f' verb; NO JSON-timestamp coercion (ISO
strings are emitted verbatim); the upsert is a MERGE gated on source.updatedDate > target.updatedDate; INSERT
values are wrapped with PARSE_JSON/TO_VARIANT per type.
"""

from __future__ import annotations

from datetime import datetime, timezone

from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util import gourl
from eds.util.batcher import Record
from eds.util.gofloat import format_f
from eds.util.gojson import stringify
from eds.util.logger import Logger
from eds.util.sql import quote_identifier


def quote_string(val: str, fn: str) -> str:
    """PARITY: quoteString — "NULL" passthrough; escape '\\' then "'"; optional wrap fn(...)."""
    if val == "NULL":
        return "NULL"
    escaped = val.replace("\\", "\\\\").replace("'", "''")
    res = "'" + escaped + "'"
    return f"{fn}({res})" if fn else res


def _quote_datetime(t: datetime, fn: str) -> str:
    if t.tzinfo is not None:
        t = t.astimezone(timezone.utc)
    s = t.strftime("%Y-%m-%d %H:%M:%S")
    if t.microsecond:
        s += "." + f"{t.microsecond:06d}".rstrip("0")
    res = "'" + s + "Z'"
    return f"{fn}({res})" if fn else res


def quote_value(value: object, fn: str) -> str:
    """PARITY: quoteValue. bool MUST precede int/float. NO timestamp coercion of strings (Snowflake-unique)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format_f(value)  # PARITY: 'f' verb
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return quote_string(value, fn)
    if isinstance(value, datetime):  # unreachable from JSON
        return _quote_datetime(value, fn)
    return quote_string(stringify(value), fn)  # dict / list / other


def generate_insert_function(prop: SchemaProperty) -> str:
    """PARITY: generateInsertFunction — the column-value wrapper for INSERT."""
    if prop.type == "object":
        return "PARSE_JSON"
    if prop.type == "array":
        if prop.items is not None and prop.items.type in ("object", "string"):
            return "PARSE_JSON"
        return "TO_VARIANT"
    return ""


def nullable_value(c: SchemaProperty, wrap: bool) -> str:
    """PARITY: nullableValue — the default for a missing column."""
    if c.nullable:
        return "NULL"
    if c.type == "object":
        return "PARSE_JSON('{}')" if wrap else "'{}'"
    if c.type == "array":
        return "PARSE_JSON('[]')" if wrap else "'[]'"
    if c.type in ("number", "integer"):
        return "0"
    if c.type == "boolean":
        return "false"
    return "''"


def to_delete_sql(record: Record) -> str:
    """PARITY: toDeleteSQL."""
    return (
        f"DELETE FROM {quote_identifier(record.table)} WHERE {quote_identifier('id')}="
        f"{quote_value(record.id, '')};\n"
    )


def to_merge_sql(record: Record, model: Schema) -> str:
    """PARITY: toMergeSQL — timestamp-gated MERGE upsert."""
    insert_columns: list[str] = []
    update_values: list[str] = []
    insert_vals: list[str] = []
    obj = record.object or {}
    for name in model.columns():
        prop = model.properties.get(name, SchemaProperty())
        insert_columns.append(quote_identifier(name))
        if name in obj:
            fn = generate_insert_function(prop)
            v = quote_value(obj[name], fn)
            update_values.append(f"{quote_identifier(name)}={v}")
            insert_vals.append(v)
        else:
            insert_vals.append(nullable_value(prop, True))

    after = (record.event.get_object() if record.event is not None else None) or {}
    updated_date = after.get("updatedDate")
    qi_id = quote_identifier("id")
    qi_ud = quote_identifier("updatedDate")
    return (
        f"MERGE INTO {quote_identifier(record.table)} AS target USING (SELECT "
        f"{quote_value(record.id, '')} AS {qi_id}, {quote_value(updated_date, '')} AS {qi_ud}) AS source "
        f"ON target.{qi_id} = source.{qi_id} "
        f"WHEN MATCHED AND source.{qi_ud} > target.{qi_ud} THEN UPDATE SET {','.join(update_values)} "
        f"WHEN NOT MATCHED THEN INSERT ({','.join(insert_columns)}) VALUES ({','.join(insert_vals)});\n"
    )


def to_sql(record: Record, model: Schema, exists: bool) -> tuple[str, int]:
    """PARITY: toSQL — delete-before-insert dedup + statement count."""
    sql = ""
    count = 0
    if exists or record.operation == "DELETE":
        sql += to_delete_sql(record)
        count += 1
    if record.operation != "DELETE":
        sql += to_merge_sql(record, model)
        count += 1
    return sql, count


def prop_type_to_sql_type(prop: SchemaProperty) -> str:
    """PARITY: propTypeToSQLType."""
    t = prop.type
    if t == "string":
        return "TIMESTAMP_NTZ" if prop.format == "date-time" else "STRING"
    if t == "integer":
        return "INTEGER"
    if t == "number":
        return "FLOAT"
    if t == "boolean":
        return "BOOLEAN"
    if t == "object":
        return "STRING"
    if t == "array":
        if prop.items is not None and prop.items.enum is not None:
            return "STRING"
        return "VARIANT"
    return "STRING"


def create_sql(s: Schema) -> str:
    """PARITY: createSQL — CREATE OR REPLACE; PK-first columns."""
    lines = []
    for name in s.columns():
        prop = s.properties.get(name, SchemaProperty())  # PARITY: Go map zero-value for a missing PK property
        not_null = " NOT NULL" if (name in s.required and not prop.nullable) else ""
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop) + not_null + ",\n")
    body = "".join(lines)
    if s.primary_keys:
        body += "\tPRIMARY KEY (" + ", ".join(quote_identifier(pk) for pk in s.primary_keys) + ")"
    return f"CREATE OR REPLACE TABLE {quote_identifier(s.table)} (\n{body}\n);\n"


def add_new_columns_sql(logger: Logger | None, columns: list[str], s: Schema, db: DatabaseSchema) -> list[str]:
    """PARITY: addNewColumnsSQL."""
    res: list[str] = []
    for column in columns:
        found, _ = db.get_type(s.table, column)
        if found:
            if logger is not None:
                logger.warn(
                    "skipping migration for column: %s for table: %s since it already exists", column, s.table
                )
            continue
        prop = s.properties.get(column, SchemaProperty())
        res.append(
            "ALTER TABLE " + quote_identifier(s.table) + " ADD COLUMN " + quote_identifier(column)
            + " " + prop_type_to_sql_type(prop) + ";"
        )
    return res


def get_connection_string_from_url(url_string: str) -> str:
    """PARITY: GetConnectionStringFromURL — gosnowflake DSN form (golden; the real connector uses kwargs)."""
    u = gourl.parse(url_string)
    out = ""
    if u.has_user_info and u.user is not None:
        out += str(u.user) + "@"  # Userinfo.String() (escaped)
    out += u.host
    if not u.path.startswith("/"):
        out += "/"
    out += u.path
    v = u.query()
    v.set("client_session_keep_alive", "true")
    v.set("application", "eds")
    return out + "?" + v.encode()
