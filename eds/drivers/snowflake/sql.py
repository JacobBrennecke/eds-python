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


# ===================================================================================================
# FEATURE(audit-mode): append / audit-trail SQL — NOT a Go port. See migration/features/audit-mode.md §3.4.
# Strictly additive: the MERGE builders above are untouched. Snowflake uses INSERT … SELECT (PARSE_JSON /
# TO_VARIANT are illegal in a VALUES list) and QUALIFY for the latest-per-object view; no secondary index.
# ===================================================================================================

_EDS_SEQ = "_eds_seq"
_EDS_OPERATION = "_eds_operation"
_EDS_MVCC = "_eds_mvcc_timestamp"
_EDS_TIMESTAMP = "_eds_timestamp"
_EDS_APPENDED_AT = "_eds_appended_at"


def to_append_sql(record: Record, model: Schema) -> str:
    """FEATURE(audit-mode): one plain INSERT … SELECT per change (NO MERGE). INSERT/UPDATE emit the full
    after-snapshot (object values reuse quote_value + generate_insert_function, so PARSE_JSON/TO_VARIANT match
    the upsert); DELETE emits a tombstone (PK value(s) + NULLs). mvcc is a BARE numeric literal (empty → NULL);
    _eds_seq + _eds_appended_at are DB-generated (never in the column list). §3.4."""
    columns = model.columns()
    vals: list[str] = []
    if record.operation == "DELETE":
        keys = (record.event.key if record.event is not None else None) or []
        for name in columns:
            if name in model.primary_keys:
                # FEATURE(audit-mode): fall back to record.id when event.key is empty/short (matches C#),
                # instead of hard-indexing (which would IndexError).
                i = model.primary_keys.index(name)
                key_val = keys[i] if i < len(keys) else record.id
                vals.append(quote_value(key_val, ""))
            else:
                vals.append("NULL")
    else:
        obj = record.object or {}
        for name in columns:
            prop = model.properties.get(name, SchemaProperty())
            if name in obj:
                vals.append(quote_value(obj[name], generate_insert_function(prop)))
            else:
                vals.append("NULL")  # FEATURE(audit-mode): append cols are nullable → NULL passes through
    event = record.event
    vals.append(quote_value(record.operation, ""))
    # FEATURE(audit-mode): bare numeric literal; falsy ("" or JSON-null None) → NULL
    vals.append(event.mvcc_timestamp if (event is not None and event.mvcc_timestamp) else "NULL")
    vals.append(str(event.timestamp) if event is not None else "0")
    cols = ",".join(
        [quote_identifier(c) for c in columns]
        + [quote_identifier(_EDS_OPERATION), quote_identifier(_EDS_MVCC), quote_identifier(_EDS_TIMESTAMP)]
    )
    return f"INSERT INTO {quote_identifier(record.table)} ({cols})\nSELECT {','.join(vals)};\n"


def create_append_sql(s: Schema) -> str:
    """FEATURE(audit-mode): the append/history base table (§3.4a). CREATE OR REPLACE (= drop+create); object
    cols REUSE prop_type_to_sql_type VERBATIM (object → STRING, matching the upsert table; the PARSE_JSON insert
    value casts back into the STRING column) but NULLABLE except PK(s); _eds_seq AUTOINCREMENT is the surrogate
    PK; no secondary index (micro-partition pruning)."""
    table = quote_identifier(s.table)
    lines = []
    for name in s.columns():
        prop = s.properties.get(name, SchemaProperty())
        not_null = " NOT NULL" if name in s.primary_keys else ""  # FEATURE: only PK(s) NOT NULL in append
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop) + not_null + ",\n")
    body = "".join(lines)
    body += "\t" + quote_identifier(_EDS_SEQ) + " NUMBER AUTOINCREMENT,\n"
    body += "\t" + quote_identifier(_EDS_OPERATION) + " STRING NOT NULL,\n"
    body += "\t" + quote_identifier(_EDS_MVCC) + " NUMBER(38,10),\n"
    body += "\t" + quote_identifier(_EDS_TIMESTAMP) + " NUMBER,\n"
    body += "\t" + quote_identifier(_EDS_APPENDED_AT) + " TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),\n"
    body += "\tPRIMARY KEY (" + quote_identifier(_EDS_SEQ) + ")"
    return f"CREATE OR REPLACE TABLE {table} (\n{body}\n);\n"


def create_current_view_sql(s: Schema) -> str:
    """FEATURE(audit-mode): latest-per-object view (§3.4c) — single-level QUALIFY ROW_NUMBER() = 1 per PK
    partition, mvcc DESC NULLS LAST, excluding rows whose latest change is a DELETE."""
    obj_lines = ",\n".join("\t" + quote_identifier(c) for c in s.columns())
    partition = ",".join(quote_identifier(pk) for pk in s.primary_keys)
    order_by = ", ".join(
        [quote_identifier(_EDS_MVCC) + " DESC NULLS LAST", quote_identifier(_EDS_TIMESTAMP) + " DESC",
         quote_identifier(_EDS_SEQ) + " DESC"]
    )
    return (
        f"CREATE OR REPLACE VIEW {quote_identifier(s.table + '_current')} AS\n"
        f"SELECT\n"
        f"{obj_lines}\n"
        f"FROM {quote_identifier(s.table)}\n"
        f"QUALIFY ROW_NUMBER() OVER (\n"
        f"\tPARTITION BY {partition}\n"
        f"\tORDER BY {order_by}\n"
        f") = 1\n"
        f"\tAND {quote_identifier(_EDS_OPERATION)} <> 'DELETE';\n"
    )


def create_timeline_view_sql(s: Schema) -> str:
    """FEATURE(audit-mode): SCD-Type-2 point-in-time view (§3.4d) — valid_from = mvcc, valid_to = LEAD(mvcc)
    over the PK partition (NULL = still valid); includes deletes."""
    obj_lines = "".join("\t" + quote_identifier(c) + ",\n" for c in s.columns())
    partition = ",".join(quote_identifier(pk) for pk in s.primary_keys)
    order_by = ", ".join(
        [quote_identifier(_EDS_MVCC) + " ASC", quote_identifier(_EDS_TIMESTAMP) + " ASC",
         quote_identifier(_EDS_SEQ) + " ASC"]
    )
    return (
        f"CREATE OR REPLACE VIEW {quote_identifier(s.table + '_timeline')} AS\n"
        f"SELECT\n"
        f"{obj_lines}"
        f"\t{quote_identifier(_EDS_OPERATION)},\n"
        f"\t{quote_identifier(_EDS_MVCC)} AS {quote_identifier('valid_from')},\n"
        f"\tLEAD({quote_identifier(_EDS_MVCC)}) OVER (\n"
        f"\t\tPARTITION BY {partition}\n"
        f"\t\tORDER BY {order_by}\n"
        f"\t) AS {quote_identifier('valid_to')}\n"
        f"FROM {quote_identifier(s.table)};\n"
    )


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
