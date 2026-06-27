"""PARITY: internal/drivers/postgresql/sql.go — byte-critical PostgreSQL SQL generation (pure, golden-tested).

GetConnectionStringFromURL (also in sql.go) is connection plumbing and lands with the driver. This module
is the SQL byte-output: quoting (incl. $_H_$ dollar-quoting), INSERT…ON CONFLICT / UPDATE-diff / DELETE,
CREATE TABLE, ALTER ADD COLUMN. Reuses eds.util.sql (to_json_string_val etc.), gojson, gofloat.format_f.
"""

from __future__ import annotations

import json
import re

from eds.dbchange import DBChangeEvent
from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util.gofloat import format_f
from eds.util.gojson import stringify
from eds.util.logger import Logger
from eds.util.sql import to_json_string_val

_MAGIC_ESCAPE = "$_H_$"
# PARITY: safeCharacters — note \Z (Go RE2 `$` = absolute end; Python `$` would allow a trailing newline).
_SAFE_CHARACTERS = re.compile(r'^["/.,;:$%/@!#$%^&*(){}\[\]|\\<>?~a-zA-Z0-9_\- ]+\Z')
_BAD_CHARACTERS = re.compile("\x00")


def quote_string(s: str) -> str:
    """PARITY: quoteString — strip NULs; single-quote if empty/safe, else $_H_$ dollar-quote (no escaping)."""
    if len(s) != 0 and _BAD_CHARACTERS.search(s):
        s = _BAD_CHARACTERS.sub("", s)
    if len(s) == 0 or _SAFE_CHARACTERS.match(s):
        return "'" + s + "'"
    return _MAGIC_ESCAPE + s + _MAGIC_ESCAPE


def quote_bytes(buf: bytes) -> str:
    """PARITY: quoteBytes — '\\x' + lowercase hex."""
    return "'\\x" + buf.hex() + "'"


def quote_identifier(val: str) -> str:
    """PARITY: pq.QuoteIdentifier — truncate at the first NUL, double internal quotes. DISTINCT from
    eds.util.sql.quote_identifier (the trivial wrap); the pg driver needs this pq-faithful version."""
    end = val.find("\x00")
    if end > -1:
        val = val[:end]
    return '"' + val.replace('"', '""') + '"'


def quote_value(arg: object) -> str:
    """PARITY: quoteValue — type-dispatch. bool MUST precede int (bool is an int subclass in Python)."""
    if arg is None:
        return "null"
    if isinstance(arg, bool):
        return "true" if arg else "false"
    if isinstance(arg, int):
        return str(arg)
    if isinstance(arg, float):
        return format_f(arg)  # PARITY: pg uses strconv 'f' -1 (format_g not needed)
    if isinstance(arg, (bytes, bytearray)):
        return quote_bytes(bytes(arg))
    if isinstance(arg, str):
        return quote_string(arg)
    # DEVIATION: the Go []string branch (pq.QuoteLiteral per-elem) is unreachable from JSON data; lists/dicts
    # route through the []interface{}/map path = quote_string(JSONStringify).
    return quote_string(stringify(arg))


def to_sql_from_object(
    operation: str, model: Schema, table: str, o: dict[str, object], diff: list[str] | None
) -> str:
    """PARITY: toSQLFromObject — INSERT … ON CONFLICT (id) DO UPDATE/NOTHING."""
    columns = model.columns()
    insert_vals: list[str] = []
    update_vals: list[str] = []

    if operation == "UPDATE":
        for name in diff or []:
            if name not in columns or name == "id":
                continue
            prop = model.properties[name]
            if name in o:
                v = to_json_string_val(name, quote_value(o[name]), prop, True)
                update_vals.append(f"{quote_identifier(name)}={v}")
            else:
                # PARITY: missing diff column appends a BARE value (no name= prefix) — sql.go:157-158 quirk.
                update_vals.append(to_json_string_val(name, "NULL", prop, True))
        for name in columns:
            prop = model.properties[name]
            if name in o:
                insert_vals.append(to_json_string_val(name, quote_value(o[name]), prop, True))
            else:
                insert_vals.append(to_json_string_val(name, "NULL", prop, True))
    else:  # INSERT (and any non-UPDATE operation)
        for name in columns:
            prop = model.properties[name]
            if name in o:
                v = to_json_string_val(name, quote_value(o[name]), prop, True)
                if name != "id":
                    update_vals.append(f"{quote_identifier(name)}={v}")
                insert_vals.append(v)
            else:
                v = to_json_string_val(name, "NULL", prop, True)
                update_vals.append(f"{quote_identifier(name)}={v}")
                insert_vals.append(v)

    cols = ",".join(quote_identifier(c) for c in columns)
    vals = ",".join(insert_vals)
    tail = ("UPDATE SET " + ",".join(update_vals)) if update_vals else "NOTHING"
    return f"INSERT INTO {quote_identifier(table)} ({cols}) VALUES ({vals}) ON CONFLICT (id) DO {tail};\n"


def to_sql(c: DBChangeEvent, model: Schema) -> str:
    """PARITY: toSQL — DELETE by primary key, else re-parse After (numbers→float) and delegate."""
    if c.operation == "DELETE":
        keys = c.key or []
        conds = [
            f"{quote_identifier(pk)}={quote_value(keys[i])}"
            for i, pk in enumerate(model.primary_keys)
        ]
        return f"DELETE FROM {quote_identifier(c.table)} WHERE " + " AND ".join(conds) + ";\n"
    o: dict[str, object] = {}
    if c.after is not None and len(c.after.value) > 0:
        # PARITY: re-parse the raw After (not the cached object); Go map[string]any → numbers are float.
        parsed = json.loads(c.after.value, parse_int=float)
        if isinstance(parsed, dict):
            o = parsed
    return to_sql_from_object(c.operation, model, c.table, o, c.diff)


def prop_type_to_sql_type(prop: SchemaProperty) -> str:
    """PARITY: propTypeToSQLType — unknown/empty type → TEXT."""
    t = prop.type
    if t == "string":
        return "TIMESTAMP WITH TIME ZONE" if prop.format == "date-time" else "TEXT"
    if t == "integer":
        return "BIGINT"
    if t == "number":
        return "DOUBLE PRECISION"
    if t == "boolean":
        return "BOOLEAN"
    if t == "object":
        return "JSONB"
    if t == "array":
        if prop.items is not None and prop.items.enum is not None:
            return "VARCHAR(64)"
        return "JSONB"
    return "TEXT"


def create_sql(s: Schema) -> str:
    """PARITY: createSQL — DROP + CREATE; columns PK-first then sorted; NOT NULL iff required AND not nullable."""
    lines = []
    for name in s.columns():
        prop = s.properties[name]
        not_null = " NOT NULL" if (name in s.required and not prop.nullable) else ""
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop) + not_null + ",\n")
    body = "".join(lines)
    if s.primary_keys:
        body += "\tPRIMARY KEY (" + ", ".join(quote_identifier(pk) for pk in s.primary_keys) + ")"
    table = quote_identifier(s.table)
    return f"DROP TABLE IF EXISTS {table};\nCREATE TABLE {table} (\n{body}\n);\n"


def add_new_columns_sql(logger: Logger | None, columns: list[str], s: Schema, db: DatabaseSchema) -> list[str]:
    """PARITY: addNewColumnsSQL — skip existing (warn); one ALTER per new column (terminated with ';', no \\n)."""
    res: list[str] = []
    for column in columns:
        found, _ = db.get_type(s.table, column)
        if found:
            if logger is not None:
                logger.warn(
                    "skipping migration for column: %s for table: %s since it already exists", column, s.table
                )
            continue
        # PARITY: a column not in the schema → zero SchemaProperty (Type "") → propTypeToSQLType → TEXT.
        prop = s.properties.get(column, SchemaProperty())
        res.append(
            "ALTER TABLE " + quote_identifier(s.table) + " ADD COLUMN " + quote_identifier(column)
            + " " + prop_type_to_sql_type(prop) + ";"
        )
    return res
