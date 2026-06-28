"""PARITY: internal/drivers/sqlserver/{sql.go, escape.go} — byte-critical SQL Server SQL generation (pure).

SQL-Server-distinct: bracket identifiers; HYBRID escaping (' → '' doubled, all other control chars
backslash); MERGE upsert (to_sql_from_object takes NO operation arg — always insert-or-update by id);
quote_scalar=False (never quotes JSON scalars); handle_schema_property INSERT-value coercion; ADD (not
ADD COLUMN) in migrations; the parse_url_to_dsn golden DSN builder.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from eds.dbchange import DBChangeEvent
from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util import gourl
from eds.util.file import is_localhost
from eds.util.gofloat import format_g
from eds.util.gojson import RawJson, stringify
from eds.util.logger import Logger
from eds.util.sql import to_json_string_val, to_user_pass

# PARITY: hybrid escaping — ' is DOUBLED (SQL standard); everything else uses MySQL-style backslash.
_ESCAPE = {
    "\x00": "\\0", "\n": "\\n", "\r": "\\r", "\x1a": "\\Z",
    "'": "''", '"': '\\"', "\\": "\\\\",
}

# PARITY: §8.4 RFC3339 detector — [0-9] (ASCII), \Z (absolute end), '.' UNESCAPED (any char — Go quirk).
_LOOKS_LIKE_JSON_TS = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(.[0-9]{1,})?Z\Z")


def quote_identifier(val: str) -> str:
    """PARITY: quoteIdentifier — square brackets, NO internal escaping."""
    return "[" + val + "]"


def escape_string(s: str) -> str:
    """PARITY: escapeStringBackslash (hybrid)."""
    return "".join(_ESCAPE.get(c, c) for c in s)


def _format_ts(t: datetime) -> str:
    s = t.strftime("%Y-%m-%d %H:%M:%S")
    if t.microsecond:
        s += "." + f"{t.microsecond:06d}".rstrip("0")
    return s


def _parse_rfc3339(v: str) -> datetime:
    if not v.endswith("Z"):
        raise ValueError("missing trailing Z")
    body = v[:-1]
    micro = 0
    if "." in body:
        base, frac = body.split(".", 1)
        if not frac.isdigit():
            raise ValueError(f"bad fraction: {frac}")
        micro = int((frac + "000000")[:6])
    else:
        base = body
    dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
    return dt.replace(microsecond=micro, tzinfo=timezone.utc)


def _quote_string_value(v: str) -> str:
    if _LOOKS_LIKE_JSON_TS.match(v):
        orig_year = int(v[:4])  # the regex guarantees 4 leading digits
        # PARITY: Go's proleptic Gregorian accepts year 0; Python's datetime can't (MINYEAR=1). Any year < 1970
        # is floored anyway, so validate a year-0 string with a leap-year placeholder, then floor by orig_year.
        parse_target = ("2000" + v[4:]) if orig_year < 1 else v
        try:
            t = _parse_rfc3339(parse_target)
        except Exception as ex:  # noqa: BLE001 — PARITY: Go panics on a regex-match that fails to parse
            raise ValueError(f"error parsing: {v}. {ex}") from ex
        if orig_year < 1970:  # PARITY: SQL Server timestamp floor
            t = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        return "'" + _format_ts(t) + "'"
    return "'" + escape_string(v) + "'"


def _quote_datetime(t: datetime) -> str:
    if t.tzinfo is not None:
        t = t.astimezone(timezone.utc)
    if t.year <= 1:
        return "'0000-00-00'"
    return "'" + _format_ts(t) + "'"


def quote_value(arg: object) -> str:
    """PARITY: escape.go quoteValue. bool MUST precede int/float."""
    if arg is None:
        return "NULL"
    if isinstance(arg, bool):
        return "1" if arg else "0"
    if isinstance(arg, float):
        return format_g(arg)
    if isinstance(arg, int):
        return str(arg)
    if isinstance(arg, str):
        return _quote_string_value(arg)
    if isinstance(arg, RawJson):
        return "'" + escape_string(arg.value) + "'"
    if isinstance(arg, (bytes, bytearray)):
        return "_binary'" + escape_string(bytes(arg).decode("latin-1")) + "'"
    if isinstance(arg, datetime):
        return _quote_datetime(arg)
    return "'" + escape_string(stringify(arg)) + "'"


def handle_schema_property(prop: SchemaProperty, v: str) -> str:
    """PARITY: handleSchemaProperty — INSERT-value coercion (applied to non-id values only)."""
    t = prop.type
    if t == "object":
        return v  # no-op (the additional_properties branch returns v either way)
    if t == "boolean":
        if v.lower() == "true" or v == "1":
            return "1"
        if (not prop.nullable and v == "") or v.lower() == "false" or v.lower() == "null":
            return "0"
        return v
    if t == "integer":
        if v == "NULL":
            return "0"
        return v
    if t == "array":
        if not prop.nullable and v == "NULL":
            return "''"
        return v
    return v


def to_sql_from_object(model: Schema, table: str, o: dict[str, object], diff: list[str] | None) -> str:
    """PARITY: toSQLFromObject — a MERGE upsert keyed on id (NO operation arg; quote_scalar=False)."""
    columns = model.columns()
    update_values: list[str] = []
    for name in (diff if diff else columns):
        if name not in columns or name == "id":
            continue
        prop = model.properties.get(name, SchemaProperty())
        if name in o:
            v = to_json_string_val(name, quote_value(o[name]), prop, False)
            update_values.append(f"{quote_identifier(name)}={v}")
        else:
            update_values.append(f"{quote_identifier(name)}=NULL")

    insert_vals: list[str] = []
    for name in columns:
        prop = model.properties.get(name, SchemaProperty())
        if name in o:
            v = to_json_string_val(name, quote_value(o[name]), prop, False)
            if name != "id":  # PARITY: id is never coerced
                v = handle_schema_property(prop, v)
            insert_vals.append(v)
        else:
            insert_vals.append(handle_schema_property(prop, "NULL"))

    out = (
        f"MERGE {quote_identifier(table)} AS target USING (VALUES('{o['id']}')) AS source (id) "
        "ON target.id=source.id"
    )
    if update_values:
        out += " WHEN MATCHED THEN UPDATE SET " + ",".join(update_values)
    out += (
        " WHEN NOT MATCHED THEN INSERT (" + ",".join(quote_identifier(n) for n in columns)
        + ") VALUES (" + ",".join(insert_vals) + ");"  # PARITY: trailing ';' required for MERGE
    )
    return out


def to_sql(c: DBChangeEvent, model: Schema) -> str:
    """PARITY: toSQL — DELETE (terminated ';\\n') or MERGE."""
    if c.operation == "DELETE":
        keys = c.key or []
        preds = [
            f"{quote_identifier(pk)}={quote_value(keys[i])}" for i, pk in enumerate(model.primary_keys)
        ]
        return f"DELETE FROM {quote_identifier(c.table)} WHERE " + " AND ".join(preds) + ";\n"
    o = c.get_object() or {}
    return to_sql_from_object(model, c.table, o, c.diff)


def prop_type_to_sql_type(prop: SchemaProperty, is_primary_key: bool) -> str:
    """PARITY: propTypeToSQLType."""
    t = prop.type
    if t == "string":
        return "VARCHAR(64)" if is_primary_key else "NVARCHAR(MAX)"
    if t == "integer":
        return "BIGINT"
    if t == "number":
        return "FLOAT"
    if t == "boolean":
        return "BIT"
    if t == "object":
        return "NVARCHAR(MAX)"
    if t == "array":
        if prop.items is not None and prop.items.enum is not None:
            return "VARCHAR(64)"
        return "NVARCHAR(MAX)"
    return "NVARCHAR(MAX)"


def create_sql(s: Schema) -> str:
    """PARITY: createSQL — DROP+CREATE; no trailing newline/charset after the final ')'."""
    lines = []
    for name in s.columns():
        prop = s.properties.get(name, SchemaProperty())  # PARITY: Go map zero-value for a missing PK property
        is_pk = name in s.primary_keys
        not_null = " NOT NULL" if (name in s.required and not prop.nullable) else ""
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop, is_pk) + not_null + ",\n")
    body = "".join(lines)
    if s.primary_keys:
        body += "\tPRIMARY KEY (" + ", ".join(quote_identifier(pk) for pk in s.primary_keys) + ")"
    table = quote_identifier(s.table)
    return f"DROP TABLE IF EXISTS {table};\nCREATE TABLE {table} (\n{body}\n)"


def add_new_columns_sql(logger: Logger | None, columns: list[str], s: Schema, db: DatabaseSchema) -> list[str]:
    """PARITY: addNewColumnsSQL — 'ADD' (not 'ADD COLUMN'); trailing ';'."""
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
            "ALTER TABLE " + quote_identifier(s.table) + " ADD " + quote_identifier(column)
            + " " + prop_type_to_sql_type(prop, False) + ";"
        )
    return res


def parse_url_to_dsn(urlstr: str) -> str:
    """PARITY: ParseURLToDSN — the go-mssqldb DSN builder (golden-tested; distinct from the real connect)."""
    u = gourl.parse(urlstr)
    vals = u.query()
    # PARITY: Go gates on Get()=="" (true for ABSENT *or* explicitly-empty), so an explicit empty is overwritten.
    if is_localhost(u.host) and vals.get("encrypt") == "":
        vals.set("encrypt", "disable")
    if vals.get("app name") == "":
        vals.set("app name", "eds")
    out = "sqlserver://"
    if u.has_user_info:
        out += to_user_pass(u) + "@"
    out += u.host
    if u.path:
        vals.set("database", u.path[1:])
    enc = vals.encode()
    if enc:
        out += "?" + enc
    return out
