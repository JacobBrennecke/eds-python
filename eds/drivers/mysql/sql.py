"""PARITY: internal/drivers/mysql/{sql.go, escape.go} — byte-critical MySQL SQL generation (pure, golden).

MySQL-distinct from PostgreSQL: backtick identifiers (no escaping), backslash string escaping, bool→1/0,
floats via the Go 'g' verb (gofloat.format_g), REPLACE INTO upserts (an UPDATE emits a full-column REPLACE —
the diff is ignored, §8.2), and JSON-timestamp coercion (§8.4: raise on bad input, clamp <1970).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from eds.dbchange import DBChangeEvent
from eds.schema import DatabaseSchema, Schema, SchemaProperty
from eds.util.gofloat import format_g
from eds.util.gojson import RawJson, stringify
from eds.util.logger import Logger
from eds.util.sql import to_json_string_val

# PARITY: escape.go escapeStringBackslash — raw-byte map; every escaped byte is ASCII so char iteration matches.
_ESCAPE = {
    "\x00": "\\0", "\n": "\\n", "\r": "\\r", "\x1a": "\\Z",
    "'": "\\'", '"': '\\"', "\\": "\\\\",
}

# PARITY: §8.4 RFC3339 detector — [0-9] not \d (ASCII), \Z not $, and '.' is UNESCAPED (any char — Go quirk).
_JSON_TS = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(.[0-9]{1,})?Z\Z")


def quote_identifier(val: str) -> str:
    """PARITY: quoteIdentifier — backtick wrap, NO escaping of an embedded backtick (Go doesn't either)."""
    return "`" + val + "`"


def escape_string(s: str) -> str:
    """PARITY: escapeStringBackslash."""
    return "".join(_ESCAPE.get(c, c) for c in s)


def _format_ts(t: datetime) -> str:
    # PARITY: Go layout "2006-01-02 15:04:05.999999" — µs precision, drop trailing zeros + the dot when zero.
    s = t.strftime("%Y-%m-%d %H:%M:%S")
    if t.microsecond:
        s += "." + f"{t.microsecond:06d}".rstrip("0")
    return s


def _parse_rfc3339(v: str) -> datetime:
    # The string matched _JSON_TS; strictly parse it (raise on any non-RFC3339 form — incl. the '.'-as-any-char
    # quirk that lets e.g. "...03X69708Z" match the regex but fail here, mirroring Go's panic).
    if not v.endswith("Z"):
        raise ValueError("missing trailing Z")
    body = v[:-1]
    micro = 0
    if "." in body:
        base, frac = body.split(".", 1)
        if not frac.isdigit():
            raise ValueError(f"bad fraction: {frac}")
        micro = int((frac + "000000")[:6])  # truncate ns -> µs
    else:
        base = body
    dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")  # raises on a bad date/time
    return dt.replace(microsecond=micro, tzinfo=timezone.utc)


def _quote_string_value(v: str) -> str:
    if _JSON_TS.match(v):
        orig_year = int(v[:4])  # the regex guarantees 4 leading digits
        # PARITY: Go's proleptic Gregorian accepts year 0; Python's datetime can't (MINYEAR=1). Any year < 1970
        # is floored anyway, so validate a year-0 string with a leap-year placeholder, then floor by orig_year.
        parse_target = ("2000" + v[4:]) if orig_year < 1 else v
        try:
            t = _parse_rfc3339(parse_target)
        except Exception as ex:  # noqa: BLE001 — PARITY: Go panics on a regex-match that fails to parse
            raise ValueError(f"error parsing: {v}. {ex}") from ex
        if orig_year < 1970:  # PARITY: MySQL TIMESTAMP range starts 1970-01-01 00:00:01 UTC
            t = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        return "'" + _format_ts(t) + "'"
    return "'" + escape_string(v) + "'"


def _quote_datetime(t: datetime) -> str:
    if t.tzinfo is not None:
        t = t.astimezone(timezone.utc)
    if t.year <= 1:  # PARITY: Go time.Time zero value
        return "'0000-00-00'"
    return "'" + _format_ts(t) + "'"


def quote_value(arg: object) -> str:
    """PARITY: escape.go quoteValue. bool MUST precede int (bool is an int subclass in Python)."""
    if arg is None:
        return "NULL"
    if isinstance(arg, bool):
        return "1" if arg else "0"  # PARITY: 1/0, not TRUE/FALSE
    if isinstance(arg, float):
        return format_g(arg)  # PARITY: Go 'g' verb (not 'f')
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
    # map / []interface{} — JSON-marshal (sorted keys), backslash-escape, single-quote.
    return "'" + escape_string(stringify(arg)) + "'"


def to_sql_from_object(
    operation: str, model: Schema, table: str, o: dict[str, object], diff: list[str] | None
) -> str:
    """PARITY: toSQLFromObject — REPLACE INTO with ALL columns (every operation; §8.2 diff is ignored)."""
    cols = [quote_identifier(n) for n in model.columns()]
    insert_vals: list[str] = []
    for name in model.columns():
        prop = model.properties.get(name, SchemaProperty())
        if name in o:
            insert_vals.append(to_json_string_val(name, quote_value(o[name]), prop, True))
        else:
            insert_vals.append(to_json_string_val(name, "NULL", prop, True))
    return (
        f"REPLACE INTO {quote_identifier(table)} (" + ",".join(cols)
        + ") VALUES (" + ",".join(insert_vals) + ");\n"
    )


def to_sql(c: DBChangeEvent, model: Schema) -> str:
    """PARITY: toSQL — DELETE by primary key, else full-column REPLACE INTO."""
    if c.operation == "DELETE":
        keys = c.key or []
        preds = [
            f"{quote_identifier(pk)}={quote_value(keys[i])}" for i, pk in enumerate(model.primary_keys)
        ]
        return f"DELETE FROM {quote_identifier(c.table)} WHERE " + " AND ".join(preds) + ";\n"
    o = c.get_object() or {}
    return to_sql_from_object(c.operation, model, c.table, o, c.diff)


def prop_type_to_sql_type(prop: SchemaProperty, is_primary_key: bool) -> str:
    """PARITY: propTypeToSQLType — note the PK check precedes the date-time check."""
    t = prop.type
    if t == "string":
        if is_primary_key:
            return "VARCHAR(64)"
        if prop.format == "date-time":
            return "TIMESTAMP"
        return "TEXT"
    if t == "integer":
        return "BIGINT"
    if t == "number":
        return "FLOAT"
    if t == "boolean":
        return "BOOLEAN"
    if t == "object":
        return "JSON"
    if t == "array":
        if prop.items is not None and prop.items.enum is not None:
            return "VARCHAR(64)"
        return "JSON"
    return "TEXT"


def create_sql(s: Schema) -> str:
    """PARITY: createSQL — DROP+CREATE, PK-first columns, utf8mb4."""
    lines = []
    for name in s.columns():
        prop = s.properties.get(name, SchemaProperty())  # PARITY: Go map zero-value for a missing PK property
        is_pk = name in s.primary_keys
        not_null = " NOT NULL" if (name in s.required and not prop.nullable) else ""
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop, is_pk) + not_null + ",\n")
    body = "".join(lines)
    if s.primary_keys:
        body += "\tPRIMARY KEY (" + ", ".join(quote_identifier(pk) for pk in s.primary_keys) + ")"
    else:
        body += "\tPRIMARY KEY (id)"  # PARITY: Go unquoted fallback
    table = quote_identifier(s.table)
    return f"DROP TABLE IF EXISTS {table};\nCREATE TABLE {table} (\n{body}\n) CHARACTER SET=utf8mb4;\n"


def add_new_columns_sql(logger: Logger | None, columns: list[str], s: Schema, db: DatabaseSchema) -> list[str]:
    """PARITY: addNewColumnsSQL — skip existing (warn); one ALTER per new column (no NOT NULL; trailing ';')."""
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
            + " " + prop_type_to_sql_type(prop, False) + ";"
        )
    return res
