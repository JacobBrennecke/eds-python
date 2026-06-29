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


# ===================================================================================================
# FEATURE(audit-mode): append / audit-trail SQL — NOT a Go port. See migration/features/audit-mode.md §3.2.
# Strictly additive: the REPLACE-INTO upsert builders above are untouched.
# ===================================================================================================

_EDS_SEQ = "_eds_seq"
_EDS_OPERATION = "_eds_operation"
_EDS_MVCC = "_eds_mvcc_timestamp"
_EDS_TIMESTAMP = "_eds_timestamp"
_EDS_APPENDED_AT = "_eds_appended_at"


def to_append_sql(c: DBChangeEvent, model: Schema) -> str:
    """FEATURE(audit-mode): one plain INSERT per change (NO REPLACE). Reuses quote_value + to_json_string_val
    so each row is a byte-sibling of the upsert SQL; DELETE → tombstone (PK value(s) + NULLs). §3.2."""
    columns = model.columns()
    vals: list[str] = []
    if c.operation == "DELETE":
        keys = c.key or []
        for name in columns:
            if name in model.primary_keys:
                vals.append(quote_value(keys[model.primary_keys.index(name)]))
            else:
                vals.append("NULL")
    else:
        o = c.get_object() or {}
        for name in columns:
            prop = model.properties.get(name, SchemaProperty())
            if name in o:
                vals.append(to_json_string_val(name, quote_value(o[name]), prop, True))
            else:
                vals.append("NULL")  # FEATURE(audit-mode): append cols are nullable → NULL passes through
    vals.append(quote_value(c.operation))
    # FEATURE(audit-mode): bare numeric literal; falsy ("" or JSON-null None) → NULL
    vals.append(c.mvcc_timestamp if c.mvcc_timestamp else "NULL")
    vals.append(str(c.timestamp))
    cols = ",".join(
        [quote_identifier(n) for n in columns]
        + [quote_identifier(_EDS_OPERATION), quote_identifier(_EDS_MVCC), quote_identifier(_EDS_TIMESTAMP)]
    )
    return f"INSERT INTO {quote_identifier(c.table)} ({cols}) VALUES ({','.join(vals)});\n"


def create_append_sql(s: Schema) -> str:
    """FEATURE(audit-mode): the history base table + inline KEY index (§3.2a). Object cols keep their upsert
    types but NULLABLE except PK(s); _eds_seq AUTO_INCREMENT is the surrogate PK (AUTO_INCREMENT must be a key).
    Drops both views first (MySQL views freeze their column list at create)."""
    table = quote_identifier(s.table)
    lines = []
    for name in s.columns():
        prop = s.properties.get(name, SchemaProperty())
        is_pk = name in s.primary_keys
        not_null = " NOT NULL" if is_pk else ""  # FEATURE: only PK(s) NOT NULL in append
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop, is_pk) + not_null + ",\n")
    body = "".join(lines)
    body += "\t" + quote_identifier(_EDS_SEQ) + " BIGINT NOT NULL AUTO_INCREMENT,\n"
    body += "\t" + quote_identifier(_EDS_OPERATION) + " VARCHAR(16) NOT NULL,\n"
    body += "\t" + quote_identifier(_EDS_MVCC) + " DECIMAL(38,10),\n"
    body += "\t" + quote_identifier(_EDS_TIMESTAMP) + " BIGINT,\n"
    body += "\t" + quote_identifier(_EDS_APPENDED_AT) + " TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
    body += "\tPRIMARY KEY (" + quote_identifier(_EDS_SEQ) + "),\n"
    # FEATURE(audit-mode): PK idents join with no-space "," (cross-port w/ C# + PARTITION BY); ", " before DESC.
    idx_cols = (
        ",".join(quote_identifier(pk) for pk in s.primary_keys)
        + ", " + ", ".join([quote_identifier(_EDS_MVCC) + " DESC", quote_identifier(_EDS_TIMESTAMP) + " DESC",
                            quote_identifier(_EDS_SEQ) + " DESC"])
    )
    body += "\tKEY " + quote_identifier(s.table + "_eds_history_idx") + " (" + idx_cols + ")"
    return (
        f"DROP VIEW IF EXISTS {quote_identifier(s.table + '_timeline')};\n"
        f"DROP VIEW IF EXISTS {quote_identifier(s.table + '_current')};\n"
        f"DROP TABLE IF EXISTS {table};\n"
        f"CREATE TABLE {table} (\n{body}\n) CHARACTER SET=utf8mb4;\n"
    )


def create_current_view_sql(s: Schema) -> str:
    """FEATURE(audit-mode): latest-per-object view (§3.2c) — ROW_NUMBER() = 1 per PK partition, excluding
    rows whose latest change is a DELETE. MySQL sorts NULL mvcc last under DESC implicitly (no NULLS LAST)."""
    obj_csv = ", ".join(quote_identifier(c) for c in s.columns())
    partition = ",".join(quote_identifier(pk) for pk in s.primary_keys)
    order_by = ", ".join(
        [quote_identifier(_EDS_MVCC) + " DESC", quote_identifier(_EDS_TIMESTAMP) + " DESC",
         quote_identifier(_EDS_SEQ) + " DESC"]
    )
    return (
        f"CREATE VIEW {quote_identifier(s.table + '_current')} AS\n"
        f"SELECT {obj_csv}\n"
        f"FROM (\n"
        f"\tSELECT {obj_csv}, {quote_identifier(_EDS_OPERATION)},\n"
        f"\t\tROW_NUMBER() OVER (\n"
        f"\t\t\tPARTITION BY {partition}\n"
        f"\t\t\tORDER BY {order_by}\n"
        f"\t\t) AS {quote_identifier('_eds_rn')}\n"
        f"\tFROM {quote_identifier(s.table)}\n"
        f") AS {quote_identifier('_eds_ranked')}\n"
        f"WHERE {quote_identifier('_eds_rn')} = 1 AND {quote_identifier(_EDS_OPERATION)} <> 'DELETE';\n"
    )


def create_timeline_view_sql(s: Schema) -> str:
    """FEATURE(audit-mode): SCD-Type-2 point-in-time view (§3.2d) — valid_from = mvcc, valid_to = LEAD(mvcc)
    over the PK partition (NULL = still valid); includes deletes."""
    obj_csv = ", ".join(quote_identifier(c) for c in s.columns())
    partition = ",".join(quote_identifier(pk) for pk in s.primary_keys)
    order_by = ", ".join(
        [quote_identifier(_EDS_MVCC) + " ASC", quote_identifier(_EDS_TIMESTAMP) + " ASC",
         quote_identifier(_EDS_SEQ) + " ASC"]
    )
    return (
        f"CREATE VIEW {quote_identifier(s.table + '_timeline')} AS\n"
        f"SELECT {obj_csv}, {quote_identifier(_EDS_OPERATION)},\n"
        f"\t{quote_identifier(_EDS_MVCC)} AS {quote_identifier('valid_from')},\n"
        f"\tLEAD({quote_identifier(_EDS_MVCC)}) OVER (\n"
        f"\t\tPARTITION BY {partition}\n"
        f"\t\tORDER BY {order_by}\n"
        f"\t) AS {quote_identifier('valid_to')}\n"
        f"FROM {quote_identifier(s.table)};\n"
    )


def drop_views_sql(s: Schema) -> list[str]:
    """FEATURE(audit-mode): drop both views (timeline then current) for the add-column migration."""
    return [
        f"DROP VIEW IF EXISTS {quote_identifier(s.table + '_timeline')};",
        f"DROP VIEW IF EXISTS {quote_identifier(s.table + '_current')};",
    ]
