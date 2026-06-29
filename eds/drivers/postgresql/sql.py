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
from eds.util import gourl
from eds.util.file import is_localhost
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
            prop = model.properties.get(name, SchemaProperty())
            if name in o:
                v = to_json_string_val(name, quote_value(o[name]), prop, True)
                update_vals.append(f"{quote_identifier(name)}={v}")
            else:
                # PARITY: missing diff column appends a BARE value (no name= prefix) — sql.go:157-158 quirk.
                update_vals.append(to_json_string_val(name, "NULL", prop, True))
        for name in columns:
            prop = model.properties.get(name, SchemaProperty())
            if name in o:
                insert_vals.append(to_json_string_val(name, quote_value(o[name]), prop, True))
            else:
                insert_vals.append(to_json_string_val(name, "NULL", prop, True))
    else:  # INSERT (and any non-UPDATE operation)
        for name in columns:
            prop = model.properties.get(name, SchemaProperty())
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
        prop = s.properties.get(name, SchemaProperty())  # PARITY: Go map zero-value for a missing PK property
        not_null = " NOT NULL" if (name in s.required and not prop.nullable) else ""
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop) + not_null + ",\n")
    body = "".join(lines)
    if s.primary_keys:
        body += "\tPRIMARY KEY (" + ", ".join(quote_identifier(pk) for pk in s.primary_keys) + ")"
    table = quote_identifier(s.table)
    return f"DROP TABLE IF EXISTS {table};\nCREATE TABLE {table} (\n{body}\n);\n"


# ===================================================================================================
# FEATURE(audit-mode): append / audit-trail SQL — NOT a Go port. See migration/features/audit-mode.md §3.1.
# Strictly additive: the upsert builders above are untouched; these emit plain INSERTs + history DDL + views.
# ===================================================================================================

# FEATURE(audit-mode): the fixed audit column names (shared literal names across all four drivers, §1.2).
_EDS_SEQ = "_eds_seq"
_EDS_OPERATION = "_eds_operation"
_EDS_MVCC = "_eds_mvcc_timestamp"
_EDS_TIMESTAMP = "_eds_timestamp"
_EDS_APPENDED_AT = "_eds_appended_at"


def to_append_sql(c: DBChangeEvent, model: Schema) -> str:
    """FEATURE(audit-mode): one plain INSERT per change (NO ON CONFLICT). INSERT/UPDATE emit the full
    after-snapshot; DELETE emits a tombstone (PK value(s) + NULLs). Reuses the upsert insert-value formatting
    (quote_value + to_json_string_val) so each row is a byte-sibling of the upsert SQL. mvcc is a BARE numeric
    literal (empty → NULL); _eds_seq + _eds_appended_at are DB-generated (never in the column list)."""
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
        o: dict[str, object] = {}
        if c.after is not None and len(c.after.value) > 0:
            # PARITY (reused): re-parse the raw After (numbers → float), exactly like upsert to_sql.
            parsed = json.loads(c.after.value, parse_int=float)
            if isinstance(parsed, dict):
                o = parsed
        for name in columns:
            prop = model.properties.get(name, SchemaProperty())
            if name in o:
                vals.append(to_json_string_val(name, quote_value(o[name]), prop, True))
            else:
                vals.append("NULL")  # FEATURE(audit-mode): append cols are nullable → NULL passes through
    vals.append(quote_value(c.operation))  # 'INSERT' / 'UPDATE' / 'DELETE'
    # FEATURE(audit-mode): bare numeric literal (NOT quoted); falsy ("" or JSON-null None) → NULL
    vals.append(c.mvcc_timestamp if c.mvcc_timestamp else "NULL")
    vals.append(str(c.timestamp))
    cols = ",".join(
        [quote_identifier(name) for name in columns]
        + [quote_identifier(_EDS_OPERATION), quote_identifier(_EDS_MVCC), quote_identifier(_EDS_TIMESTAMP)]
    )
    return f"INSERT INTO {quote_identifier(c.table)} ({cols}) VALUES ({','.join(vals)});\n"


def create_append_sql(s: Schema) -> str:
    """FEATURE(audit-mode): the append/history base table + history index (§3.1a). Object columns keep their
    upsert types but are NULLABLE except the PK(s); a surrogate _eds_seq IDENTITY PK replaces the object-id PK;
    the audit columns are appended. DROP TABLE … CASCADE auto-drops dependent views on recreate."""
    table = quote_identifier(s.table)
    lines = []
    for name in s.columns():
        prop = s.properties.get(name, SchemaProperty())
        not_null = " NOT NULL" if name in s.primary_keys else ""  # FEATURE: only PK(s) NOT NULL in append
        lines.append("\t" + quote_identifier(name) + " " + prop_type_to_sql_type(prop) + not_null + ",\n")
    body = "".join(lines)
    body += "\t" + quote_identifier(_EDS_SEQ) + " BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n"
    body += "\t" + quote_identifier(_EDS_OPERATION) + " TEXT NOT NULL,\n"
    body += "\t" + quote_identifier(_EDS_MVCC) + " NUMERIC(38,10),\n"
    body += "\t" + quote_identifier(_EDS_TIMESTAMP) + " BIGINT NOT NULL,\n"
    body += "\t" + quote_identifier(_EDS_APPENDED_AT) + " TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()"
    # FEATURE(audit-mode): PK idents join with no-space "," (cross-port w/ C#); ", " precedes the DESC suffix.
    idx_cols = (
        ",".join(quote_identifier(pk) for pk in s.primary_keys)
        + ", " + ", ".join([quote_identifier(_EDS_MVCC) + " DESC", quote_identifier(_EDS_TIMESTAMP) + " DESC",
                            quote_identifier(_EDS_SEQ) + " DESC"])
    )
    idx = quote_identifier(s.table + "__eds_history_idx")
    return (
        f"DROP TABLE IF EXISTS {table} CASCADE;\n"
        f"CREATE TABLE {table} (\n{body}\n);\n"
        f"CREATE INDEX {idx} ON {table} ({idx_cols});\n"
    )


def create_current_view_sql(s: Schema) -> str:
    """FEATURE(audit-mode): the latest-per-object view <table>_current (§3.1c) — DISTINCT ON the PK(s), ordered
    mvcc DESC NULLS LAST, projecting ONLY the object columns and EXCLUDING rows whose latest change is a DELETE
    (so it is row-for-row equal to the legacy upsert table)."""
    obj_csv = ",".join(quote_identifier(c) for c in s.columns())
    distinct = ",".join(quote_identifier(pk) for pk in s.primary_keys)
    # FEATURE(audit-mode): PK idents join with no-space "," (== DISTINCT ON + C#); ", " precedes the DESC suffix.
    order_by = (
        ",".join(quote_identifier(pk) for pk in s.primary_keys)
        + ", " + ", ".join([quote_identifier(_EDS_MVCC) + " DESC NULLS LAST",
                            quote_identifier(_EDS_TIMESTAMP) + " DESC", quote_identifier(_EDS_SEQ) + " DESC"])
    )
    return (
        f"CREATE OR REPLACE VIEW {quote_identifier(s.table + '_current')} AS\n"
        f"SELECT {obj_csv}\n"
        f"FROM (\n"
        f"\tSELECT DISTINCT ON ({distinct})\n"
        f"\t\t{obj_csv},{quote_identifier(_EDS_OPERATION)}\n"
        f"\tFROM {quote_identifier(s.table)}\n"
        f"\tORDER BY {order_by}\n"
        f') "latest"\n'
        f"WHERE {quote_identifier(_EDS_OPERATION)} <> 'DELETE';\n"
    )


def create_timeline_view_sql(s: Schema) -> str:
    """FEATURE(audit-mode): the SCD-Type-2 point-in-time view <table>_timeline (§3.1d) — every change row
    (INCLUDING deletes), plus valid_from = mvcc and valid_to = LEAD(mvcc) over the PK partition (NULL = still
    valid). Point-in-time: WHERE pk = X AND T >= valid_from AND (valid_to IS NULL OR T < valid_to)."""
    obj_csv = ",".join(quote_identifier(c) for c in s.columns())
    partition = ",".join(quote_identifier(pk) for pk in s.primary_keys)
    order_by = ", ".join(
        [quote_identifier(_EDS_MVCC) + " ASC", quote_identifier(_EDS_TIMESTAMP) + " ASC",
         quote_identifier(_EDS_SEQ) + " ASC"]
    )
    return (
        f"CREATE OR REPLACE VIEW {quote_identifier(s.table + '_timeline')} AS\n"
        f"SELECT\n"
        f"\t{obj_csv},\n"
        f"\t{quote_identifier(_EDS_OPERATION)},\n"
        f'\t{quote_identifier(_EDS_MVCC)} AS {quote_identifier("valid_from")},\n'
        f"\tLEAD({quote_identifier(_EDS_MVCC)}) OVER (\n"
        f"\t\tPARTITION BY {partition}\n"
        f"\t\tORDER BY {order_by}\n"
        f'\t) AS {quote_identifier("valid_to")}\n'
        f"FROM {quote_identifier(s.table)};\n"
    )


def drop_views_sql(s: Schema) -> list[str]:
    """FEATURE(audit-mode): drop both views (timeline then current) — used by the add-column migration before
    recreating them (a new sorted-position column shifts the view output columns, so they must be rebuilt)."""
    return [
        f"DROP VIEW IF EXISTS {quote_identifier(s.table + '_timeline')};",
        f"DROP VIEW IF EXISTS {quote_identifier(s.table + '_current')};",
    ]


def get_connection_string_from_url(urlstr: str) -> str:
    """PARITY: GetConnectionStringFromURL — force scheme postgresql, default port 5432, inject
    application_name=eds and (for localhost) sslmode=disable. The query is re-encoded (sorted) ONLY when a
    default was injected; otherwise raw_query is preserved verbatim."""
    try:
        u = gourl.parse(urlstr)
    except ValueError as e:
        raise ValueError(f"error parsing postgres db url: {e}") from e
    u.scheme = "postgresql"
    if u.port() == "":
        u.host = u.host + ":5432"
    q = u.query()
    reencode = False
    if not u.query().has("application_name"):  # PARITY: Has checks the ORIGINAL query
        q.set("application_name", "eds")
        reencode = True
    if is_localhost(u.host) and not u.query().has("sslmode"):
        q.set("sslmode", "disable")
        reencode = True
    if reencode:
        u.raw_query = q.encode()
    return str(u)


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
