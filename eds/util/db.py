"""PARITY: internal/util/dbschema.go + the SQL helpers from internal/util/sql.go (SQLExecuter, DropTable).

Generic SQL helpers over a DB-API 2.0 connection (psycopg / PyMySQL / pyodbc all qualify), shared by the SQL
drivers' DB adapters. Queries use raw string interpolation exactly like Go (no bound params) so the emitted
SQL is byte-identical. The info-schema reflection builds an eds.schema.DatabaseSchema.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from eds.schema import DatabaseSchema
from eds.util.logger import Logger


class _Cursor(Protocol):
    def execute(self, sql: str) -> Any: ...
    def fetchone(self) -> Any: ...
    def fetchall(self) -> Any: ...
    def __enter__(self) -> _Cursor: ...
    def __exit__(self, *exc: object) -> Any: ...


class DbConn(Protocol):
    """The minimal DB-API surface the helpers need."""

    def cursor(self) -> _Cursor: ...


def query_single_value(conn: DbConn, fn: str) -> str:
    """PARITY: QuerySingleValue — SELECT <fn> (raw concat, no params); first column of the first row."""
    with conn.cursor() as cur:
        cur.execute("SELECT " + fn)
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"no rows returned for: SELECT {fn}")
        return str(row[0])


def build_db_schema_from_info_schema(
    logger: Logger | None,
    conn: DbConn,
    column: str,
    value: str,
    fail_if_empty: bool,
    conditions: Sequence[tuple[str, str]] = (),
) -> DatabaseSchema:
    """PARITY: BuildDBSchemaFromInfoSchema[WithConditions] — reflect information_schema.columns into a
    DatabaseSchema (table -> column -> data_type)."""
    sql = (
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        f"WHERE {column} = '{value}'"
    )
    for col, val in conditions:
        sql += f" AND {col} = '{val}'"
    start = time.perf_counter()
    res = DatabaseSchema()
    with conn.cursor() as cur:
        cur.execute(sql)
        for table_name, column_name, data_type in cur.fetchall():
            res.setdefault(table_name, {})[column_name] = data_type
    if fail_if_empty and len(res) == 0:
        raise ValueError(f"no tables found using {column} = {value}")
    if logger is not None:
        # DEVIATION: Go logs %v of a time.Duration; the duration string format is cosmetic (not byte-tested).
        logger.info("refreshed %d tables ddl in %v", len(res), _format_duration(time.perf_counter() - start))
    return res


def sql_executer(logger: Logger, conn: DbConn, dry_run: bool) -> Callable[[str], None]:
    """PARITY: SQLExecuter — closure that logs+executes (or logs only when dry-run)."""

    def execute(sql: str) -> None:
        if dry_run:
            logger.info("[dry-run] %s", sql)
            return
        logger.debug("executing: %s", sql.rstrip("\n"))  # PARITY: TrimRight(sql, "\n")
        with conn.cursor() as cur:
            cur.execute(sql)

    return execute


def drop_table(conn: DbConn, table: str) -> None:
    """PARITY: DropTable — DROP TABLE IF EXISTS <table> (table passed already-quoted by the caller; no log)."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS " + table)


def _format_duration(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.3f}µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:.3f}ms"
    return f"{seconds:.3f}s"
