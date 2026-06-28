"""PARITY: the psycopg-backed SqlDb seam (mirrors the C# PostgresDataDb over ISqlDb).

Wraps a psycopg (v3) connection and implements the eds.drivers.sql_base.SqlDb interface by delegating to the
generic eds.util.db helpers, plus the PostgreSQL-specific transaction handling: the flush batch (many ';\\n'-
separated statements) runs in one transaction, and the "offending sql" is logged ONLY on the exec failure
(not on begin/commit), with the caller's buffer left intact (the base never resets on error).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import psycopg

from eds.schema import DatabaseSchema
from eds.util import db as dbutil
from eds.util.db import DbConn
from eds.util.logger import Logger


class PsycopgDb:
    """PARITY: PostgresDataDb. autocommit connection — explicit transaction() only for the flush batch."""

    def __init__(self, conn: psycopg.Connection[Any], logger: Logger) -> None:
        self._conn = conn
        self._logger = logger

    @property
    def _c(self) -> DbConn:
        # psycopg's overloaded cursor() isn't structurally matched to DbConn by mypy; the runtime is compatible.
        return cast(DbConn, self._conn)

    def query_single_value(self, fn: str) -> str:
        return dbutil.query_single_value(self._c, fn)

    def build_schema(
        self, logger: Logger, column: str, value: str, fail_if_empty: bool, conditions: list[tuple[str, str]]
    ) -> DatabaseSchema:
        return dbutil.build_db_schema_from_info_schema(logger, self._c, column, value, fail_if_empty, conditions)

    def execute_in_transaction(self, sql: str, logger: Logger) -> None:
        try:
            with self._conn.transaction():
                try:
                    self._conn.execute(sql)  # PARITY: whole batch in one tx (libpq simple, multi-statement)
                except Exception as e:
                    logger.error("offending sql: %s", sql)  # PARITY: logged only on the exec failure
                    raise ValueError(f"unable to execute sql: {e}") from e
        except ValueError:
            raise
        except Exception as e:  # PARITY: begin/commit failure
            raise ValueError(f"unable to commit transaction: {e}") from e

    def exec(self, sql: str) -> None:
        self._conn.execute(sql)

    def drop_table(self, quoted_table: str) -> None:
        dbutil.drop_table(self._c, quoted_table)

    def create_import_executor(self, dry_run: bool) -> Callable[[str], None]:
        return dbutil.sql_executer(self._logger, self._c, dry_run)

    def close(self) -> None:
        self._conn.close()
