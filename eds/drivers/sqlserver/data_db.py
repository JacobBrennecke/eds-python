"""PARITY: the pymssql-backed SqlDb seam (mirrors the C# MssqlDataDb over ISqlDb).

Go builds a go-mssqldb DSN; pymssql takes kwargs, so we parse the URL (via GoUrl) into connect kwargs. The
connection is autocommit (DDL + reads auto-commit); the flush batch runs in one explicit transaction (autocommit
toggled off), logging the offending SQL only on the exec failure and never resetting the caller's buffer.
pymssql/FreeTDS needs no external ODBC driver (DEVIATION from go-mssqldb's TLS posture: encryption-off for the
localhost/absent-encrypt case, which is what the configured URL specifies).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import pymssql

from eds.schema import DatabaseSchema
from eds.util import db as dbutil
from eds.util import gourl
from eds.util.db import DbConn
from eds.util.logger import Logger


def _connect_kwargs(url: str) -> dict[str, Any]:
    u = gourl.parse(url)
    return {
        "server": u.hostname() or "",
        "port": int(u.port()) if u.port() else 1433,
        "user": u.username,
        "password": u.password,
        "database": u.path.lstrip("/"),
    }


class MssqlDataDb:
    """PARITY: MssqlDataDb."""

    def __init__(self, conn: Any, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger

    @classmethod
    def open(cls, url: str, logger: Logger) -> MssqlDataDb:
        conn = pymssql.connect(
            **_connect_kwargs(url), appname="eds", login_timeout=5, timeout=0, autocommit=True
        )
        return cls(conn, logger)

    @property
    def _c(self) -> DbConn:
        return cast(DbConn, self._conn)

    def query_single_value(self, fn: str) -> str:
        return dbutil.query_single_value(self._c, fn)

    def build_schema(
        self, logger: Logger, column: str, value: str, fail_if_empty: bool, conditions: list[tuple[str, str]]
    ) -> DatabaseSchema:
        return dbutil.build_db_schema_from_info_schema(logger, self._c, column, value, fail_if_empty, conditions)

    def execute_in_transaction(self, sql: str, logger: Logger) -> None:
        self._conn.autocommit(False)
        try:
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql)
                except Exception as e:
                    logger.error("offending sql: %s", sql)  # PARITY: logged only on the exec failure
                    raise ValueError(f"unable to execute sql: {e}") from e
            try:
                self._conn.commit()
            except Exception as e:
                raise ValueError(f"unable to commit transaction: {e}") from e
        except Exception:
            self._conn.rollback()
            raise
        finally:
            self._conn.autocommit(True)

    def exec(self, sql: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql)

    def drop_table(self, quoted_table: str) -> None:
        self.exec("DROP TABLE IF EXISTS " + quoted_table)

    def create_import_executor(self, dry_run: bool) -> Callable[[str], None]:
        def execute(sql: str) -> None:
            if dry_run:
                self._logger.info("[dry-run] %s", sql)
                return
            self._logger.debug("executing: %s", sql.rstrip("\n"))
            self.exec(sql)

        return execute

    def close(self) -> None:
        self._conn.close()
