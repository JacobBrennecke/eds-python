"""PARITY: the PyMySQL-backed SqlDb seam (mirrors the C# MysqlDataDb over ISqlDb).

Go builds a go-sql-driver DSN with multiStatements=true; PyMySQL takes kwargs, so we parse the URL (via the
faithful GoUrl) into connect kwargs and set client_flag=MULTI_STATEMENTS — which, like Go, lets the whole
';'-separated flush batch run in one execute. Every execute drains nextset() (DROP+CREATE pairs and the
multi-statement batch each produce several result sets).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import pymysql
from pymysql.constants import CLIENT

from eds.schema import DatabaseSchema
from eds.util import db as dbutil
from eds.util import gourl
from eds.util.db import DbConn
from eds.util.logger import Logger


def _connect_kwargs(url: str) -> dict[str, Any]:
    u = gourl.parse(url)
    return {
        "host": u.hostname() or "",
        "port": int(u.port()) if u.port() else 3306,
        "user": u.username or None,
        "password": u.password or "",
        "database": u.path.lstrip("/"),
    }


class MysqlDataDb:
    """PARITY: MysqlDataDb."""

    def __init__(self, conn: Any, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger

    @classmethod
    def open(cls, url: str, logger: Logger) -> MysqlDataDb:
        conn = pymysql.connect(
            **_connect_kwargs(url),
            client_flag=CLIENT.MULTI_STATEMENTS,  # PARITY: Go multiStatements=true
            charset="utf8mb4",
            autocommit=False,
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
        try:
            self._conn.begin()
        except Exception as e:
            raise ValueError(f"unable to start transaction: {e}") from e
        try:
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    while cur.nextset():  # PARITY: drain every statement's result set
                        pass
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

    def exec(self, sql: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            while cur.nextset():
                pass
        self._conn.commit()

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
