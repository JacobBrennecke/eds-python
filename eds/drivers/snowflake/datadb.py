"""PARITY: the snowflake-connector-python-backed ISnowflakeDb seam (mirrors C# SnowflakeDataDb).

Unit-untestable (no Snowflake account); snowflake.connector + cryptography are lazily imported. Go/gosnowflake
use a DSN; the connector takes kwargs, so the URL is parsed into kwargs (DEVIATION: snowflake-connector-format).
Multi-statement uses the connector's num_statements cursor parameter.
"""

from __future__ import annotations

from typing import Any, cast

from eds.schema import DatabaseSchema
from eds.util import db as dbutil
from eds.util import gourl
from eds.util.db import DbConn
from eds.util.logger import Logger


class SnowflakeDataDb:
    """PARITY: SnowflakeDataDb."""

    def __init__(self, conn: Any, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger

    @classmethod
    def open_from_url(cls, url: str, logger: Logger) -> SnowflakeDataDb:
        import snowflake.connector

        u = gourl.parse(url)
        conn = snowflake.connector.connect(
            account=u.host,
            user=u.username or None,
            password=u.password or None,
            database=u.path.lstrip("/") or None,
            application="eds",
            client_session_keep_alive=True,
        )
        self = cls(conn, logger)
        self._ping()
        return self

    @classmethod
    def open_with_key_pair(
        cls, account: str, user: str, database: str, schema: str, secret: str, logger: Logger
    ) -> SnowflakeDataDb:
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization

        if not secret:
            raise ValueError("failed to decode secret")
        try:
            key = serialization.load_pem_private_key(secret.encode(), password=None)
        except Exception as ex:  # noqa: BLE001 — PARITY: "failed to parse private key"
            raise ValueError(f"failed to parse private key: {ex}") from ex
        der = key.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        conn = snowflake.connector.connect(
            account=account, user=user, database=database, schema=schema,
            private_key=der, authenticator="snowflake_jwt",
            application="eds", client_session_keep_alive=True,
        )
        self = cls(conn, logger)
        self._ping()
        return self

    def _ping(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1")

    @property
    def _c(self) -> DbConn:
        return cast(DbConn, self._conn)

    def query_single_value(self, fn: str) -> str:
        return dbutil.query_single_value(self._c, fn)

    def build_schema(self, catalog: str, schema: str, fail_if_empty: bool) -> DatabaseSchema:
        return dbutil.build_db_schema_from_info_schema(
            self._logger, self._c, "table_catalog", catalog, fail_if_empty, [("table_schema", schema)]
        )

    def exec_multi_statement(self, sql: str, statement_count: int) -> int:
        rows = 0
        with self._conn.cursor() as cur:
            cur.execute(sql, num_statements=statement_count)
            while True:
                rows += cur.rowcount or 0
                if not cur.nextset():
                    break
        return rows

    def exec(self, sql: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql)

    def close(self) -> None:
        self._conn.close()
