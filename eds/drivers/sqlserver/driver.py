"""PARITY: internal/drivers/sqlserver/sqlserver.go — the SQL Server driver (thin SqlDriverBase subclass).

The one structural difference from PG/MySQL: to_sql_from_object is a MERGE upsert that takes NO operation arg,
so the base's import path (which passes "INSERT") is adapted here to drop it.
"""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.drivers.sql_base import SqlDb, SqlDriverBase
from eds.drivers.sqlserver import sql
from eds.schema import DatabaseSchema, Schema
from eds.util.logger import Logger


class MssqlDriver(SqlDriverBase):
    """PARITY: sqlserverDriver."""

    def log_prefix(self) -> str:
        return "[sqlserver]"

    def validate_scheme(self) -> str:
        return "sqlserver"

    def default_port(self) -> int:
        return 1433

    def db_name_function(self) -> str:
        return "DB_NAME()"

    def schema_column(self) -> str:
        return "table_catalog"

    def quote_identifier(self, name: str) -> str:
        return sql.quote_identifier(name)

    def to_sql(self, event: DBChangeEvent, schema: Schema) -> str:
        return sql.to_sql(event, schema)

    def to_sql_from_object(
        self, operation: str, schema: Schema, table: str, o: dict[str, object], diff: list[str] | None
    ) -> str:
        # PARITY: SQL Server's MERGE upsert ignores the operation (always insert-or-update by id).
        return sql.to_sql_from_object(schema, table, o, diff)

    def create_table_sql(self, schema: Schema) -> str:
        return sql.create_sql(schema)

    def add_new_columns_sql(
        self, logger: Logger, columns: list[str], schema: Schema, db: DatabaseSchema
    ) -> list[str]:
        return sql.add_new_columns_sql(logger, columns, schema, db)

    def get_connection_string_from_url(self, url: str) -> str:
        return url  # pymssql parses the URL into kwargs in open_db

    def open_db(self, conninfo: str) -> SqlDb:
        from eds.drivers.sqlserver.data_db import MssqlDataDb

        assert self._logger is not None
        return MssqlDataDb.open(conninfo, self._logger)

    def name(self) -> str:
        return "Microsoft SQL Server"

    def description(self) -> str:
        return "Supports streaming EDS messages to a Microsoft SQL Server database."

    def example_url(self) -> str:
        return "sqlserver://user:password@localhost:1433/database"

    def aliases(self) -> list[str]:
        return ["mssql"]
