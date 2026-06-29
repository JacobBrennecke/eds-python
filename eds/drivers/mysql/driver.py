"""PARITY: internal/drivers/mysql/mysql.go — the MySQL driver (thin SqlDriverBase subclass).

No per-driver flush/import divergence beyond the SQL hooks; on_flushed is the base no-op (MySQL, unlike
PostgreSQL, logs no flushed-count). The connection string is the URL itself (PyMySQL takes kwargs, parsed in
open_db) — Go's go-sql-driver DSN form is not reproduced (parity-neutral; SQL bytes are unaffected).
"""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.drivers.mysql import sql
from eds.drivers.sql_base import SqlDb, SqlDriverBase
from eds.schema import DatabaseSchema, Schema
from eds.util.logger import Logger


class MysqlDriver(SqlDriverBase):
    """PARITY: mysqlDriver."""

    def log_prefix(self) -> str:
        return "[mysql]"

    def validate_scheme(self) -> str:
        return "mysql"

    def default_port(self) -> int:
        return 3306

    def db_name_function(self) -> str:
        return "DATABASE()"

    def schema_column(self) -> str:
        return "table_schema"

    def quote_identifier(self, name: str) -> str:
        return sql.quote_identifier(name)

    def to_sql(self, event: DBChangeEvent, schema: Schema) -> str:
        return sql.to_sql(event, schema)

    # ---- FEATURE(audit-mode): append-mode builders (see migration/features/audit-mode.md §3.2) ----
    def to_append_sql(self, event: DBChangeEvent, schema: Schema) -> str:
        return sql.to_append_sql(event, schema)

    def create_append_table_sql(self, schema: Schema) -> str:
        return sql.create_append_sql(schema)

    def create_current_view_sql(self, schema: Schema) -> str:
        return sql.create_current_view_sql(schema)

    def create_timeline_view_sql(self, schema: Schema) -> str:
        return sql.create_timeline_view_sql(schema)

    def drop_views_sql(self, schema: Schema) -> list[str]:
        return sql.drop_views_sql(schema)

    def to_sql_from_object(
        self, operation: str, schema: Schema, table: str, o: dict[str, object], diff: list[str] | None
    ) -> str:
        return sql.to_sql_from_object(operation, schema, table, o, diff)

    def create_table_sql(self, schema: Schema) -> str:
        return sql.create_sql(schema)

    def add_new_columns_sql(
        self, logger: Logger, columns: list[str], schema: Schema, db: DatabaseSchema
    ) -> list[str]:
        return sql.add_new_columns_sql(logger, columns, schema, db)

    def get_connection_string_from_url(self, url: str) -> str:
        return url  # PyMySQL parses the URL into kwargs in open_db

    def open_db(self, conninfo: str) -> SqlDb:
        from eds.drivers.mysql.data_db import MysqlDataDb

        assert self._logger is not None
        return MysqlDataDb.open(conninfo, self._logger)

    def name(self) -> str:
        return "MySQL"

    def description(self) -> str:
        return "Supports streaming EDS messages to a MySQL database."

    def example_url(self) -> str:
        return "mysql://user:password@localhost:3306/database"
