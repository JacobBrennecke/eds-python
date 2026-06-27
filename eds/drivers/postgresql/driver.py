"""PARITY: internal/drivers/postgresql/postgresql.go — the PostgreSQL driver (thin SqlDriverBase subclass).

The streaming/migration/import orchestration is in eds.drivers.sql_base.SqlDriverBase; this class supplies the
PostgreSQL hooks (SQL generation, quoting, schema/db-name SQL, the flushed-count log, metadata). The real
connect path (get_connection_string_from_url + the psycopg adapter) lands with the GoUrl util in the next step.
"""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.drivers.postgresql import sql
from eds.drivers.sql_base import SqlDriverBase
from eds.schema import DatabaseSchema, Schema
from eds.util.logger import Logger


class PostgresqlDriver(SqlDriverBase):
    """PARITY: postgresqlDriver."""

    def log_prefix(self) -> str:
        return "[postgres]"

    def validate_scheme(self) -> str:
        return "postgres"

    def default_port(self) -> int:
        return 5432

    def db_name_function(self) -> str:
        return "current_database()"

    def schema_column(self) -> str:
        return "table_catalog"

    def quote_identifier(self, name: str) -> str:
        return sql.quote_identifier(name)

    def to_sql(self, event: DBChangeEvent, schema: Schema) -> str:
        return sql.to_sql(event, schema)

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

    def on_flushed(self, logger: Logger, count: int) -> None:
        # PARITY: PostgreSQL-only post-commit log (MySQL/MSSQL omit it).
        logger.debug("flushed %d records", count)

    def name(self) -> str:
        return "PostgreSQL"

    def description(self) -> str:
        return "Supports streaming EDS messages to a PostgreSQL database."

    def example_url(self) -> str:
        return "postgres://localhost:5432/database"

    def aliases(self) -> list[str]:
        return ["postgresql"]
