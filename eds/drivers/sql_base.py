"""Shared SQL-driver orchestration (port of the C# WS7 SqlDriverBase + ISqlDb seam).

The PostgreSQL / MySQL / SQL Server / Snowflake drivers in Go are ~90% identical: buffer SQL on Process,
flush the whole batch in one transaction (NO buffer reset on error — the next Flush resends), migrate by
drop+recreate / alter, byte-batched import. That common logic lives here; each driver supplies the per-driver
hooks (SQL generation, quoting, connection string, schema/db-name SQL, the on-flushed log, the DB adapter).

The DB seam (`SqlDb`) makes the orchestration unit-testable against a fake with no Docker — exactly how the C#
SqlDriverBaseTests lock the no-reset/flush quirks.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from eds.dbchange import DBChangeEvent
from eds.driver import (
    DriverConfig,
    DriverField,
    FieldError,
    ImporterConfig,
    new_database_configuration,
    url_from_database_configuration,
)
from eds.schema import DatabaseSchema, Schema, SchemaMap, SchemaRegistry
from eds.util.logger import Logger

# PARITY: postgresql.go maxBytesSizeInsert.
MAX_BYTES_SIZE_INSERT = 5_000_000


@runtime_checkable
class SqlDb(Protocol):
    """PARITY: the ISqlDb seam — the per-driver DB adapter the base orchestrates."""

    def query_single_value(self, fn: str) -> str: ...
    def build_schema(
        self, logger: Logger, column: str, value: str, fail_if_empty: bool, conditions: list[tuple[str, str]]
    ) -> DatabaseSchema: ...
    def execute_in_transaction(self, sql: str, logger: Logger) -> None: ...
    def exec(self, sql: str) -> None: ...
    def drop_table(self, quoted_table: str) -> None: ...
    def create_import_executor(self, dry_run: bool) -> Callable[[str], None]: ...
    def close(self) -> None: ...


class SqlDriverBase:
    """Shared lifecycle/flush/migration/import for the SQL drivers. Subclasses override the hooks below."""

    def __init__(self) -> None:
        self._logger: Logger | None = None
        self._db: SqlDb | None = None
        self._registry: SchemaRegistry | None = None
        self._ctx: Any = None
        self._pending: list[str] = []
        self._count = 0
        self._size = 0
        self._dbname = ""
        self._dbschema: DatabaseSchema = DatabaseSchema()
        self._import_config: ImporterConfig | None = None
        self._executor: Callable[[str], None] | None = None
        self._stopped = False
        self._lock = threading.Lock()

    # ---- per-driver hooks (subclass MUST override the SQL/connection ones) ----

    def log_prefix(self) -> str:
        raise NotImplementedError

    def validate_scheme(self) -> str:
        raise NotImplementedError

    def default_port(self) -> int:
        raise NotImplementedError

    def db_name_function(self) -> str:
        raise NotImplementedError

    def schema_column(self) -> str:
        raise NotImplementedError

    def quote_identifier(self, name: str) -> str:
        raise NotImplementedError

    def to_sql(self, event: DBChangeEvent, schema: Schema) -> str:
        raise NotImplementedError

    def to_sql_from_object(
        self, operation: str, schema: Schema, table: str, o: dict[str, object], diff: list[str] | None
    ) -> str:
        raise NotImplementedError

    def create_table_sql(self, schema: Schema) -> str:
        raise NotImplementedError

    def add_new_columns_sql(self, logger: Logger, columns: list[str], schema: Schema, db: DatabaseSchema) -> list[str]:
        raise NotImplementedError

    def get_connection_string_from_url(self, url: str) -> str:
        raise NotImplementedError

    def open_db(self, conninfo: str) -> SqlDb:
        raise NotImplementedError

    def on_flushed(self, logger: Logger, count: int) -> None:
        """PARITY: PostgreSQL logs 'flushed %d records' here; the others don't (default no-op)."""

    def max_batch_size(self) -> int:
        return 500  # PARITY: postgresql.go maxBatchSize (drivers may override)

    # DriverHelp metadata hooks
    def name(self) -> str:
        raise NotImplementedError

    def description(self) -> str:
        raise NotImplementedError

    def example_url(self) -> str:
        raise NotImplementedError

    def help(self) -> str:
        return ""

    def aliases(self) -> list[str]:
        return []

    # ---- shared lifecycle ----

    def start(self, config: DriverConfig) -> None:
        """PARITY: Start — prefix the logger, connect (which refreshes the schema)."""
        assert config.logger is not None
        self._logger = config.logger.with_prefix(self.log_prefix())
        self._ctx = config.context
        self._registry = config.schema_registry
        self._db = self._connect_to_db(config.context, config.url)

    def _connect_to_db(self, ctx: Any, url: str) -> SqlDb:
        conninfo = self.get_connection_string_from_url(url)
        db = self.open_db(conninfo)
        try:
            self._refresh_schema(db, fail_if_empty=False)
        except Exception:
            db.close()
            raise
        return db

    def _refresh_schema(self, db: SqlDb, fail_if_empty: bool) -> None:
        assert self._logger is not None
        if self._dbname == "":
            try:
                self._dbname = db.query_single_value(self.db_name_function())
            except Exception as e:
                raise ValueError(f"error getting current database name: {e}") from e
        try:
            self._dbschema = db.build_schema(
                self._logger, self.schema_column(), self._dbname, fail_if_empty, []
            )
        except Exception as e:
            raise ValueError(f"error building database schema: {e}") from e

    def stop(self) -> None:
        """PARITY: Stop — idempotent; close the db once."""
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            if self._logger is not None:
                self._logger.debug("stopping")
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._logger is not None:
                self._logger.debug("stopped")

    def test(self, ctx: Any, logger: Logger, url: str) -> None:
        """PARITY: Test — connect (open+refresh) then close."""
        self._logger = logger.with_prefix(self.log_prefix())
        db = self._connect_to_db(ctx, url)
        db.close()

    # ---- streaming ----

    def process(self, logger: Logger, event: DBChangeEvent) -> bool:
        """PARITY: Process — buffer the SQL; always return False (the consumer drives batching)."""
        assert self._registry is not None
        logger.trace("processing event: %s", event)
        try:
            _, version = self._registry.get_table_version(event.table)  # PARITY: the found bool is ignored
        except Exception as e:
            raise ValueError(f"unable to get table version for table: {event.table}: {e}") from e
        try:
            schema = self._registry.get_schema(event.table, version)
        except Exception as e:
            raise ValueError(f"unable to get schema for table: {event.table} ({version}). {e}") from e
        sql = self.to_sql(event, schema)
        logger.trace("sql: %s", sql)
        with self._lock:
            self._pending.append(sql)
            self._count += 1
        return False

    def flush(self, logger: Logger) -> None:
        """PARITY: Flush — execute the whole batch in one transaction. NO buffer reset on error (the reset is
        only reached on success/empty), so the next Flush resends the batch."""
        assert self._db is not None
        logger.debug("flush")
        with self._lock:
            if self._count > 0:
                self._db.execute_in_transaction("".join(self._pending), logger)
                self.on_flushed(logger, self._count)
            self._pending = []
            self._count = 0

    # ---- migration ----

    def migrate_new_table(self, ctx: Any, logger: Logger, schema: Schema) -> None:
        """PARITY: MigrateNewTable — drop+recreate if the table exists, else create."""
        assert self._db is not None
        if schema.table in self._dbschema:
            logger.info("table already exists for: %s, dropping and recreating...", schema.table)
            self._db.drop_table(self.quote_identifier(schema.table))
        sql = self.create_table_sql(schema)
        logger.trace("migrate new table: %s", sql)
        self._db.exec(sql)
        self._refresh_schema(self._db, fail_if_empty=True)

    def migrate_new_columns(self, ctx: Any, logger: Logger, schema: Schema, columns: list[str]) -> None:
        """PARITY: MigrateNewColumns — one ALTER per new column, then refresh."""
        assert self._db is not None
        for sql in self.add_new_columns_sql(logger, columns, schema, self._dbschema):
            logger.trace("migrating new columns: %s", sql)
            self._db.exec(sql)
            logger.debug("migrated new columns: %s", sql)
        self._refresh_schema(self._db, fail_if_empty=True)

    def get_destination_schema(self, ctx: Any, logger: Logger) -> DatabaseSchema:
        """PARITY: GetDestinationSchema."""
        return self._dbschema

    # ---- import handler (the M5 runner drives these) ----

    def create_datasource(self, schema: SchemaMap) -> None:
        """PARITY: CreateDatasource — create each configured table."""
        assert self._import_config is not None and self._executor is not None and self._logger is not None
        for table in self._import_config.tables:
            data = schema[table]
            self._logger.debug("creating table %s", table)
            try:
                self._executor(self.create_table_sql(data))
            except Exception as e:
                raise ValueError(f"error creating table: {table}. {e}") from e
            self._logger.debug("created table %s", table)

    def import_event(self, event: DBChangeEvent, data: Schema) -> None:
        """PARITY: ImportEvent — full upsert; byte-batched flush at maxBytesSizeInsert (or single)."""
        assert self._import_config is not None and self._executor is not None and self._logger is not None
        o = event.get_object() or {}
        sql = self.to_sql_from_object("INSERT", data, event.table, o, None)
        self._pending.append(sql)
        self._count += 1
        self._size += len(sql.encode("utf-8"))  # PARITY: Go len(sql) is the UTF-8 byte length
        if self._size >= MAX_BYTES_SIZE_INSERT or self._import_config.single:
            self._flush_import()

    def import_completed(self) -> None:
        """PARITY: ImportCompleted — final flush (does NOT reset; end of run)."""
        assert self._executor is not None and self._logger is not None
        if self._size > 0:
            batch = "".join(self._pending)
            try:
                self._executor(batch)
            except Exception as e:
                self._logger.error("offending sql: %s", batch)
                raise ValueError(f"unable to execute sql: {e}") from e

    def _flush_import(self) -> None:
        assert self._executor is not None and self._logger is not None
        batch = "".join(self._pending)
        try:
            self._executor(batch)
        except Exception as e:
            self._logger.error("offending sql: %s", batch)
            raise ValueError(f"unable to execute sql: {e}") from e
        self._pending = []
        self._size = 0

    # ---- help / config / validate ----

    def configuration(self) -> list[DriverField]:
        """PARITY: Configuration — NewDatabaseConfiguration(default_port)."""
        return new_database_configuration(self.default_port())

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        """PARITY: Validate — URLFromDatabaseConfiguration(scheme, default_port, values)."""
        return url_from_database_configuration(self.validate_scheme(), self.default_port(), values)
