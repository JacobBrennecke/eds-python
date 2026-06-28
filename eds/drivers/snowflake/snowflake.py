"""PARITY: internal/drivers/snowflake/snowflake.go — the Snowflake driver.

Snowflake diverges from a plain SqlDriverBase: it batches Records (not raw SQL), runs RecordOptimize before
generating SQL, plans the flush as a single multi-statement exec, and uses the tracker to force a
delete-before-insert when an insert for a key was seen within 24h. plan_flush is the pure, golden-testable
core (dedup + statement count + cache/delete key planning). The real connection (snowflake-connector-python)
is unit-untestable (no account); it is lazily imported in open_db.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from eds.dbchange import DBChangeEvent
from eds.driver import (
    DriverConfig,
    DriverField,
    DriverStoppedError,
    FieldError,
    ImporterConfig,
    new_database_configuration,
    url_from_database_configuration,
)
from eds.drivers.snowflake import sql
from eds.schema import DatabaseSchema, Schema, SchemaMap, SchemaRegistry
from eds.util.batcher import Batcher, Record
from eds.util.logger import Logger
from eds.util.optimize import combine_records_with_same_primary_key, sort_records_by_mvcc_timestamp

_MAX_BATCH_SIZE = 200
_CACHE_TTL_SECONDS = 24 * 3600.0
# PARITY: SetSessionID accepts only a UUID-shaped id (lowercase hex), matched as a substring (Go MatchString).
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@dataclass
class FlushPlan:
    """PARITY: the planned flush — the multi-statement query, its statement count, and tracker key changes."""

    query: str = ""
    statement_count: int = 0
    cache_keys: list[str] = field(default_factory=list)
    delete_keys: list[str] = field(default_factory=list)


@runtime_checkable
class ISnowflakeDb(Protocol):
    """PARITY: the Snowflake DB seam (mirrors the C# ISnowflakeDb)."""

    def query_single_value(self, fn: str) -> str: ...
    def build_schema(self, catalog: str, schema: str, fail_if_empty: bool) -> DatabaseSchema: ...
    def exec_multi_statement(self, sql: str, statement_count: int) -> int: ...
    def exec(self, sql: str) -> None: ...
    def close(self) -> None: ...


def plan_flush(
    records: list[Record], registry: SchemaRegistry, tracker_has_key: Callable[[str], bool], logger: Logger
) -> FlushPlan:
    """PARITY: the per-record flush planning body (PURE) — update-noise skip, tracker-gated force-delete, and
    the delete-before-insert statement count. cache_keys gets INSERT+DELETE keys (record order); delete_keys
    gets only DELETE keys."""
    query = ""
    statement_count = 0
    cache_keys: list[str] = []
    delete_keys: list[str] = []
    for record in records:
        force = False
        key = ""
        op = record.operation
        if op == "INSERT":
            key = f"snowflake:{record.table}:{record.id}"
            force = tracker_has_key(key)
            if force:
                logger.trace(
                    "forcing delete before insert because we've seen an insert for %s/%s", record.table, record.id
                )
        elif op == "UPDATE":
            diff = record.diff or []
            just_updated_date = len(diff) == 1 and diff[0] == "updatedDate"
            just_updated_meta = len(diff) == 2 and "updatedDate" in diff and "meta" in diff
            no_updates = len(diff) == 0
            if just_updated_date or no_updates or just_updated_meta:
                logger.trace(
                    "skipping update because only updatedDate/meta changed for %s/%s", record.table, record.id
                )
                continue
        elif op == "DELETE":
            key = f"snowflake:{record.table}:{record.id}"
            delete_keys.append(key)
        _, version = registry.get_table_version(record.table)  # PARITY: Go ignores the found bool
        schema = registry.get_schema(record.table, version)
        s, c = sql.to_sql(record, schema, force)
        statement_count += c
        query += s
        if key != "":
            cache_keys.append(key)
    return FlushPlan(query, statement_count, cache_keys, delete_keys)


class SnowflakeDriver:
    """PARITY: snowflakeDriver."""

    def __init__(self) -> None:
        self._logger: Logger | None = None
        self._db: ISnowflakeDb | None = None
        self._registry: SchemaRegistry | None = None
        self._tracker: Any = None
        self._ctx: Any = None
        self._batcher = Batcher()
        self._dbname = ""
        self._schema_name = ""
        self._dbschema: DatabaseSchema = DatabaseSchema()
        self._session_id = ""
        self._sequence = 0
        self._stopped = False
        self._lock = threading.Lock()
        self._prof_last = 0.0
        self._prof_records = 0

    # ---- hooks / metadata ----
    def log_prefix(self) -> str:
        return "[snowflake]"

    def max_batch_size(self) -> int:
        return _MAX_BATCH_SIZE

    def name(self) -> str:
        return "Snowflake [DEPRECATED]"

    def description(self) -> str:
        return (
            "This driver is provided for legacy support of Snowflake username/password authentication. "
            "New Snowflake connections should use the Snowflake Key Pair driver."
        )

    def example_url(self) -> str:
        return "snowflake://user:password@host/database"

    def help(self) -> str:
        return ""

    def configuration(self) -> list[DriverField]:
        return new_database_configuration(-1)

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        return url_from_database_configuration("snowflake", -1, values)

    # ---- lifecycle ----
    def start(self, config: DriverConfig) -> None:
        assert config.logger is not None
        self._logger = config.logger.with_prefix(self.log_prefix())
        self._ctx = config.context
        self._registry = config.schema_registry
        self._tracker = config.tracker
        self._db = self._connect_to_db(config.context, config.url)

    def _connect_to_db(self, ctx: Any, url: str) -> ISnowflakeDb:
        from eds.drivers.snowflake.datadb import SnowflakeDataDb

        assert self._logger is not None
        db = SnowflakeDataDb.open_from_url(url, self._logger)
        try:
            self._refresh_schema(db, fail_if_empty=False)
        except Exception:
            db.close()
            raise
        return db

    def _refresh_schema(self, db: ISnowflakeDb, fail_if_empty: bool) -> None:
        if self._dbname == "":
            self._dbname = db.query_single_value("CURRENT_DATABASE()")
        if self._schema_name == "":
            self._schema_name = db.query_single_value("CURRENT_SCHEMA()")
        self._dbschema = db.build_schema(self._dbname, self._schema_name, fail_if_empty)

    def set_session_id(self, session_id: str) -> None:
        """PARITY: SetSessionID — accept only a UUID-shaped id."""
        if session_id != "" and _UUID_RE.search(session_id):
            self._session_id = session_id

    def stop(self) -> None:
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            if self._db is not None:
                self._db.close()
                self._db = None

    def test(self, ctx: Any, logger: Logger, url: str) -> None:
        self._logger = logger.with_prefix(self.log_prefix())
        db = self._connect_to_db(ctx, url)
        db.close()

    # ---- streaming ----
    def process(self, logger: Logger, event: DBChangeEvent) -> bool:
        logger.trace("processing event: %s", event)
        with self._lock:
            self._batcher.add(event)
        return False

    def flush(self, logger: Logger) -> None:
        """PARITY: Flush — RecordOptimize, plan, single multi-statement exec, then tracker bookkeeping."""
        assert self._registry is not None
        with self._lock:
            if self._db is None:
                raise DriverStoppedError()
            records = self._batcher.records()
            count = len(records)
            self._batcher.clear()  # PARITY: cleared BEFORE exec — no re-add on error
            if count == 0:
                return
            self._sequence += 1
            records = sort_records_by_mvcc_timestamp(records)
            records = combine_records_with_same_primary_key(records)
            tag = f"eds-{self._session_id}/{self._sequence}/{count}"
            plan = plan_flush(records, self._registry, lambda k: self._tracker.get_key(k)[0], logger)
            if plan.statement_count > 0:
                rows = self._db.exec_multi_statement(plan.query, plan.statement_count)
                if rows != plan.statement_count:
                    logger.warn("expected %d rows affected but got %d", plan.statement_count, rows)
            if plan.cache_keys:
                self._tracker.set_keys(plan.cache_keys, tag, _CACHE_TTL_SECONDS)
            if plan.delete_keys:
                self._tracker.delete_key(*plan.delete_keys)

    # ---- migration ----
    def migrate_new_table(self, ctx: Any, logger: Logger, schema: Schema) -> None:
        assert self._db is not None
        with self._lock:
            if schema.table in self._dbschema:
                logger.info("table already exists for: %s, dropping and recreating...", schema.table)
                self._db.exec("DROP TABLE IF EXISTS " + sql.quote_identifier(schema.table))
                if self._tracker is not None:
                    del_count = self._tracker.delete_keys_with_prefix("snowflake:" + schema.table + ":")
                    logger.debug("deleted %d cache keys for table %s", del_count, schema.table)
            create = sql.create_sql(schema)
            logger.trace("migrate new table: %s", create)
            self._db.exec(create)
            self._refresh_schema(self._db, fail_if_empty=True)

    def migrate_new_columns(self, ctx: Any, logger: Logger, schema: Schema, columns: list[str]) -> None:
        assert self._db is not None
        with self._lock:
            for stmt in sql.add_new_columns_sql(logger, columns, schema, self._dbschema):
                logger.trace("migrating new columns: %s", stmt)
                self._db.exec(stmt)
                logger.debug("migrated new columns: %s", stmt)
            self._refresh_schema(self._db, fail_if_empty=True)

    def get_destination_schema(self, ctx: Any, logger: Logger) -> DatabaseSchema:
        return self._dbschema

    def aliases(self) -> list[str]:
        return []

    def run_import(self, config: ImporterConfig) -> None:
        # PARITY: snowflake.go Import is a BULK load (CREATE STAGE + PUT + COPY INTO), not the per-event
        # importer.Run path the other drivers use — and it's unit-untestable (no Snowflake account). Deferred.
        raise NotImplementedError("snowflake bulk import (stage/PUT/COPY) is unit-untestable; follows")


# Re-export for type hints elsewhere.
__all__ = ["FlushPlan", "ISnowflakeDb", "SnowflakeDriver", "plan_flush", "SchemaMap"]
