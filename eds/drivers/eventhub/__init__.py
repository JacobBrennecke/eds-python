"""PARITY: internal/drivers/eventhub/eventhub.go — the Azure Event Hubs streaming driver + importer.

Events are accumulated in a util.Batcher and, on Flush, coalesced into EventDataBatches — consecutive records
that share a partition key (``<table>.<company|NONE>.<location|NONE>.<id>``) go into the same batch. Each
EventData carries the JSON-encoded event as its body, the event id as MessageID, ``application/json`` content
type, and the ``dbchange.…`` key as the ``objectId`` property. The pure connection-string / key /
partition-key / batch-coalescing / Validate logic lives here as module functions (the C# port isolates it as
EventHubSink.cs) and is unit-testable WITHOUT azure-eventhub.

LAZY-import azure.eventhub: the SDK is imported only inside ``_connect`` (mirrors snowflake's lazy connector),
so the unit/golden tests run with azure-eventhub absent.

NOTE: there is no local Event Hubs emulator, so there is NO e2e test (per migration note) — the connection
string, keys, partition keys, and batch coalescing are unit-tested; the SDK send is the untestable binding.
"""

from __future__ import annotations

from typing import Any

from eds.dbchange import DBChangeEvent
from eds.driver import (
    DriverConfig,
    DriverField,
    FieldError,
    ImporterConfig,
    get_required_string_value,
    new_field_error,
    required_string_field,
)
from eds.schema import Schema, SchemaMap
from eds.util import gourl
from eds.util.batcher import Batcher, Record
from eds.util.gojson import stringify
from eds.util.help import generate_help_section
from eds.util.logger import Logger

_MAX_IMPORT_BATCH_SIZE = 100
_CONTENT_TYPE = "application/json"


def str_with_def(val: str | None, default: str) -> str:
    """PARITY: strWithDef — default when the pointer is nil or the string is empty."""
    if val is None or val == "":
        return default
    return val


def parse_connection_string(url_string: str) -> str:
    """PARITY: ParseConnectionString — rewrite the URL scheme to "sb" and prefix "Endpoint=" ."""
    try:
        u = gourl.parse(url_string)
    except ValueError as e:
        raise ValueError(f"unable to parse url: {e}") from e  # PARITY: fmt.Errorf("unable to parse url: %w")
    u.scheme = "sb"
    return "Endpoint=" + str(u)


def new_partition_key(table: str, company_id: str | None, location_id: str | None, id: str) -> str:
    """PARITY: NewPartitionKey — <table>.<company|NONE>.<location|NONE>.<id>."""
    return f"{table}.{str_with_def(company_id, 'NONE')}.{str_with_def(location_id, 'NONE')}.{id}"


def get_keys(table: str, operation: str, company_id: str, location_id: str, id: str) -> tuple[str, str]:
    """PARITY: getKeys — (objectId key, partition key)."""
    key = (
        f"dbchange.{table}.{operation}."
        f"{str_with_def(company_id, 'NONE')}.{str_with_def(location_id, 'NONE')}.{id}"
    )
    pkey = new_partition_key(table, company_id, location_id, id)
    return key, pkey


class EventBatchGroup:
    """One coalesced send group: a partition key + its (record, objectId-key) events."""

    def __init__(self, partition_key: str) -> None:
        self.partition_key = partition_key
        self.events: list[tuple[Record, str]] = []


def plan_batches(records: list[Record]) -> list[EventBatchGroup]:
    """PARITY: the Flush batch grouping — coalesce only CONSECUTIVE records that share a partition key (a key
    that reappears after a different key starts a NEW group). company/location are read from Object as strings."""
    groups: list[EventBatchGroup] = []
    pending_partition_key: str | None = None
    for record in records:
        company_id = ""
        location_id = ""
        if record.object is not None:
            c = record.object.get("companyId")
            if isinstance(c, str):
                company_id = c
            loc = record.object.get("locationId")
            if isinstance(loc, str):
                location_id = loc
        key, pkey = get_keys(record.table, record.operation, company_id, location_id, record.id)
        if pending_partition_key == pkey and groups:
            groups[-1].events.append((record, key))
        else:
            group = EventBatchGroup(pkey)
            group.events.append((record, key))
            groups.append(group)
            pending_partition_key = pkey
    return groups


def validate_config(values: dict[str, Any]) -> tuple[str, list[FieldError]]:
    """PARITY: Validate — Connection String must be ``Endpoint=<scheme>://…``; returns ``eventhub://<rest>``."""
    val, field_error = get_required_string_value("Connection String", values)
    if field_error is not None:
        return "", [field_error]
    if not val.startswith("Endpoint="):
        return "", [new_field_error("Connection String", "expected to start with the prefix Endpoint=")]
    i = val.find("://")
    if i < 0:
        return "", [new_field_error("Connection String", "expected a url scheme after Endpoint= prefix")]
    return "eventhub://" + val[i + 3 :], []


class EventHubDriver:
    """PARITY: eventHubDriver."""

    def __init__(self) -> None:
        self._config: DriverConfig | None = None
        self._logger: Logger | None = None
        self._batcher: Batcher = Batcher()
        self._producer: Any = None  # azure EventHubProducerClient (lazy)
        self._import_config: ImporterConfig | None = None
        self._dry_run = False
        self._stopped = False

    # ---- connection ----
    def _connect(self, url_string: str) -> None:
        """PARITY: connect — build the producer from the parsed connection string. Errors (parse + client
        creation) are wrapped "error connecting to eventhub: …" (Go connect's fmt.Errorf wrap).

        azure.eventhub is imported HERE so unit/golden tests run without it installed."""
        from azure.eventhub import EventHubProducerClient  # noqa: PLC0415 — lazy

        try:
            conn = parse_connection_string(url_string)
            self._producer = EventHubProducerClient.from_connection_string(conn)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"error connecting to eventhub: {e}") from e

    # ---- lifecycle ----
    def start(self, config: DriverConfig) -> None:
        """PARITY: Start."""
        assert config.logger is not None
        self._config = config
        self._batcher = Batcher()
        self._logger = config.logger.with_prefix("[eventhub]")
        self._connect(config.url)
        self._logger.info("started")

    def stop(self) -> None:
        """PARITY: Stop — flush, then close the producer (once)."""
        if self._logger is not None:
            self._logger.debug("stopping")
        if self._stopped:
            return
        self._stopped = True
        assert self._logger is not None
        self.flush(self._logger)
        if self._producer is not None:
            self._producer.close()
        self._logger.debug("stopped")

    def max_batch_size(self) -> int:
        """PARITY: MaxBatchSize — no limit."""
        return -1

    # ---- streaming ----
    def process(self, logger: Logger, event: DBChangeEvent) -> bool:
        """PARITY: Process — batch the event."""
        self._batcher.add(event)
        return False

    def _snapshot_and_plan(self) -> tuple[int, list[EventBatchGroup]]:
        """PARITY: read the batched records, capture the count, clear the batcher, then plan the groups
        (read-before-clear ordering, since Records() returns the live list)."""
        records = list(self._batcher.records())
        count = len(records)
        if count == 0:
            return 0, []
        self._batcher.clear()
        return count, plan_batches(records)

    def flush(self, logger: Logger) -> None:
        """PARITY: Flush — build one EventDataBatch per coalesced group, then send (or dry-run log) each."""
        logger.debug("flush")
        count, groups = self._snapshot_and_plan()
        if count == 0:
            return

        from azure.eventhub import EventData  # noqa: PLC0415

        built = []
        for group in groups:
            batch = self._producer.create_batch(partition_key=group.partition_key)
            for record, object_id_key in group.events:
                assert record.event is not None
                data = EventData(stringify(record.event).encode("utf-8"))
                data.message_id = record.event.id
                data.content_type = _CONTENT_TYPE
                data.properties = {"objectId": object_id_key}
                batch.add(data)
            built.append(batch)

        offset = 0
        for batch in built:
            if self._dry_run:
                logger.trace(
                    "would send batch (%03d/%03d) with count: %d, bytes: %d",
                    1 + offset, count, len(batch), batch.size_in_bytes,
                )
            else:
                logger.trace(
                    "sending batch (%03d/%03d) with count: %d, bytes: %d",
                    1 + offset, count, len(batch), batch.size_in_bytes,
                )
                self._producer.send_batch(batch)
            offset += len(batch)

    def test(self, ctx: Any, logger: Logger, url: str) -> None:
        """PARITY: Test — connect then close the producer (Go ignores the logger arg; this instance is a
        throwaway, so don't mutate self._logger)."""
        self._connect(url)
        if self._producer is not None:
            self._producer.close()

    # ---- DriverHelp ----
    def name(self) -> str:
        return "Microsoft Azure EventHub"

    def description(self) -> str:
        return "Supports streaming EDS messages to a Microsoft Azure EventHub."

    def example_url(self) -> str:
        return (
            "eventhub://my-eventhub.servicebus.windows.net/;SharedAccessKeyName=send;"
            "SharedAccessKey=YXNkZmFzZGZhc2RmYXNkZmFzZGZhcwo=;EntityPath=my-eventhub"
        )

    def help(self) -> str:
        return (
            generate_help_section(
                "Partitioning",
                "The partition key is calculated automatically based on the number of partitions for the "
                "topic and the incoming message.\nThe partition key is in the format: "
                "[TABLE].[COMPANY_ID].[LOCATION_ID].[PRIMARY_KEY].\n",
            )
            + "\n"
            + generate_help_section(
                "Message Value", "The message value is a JSON encoded value of the EDS DBChange event."
            )
        )

    # ---- import Handler ----
    def create_datasource(self, schema: SchemaMap) -> None:
        """PARITY: CreateDatasource — no-op."""

    def import_event(self, event: DBChangeEvent, schema: Schema) -> None:
        """PARITY: ImportEvent — batch; flush at the 100-event threshold."""
        self._batcher.add(event)
        if len(self._batcher) >= _MAX_IMPORT_BATCH_SIZE:
            assert self._logger is not None
            self.flush(self._logger)

    def import_completed(self) -> None:
        """PARITY: ImportCompleted — flush (the batcher is always set in run_import)."""
        assert self._logger is not None
        self.flush(self._logger)

    def run_import(self, config: ImporterConfig) -> None:
        """PARITY: Import."""
        if config.schema_only:
            return
        assert config.logger is not None
        self._logger = config.logger.with_prefix("[eventhub]")
        self._connect(config.url)
        if self._config is None:
            self._config = DriverConfig()
        self._config.context = config.context
        self._dry_run = config.dry_run
        self._import_config = config
        self._batcher = Batcher()
        from eds.importer import run as importer_run  # noqa: PLC0415

        importer_run(self._logger, config, self)

    def supports_delete(self) -> bool:
        return False

    # ---- config ----
    def configuration(self) -> list[DriverField]:
        return [
            required_string_field(
                "Connection String", "The connection string primary key from the Event Hub console.", None
            )
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        """PARITY: Validate."""
        return validate_config(values)


__all__ = [
    "EventBatchGroup",
    "EventHubDriver",
    "get_keys",
    "new_partition_key",
    "parse_connection_string",
    "plan_batches",
    "str_with_def",
    "validate_config",
]
