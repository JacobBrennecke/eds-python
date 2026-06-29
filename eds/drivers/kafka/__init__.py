"""PARITY: internal/drivers/kafka/kafka.go — the Kafka streaming driver + importer.

Each DBChangeEvent is produced to a topic as a JSON value with a ``dbchange.<table>.<op>.<company>.<location>.
<id>`` key and an ``eds-partitionkey`` header (``<table>.<company>.<location>.<pk>``). The pure key /
partition-key / balancer / Validate logic lives here as module functions (the C# port isolates it as
KafkaSink.cs) and is golden-testable WITHOUT confluent-kafka.

LAZY-import confluent_kafka: the client is imported only inside ``_connect`` (mirrors snowflake's lazy
connector), so the unit/golden tests run with confluent-kafka absent.

DEVIATION: see DEVIATIONS.md#kafka-explicit-partition — Go uses segmentio/kafka-go's custom Balancer (the
broker partitioner consults the live partition list per send); librdkafka (confluent-kafka) can't host a
managed partitioner, so the port computes the partition itself from the topic metadata and produces to that
explicit partition — preserving Go's header-based ordering (same as the C# port).
"""

from __future__ import annotations

import time
from typing import Any

from eds.dbchange import DBChangeEvent
from eds.driver import (
    DriverConfig,
    DriverField,
    FieldError,
    ImporterConfig,
    get_optional_int_value,
    get_required_string_value,
    optional_number_field,
    required_string_field,
)
from eds.schema import Schema, SchemaMap
from eds.util import gourl
from eds.util.gojson import stringify
from eds.util.hash import hash as eds_hash
from eds.util.hash import modulo
from eds.util.help import generate_help_section
from eds.util.logger import Logger

EDS_PARTITION_KEY_HEADER = "eds-partitionkey"
_MAX_IMPORT_BATCH_SIZE = 1_000


def str_with_def(val: str | None, default: str) -> str:
    """PARITY: strWithDef — default when the pointer is nil or the string is empty."""
    if val is None or val == "":
        return default
    return val


def message_key(table: str, operation: str, company_id: str | None, location_id: str | None, id: str) -> str:
    """PARITY: the message key — dbchange.<table>.<op>.<company|NONE>.<location|NONE>.<id>."""
    return (
        f"dbchange.{table}.{operation}."
        f"{str_with_def(company_id, 'NONE')}.{str_with_def(location_id, 'NONE')}.{id}"
    )


def partition_key(table: str, company_id: str | None, location_id: str | None, primary_key: str) -> str:
    """PARITY: the partition key — <table>.<company|NONE>.<location|NONE>.<pk>."""
    return f"{table}.{str_with_def(company_id, 'NONE')}.{str_with_def(location_id, 'NONE')}.{primary_key}"


def balance(partition_key_header: str | None, msg_key: str, partition_count: int) -> int:
    """PARITY: messageBalancer.Balance — single partition → 0; else FNV-mod the hash of the eds-partitionkey
    header value (falling back to the message key) by the partition count."""
    if partition_count == 1:
        return 0
    value = partition_key_header if partition_key_header is not None else msg_key
    return modulo(eds_hash(value), partition_count)


def is_leader_not_available(e: BaseException) -> bool:
    """PARITY (intent): the leader-not-available retry decision Go makes via `strings.Contains(err.Error(),
    "Leader Not Available")` (segmentio/kafka-go's title-cased text). DEVIATION: see
    DEVIATIONS.md#kafka-leader-retry — confluent_kafka/librdkafka emits a KafkaError with code
    LEADER_NOT_AVAILABLE and the lowercase text "Broker: Leader not available", so we match the error CODE
    first (the robust signal) and fall back to a case-insensitive substring (covers wrapped/string errors)."""
    try:
        from confluent_kafka import KafkaError, KafkaException  # noqa: PLC0415
    except Exception:  # noqa: BLE001 — confluent-kafka not installed (unit/golden path)
        KafkaError = KafkaException = None  # type: ignore[assignment,misc]
    if KafkaException is not None and isinstance(e, KafkaException) and e.args:
        err = e.args[0]
        if hasattr(err, "code") and callable(err.code) and err.code() == KafkaError.LEADER_NOT_AVAILABLE:
            return True
    return "leader not available" in str(e).lower()


def validate_config(values: dict[str, Any]) -> tuple[str, list[FieldError]]:
    """PARITY: Validate — kafka://<host>:<port>/<topic> (default port 9092)."""
    field_errors: list[FieldError] = []
    hostname, field_error = get_required_string_value("Hostname", values)
    if field_error is not None:
        field_errors.append(field_error)
    port = get_optional_int_value("Port", 9092, values)
    topic, field_error = get_required_string_value("Topic", values)
    if field_error is not None:
        field_errors.append(field_error)
    if field_errors:
        return "", field_errors
    return f"kafka://{hostname}:{port}/{topic}", []


class KafkaDriver:
    """PARITY: kafkaDriver."""

    def __init__(self) -> None:
        self._config: DriverConfig | None = None
        self._logger: Logger | None = None
        self._producer: Any = None  # confluent_kafka.Producer (lazy)
        self._topic = ""
        self._host = ""
        self._partition_count = 0  # 0 = not yet resolved
        self._import_config: ImporterConfig | None = None
        self._pending: list[tuple[bytes, bytes, str]] = []  # (key, value, partition_key_header)

    # ---- connection ----
    def _connect(self, url_string: str) -> None:
        """PARITY: connect — parse url, require a topic path, build the producer.

        confluent_kafka is imported HERE so unit/golden tests run without it installed."""
        u = gourl.parse(url_string)
        if u.path == "":
            raise ValueError("kafka url requires a path which is the topic")
        self._host = u.host
        self._topic = u.path[1:]  # PARITY: trim leading slash
        self._partition_count = 0

        from confluent_kafka import Producer  # noqa: PLC0415 — lazy: keep off the unit/golden import path

        self._producer = Producer(
            {
                "bootstrap.servers": self._host,
                "acks": "all",  # PARITY: RequireAll
                "message.send.max.retries": 25,  # PARITY: MaxAttempts
                "allow.auto.create.topics": True,  # PARITY: AllowAutoTopicCreation
            }
        )

    # ---- lifecycle ----
    def start(self, config: DriverConfig) -> None:
        """PARITY: Start."""
        assert config.logger is not None
        self._config = config
        self._logger = config.logger.with_prefix("[kafka]")
        self._connect(config.url)
        self._logger.info("started")

    def stop(self) -> None:
        """PARITY: Stop — drain + close the producer."""
        if self._logger is not None:
            self._logger.debug("stopping")
        if self._producer is not None:
            self._producer.flush(10)
            self._producer = None
        if self._logger is not None:
            self._logger.debug("stopped")

    def max_batch_size(self) -> int:
        """PARITY: MaxBatchSize — no limit."""
        return -1

    # ---- streaming ----
    def _process(self, event: DBChangeEvent, dry_run: bool) -> None:
        key = message_key(event.table, event.operation, event.company_id, event.location_id, event.id)
        pk = event.get_primary_key()
        pkey = partition_key(event.table, event.company_id, event.location_id, pk)
        if dry_run:
            assert self._logger is not None
            self._logger.trace("would store key: %s, partition key: %s", key, pkey)
            return
        self._pending.append((key.encode("utf-8"), stringify(event).encode("utf-8"), pkey))
        if len(self._pending) >= _MAX_IMPORT_BATCH_SIZE:
            assert self._logger is not None
            self.flush(self._logger)

    def process(self, logger: Logger, event: DBChangeEvent) -> bool:
        """PARITY: Process."""
        self._process(event, False)
        return False

    def _resolve_partition_count(self) -> int:
        """PARITY: the balancer routes against the live partition list — resolve it from topic metadata. We
        must NOT silently default to 1 (that would misroute every message to partition 0); a failure to
        resolve propagates so Flush preserves _pending and NAKs (matching Go's WriteMessages error path)."""
        if self._partition_count > 0:
            return self._partition_count
        md = self._producer.list_topics(self._topic, timeout=10)
        topic_md = md.topics.get(self._topic)
        if topic_md is None:
            raise RuntimeError(f"unable to resolve partition count for topic {self._topic}")
        if topic_md.error is not None:
            # PARITY: propagate the broker error (preserving its code) so a leader-not-available metadata error
            # is recognized as retryable by Flush (is_leader_not_available), mirroring Go's WriteMessages path.
            from confluent_kafka import KafkaException  # noqa: PLC0415

            raise KafkaException(topic_md.error)
        if not topic_md.partitions:
            raise RuntimeError(f"unable to resolve partition count for topic {self._topic}")
        self._partition_count = len(topic_md.partitions)
        return self._partition_count

    def _produce_all(self) -> None:
        from confluent_kafka import KafkaException  # noqa: PLC0415

        count = self._resolve_partition_count()
        errors: list[BaseException] = []

        def _cb(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(KafkaException(err))

        for key, value, pkey in self._pending:
            partition = balance(pkey, key.decode("utf-8"), count)
            self._producer.produce(
                topic=self._topic,
                partition=partition,
                key=key,
                value=value,
                headers=[(EDS_PARTITION_KEY_HEADER, pkey.encode("utf-8"))],
                on_delivery=_cb,
            )
        self._producer.flush(10)
        if errors:
            raise errors[0]

    def flush(self, logger: Logger) -> None:
        """PARITY: Flush — retry the whole batch up to 10s while the leader is unavailable; any other error
        propagates (NAK) with _pending preserved; clear _pending on success or 10s timeout."""
        if not self._pending:
            return
        started = time.monotonic()
        while time.monotonic() - started < 10.0:
            try:
                self._produce_all()
                logger.debug("flushed %d messages", len(self._pending))
                break
            except Exception as e:  # noqa: BLE001
                if is_leader_not_available(e):
                    logger.debug("waiting for kafka to become available")
                    time.sleep(1.0)
                    continue
                raise RuntimeError(f"error publishing message. {e}") from e
        self._pending = []

    def test(self, ctx: Any, logger: Logger, url: str) -> None:
        """PARITY: Test — connect then close the producer."""
        self._logger = logger.with_prefix("[kafka]")
        self._connect(url)
        if self._producer is not None:
            self._producer.flush(10)
            self._producer = None

    # ---- DriverHelp ----
    def name(self) -> str:
        return "Kafka"

    def description(self) -> str:
        return "Supports streaming EDS messages to a Kafka topic."

    def example_url(self) -> str:
        return "kafka://kafka:9092/topic"

    def help(self) -> str:
        return (
            generate_help_section(
                "Partitioning",
                "The partition key is calculated automatically based on the number of partitions for the "
                "topic and the incoming message.\nThe algorithm is to calculate a value (hash input) in the "
                "format: [TABLE].[COMPANY_ID].[LOCATION_ID].[PRIMARY_KEY]\nand use a hash function to generate "
                "a value modulo the number of topic partitions. This guarantees the correct ordering\nfor a "
                "given table and primary key while providing the ability to safely scale processing "
                "horizontally.\n",
            )
            + "\n"
            + generate_help_section(
                "Message Key",
                "The message key is computed in the format: "
                "dbchange.[TABLE].[OPERATION].[COMPANY_ID].[LOCATION_ID].[MESSAGE_ID].\n",
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
        """PARITY: ImportEvent."""
        dry_run = self._import_config.dry_run if self._import_config is not None else False
        self._process(event, dry_run)

    def import_completed(self) -> None:
        """PARITY: ImportCompleted — flush, then close the writer."""
        assert self._logger is not None
        self.flush(self._logger)
        if self._producer is not None:
            self._producer.flush(10)
            self._producer = None

    def run_import(self, config: ImporterConfig) -> None:
        """PARITY: Import."""
        if config.schema_only:
            return
        assert config.logger is not None
        self._logger = config.logger.with_prefix("[kafka]")
        self._import_config = config
        self._connect(config.url)
        from eds.importer import run as importer_run  # noqa: PLC0415

        importer_run(self._logger, config, self)

    def supports_delete(self) -> bool:
        return False

    # ---- config ----
    def configuration(self) -> list[DriverField]:
        return [
            required_string_field("Hostname", "The hostname or ip address to the kafka broker", None),
            optional_number_field("Port", "The port to connect to the kafka broker", 9092),
            required_string_field("Topic", "The kafka topic to stream data", None),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        """PARITY: Validate."""
        return validate_config(values)


__all__ = [
    "EDS_PARTITION_KEY_HEADER",
    "KafkaDriver",
    "balance",
    "is_leader_not_available",
    "message_key",
    "partition_key",
    "str_with_def",
    "validate_config",
]
