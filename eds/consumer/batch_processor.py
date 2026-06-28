"""PARITY: the per-message decision state machine of internal/consumer/consumer.go's bufferer.

Following the reviewed C# split, all the load-bearing DECISIONS live here (flush triggers, skip, migration,
ack/nak) so they are unit-testable with zero NATS — against a fake Msg + fake driver. The async Bufferer
drains the queue and calls this; the Consumer does the NATS I/O.

Stop signals are exceptions (C# model): ConsumerStoppedError = a clean stop (driver stopped / ack failure);
ConsumerFatalError = a surfaced fatal error (out-of-order, decode, process/flush error) — both nak the pending
batch first. A successful process/flush returns normally.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from eds.dbchange import DBChangeEvent
from eds.driver import DriverStoppedError
from eds.schema import SchemaValidationError
from eds.util.json import json_diff
from eds.util.logger import Logger


class ConsumerStoppedError(Exception):
    """A clean stop (driver stopped, or an ack failure) — the batch was nak'd; not surfaced as an error."""


class ConsumerFatalError(Exception):
    """A fatal consumer error (out-of-order/decode/process/flush) — the batch was nak'd; surfaced via Error()."""


@dataclass
class MsgMetadata:
    """The JetStream metadata the bufferer needs."""

    consumer_seq: int
    stream_seq: int = 0
    num_delivered: int = 1
    num_pending: int = 0


class Msg(Protocol):
    """The message surface the BatchProcessor needs (a thin seam over a nats-py JetStream msg)."""

    @property
    def data(self) -> bytes: ...
    def metadata(self) -> MsgMetadata: ...
    async def ack(self) -> None: ...
    async def nak(self) -> None: ...


class BatchProcessor:
    """PARITY: the bufferer's per-message body + flush/skip/migration/ack-nak."""

    def __init__(
        self,
        driver: Any,
        registry: Any,
        validator: Any,
        metrics: Any,
        logger: Logger,
        *,
        max_batch: int,
        min_pending_latency: float,
        max_pending_latency: float,
        table_timestamps: dict[str, datetime | None] | None = None,
        supports_migration: bool = False,
        sequence: int = 0,
        ctx: Any = None,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._driver = driver
        self._registry = registry
        self._validator = validator
        self._metrics = metrics
        self._logger = logger
        self._max = max_batch
        self._min_pending_latency = min_pending_latency
        self._max_pending_latency = max_pending_latency
        self._table_timestamps = table_timestamps
        self._supports_migration = supports_migration
        self._sequence = sequence
        self._ctx = ctx
        self._now = now
        self._pending: list[Msg] = []
        self._pending_started: float | None = None

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def sequence(self) -> int:
        return self._sequence

    async def process_message(self, msg: Msg) -> None:
        """PARITY: bufferer Arm B — process one message, possibly flushing. Raises on stop paths."""
        log = self._logger
        try:
            m = msg.metadata()
        except Exception as e:  # noqa: BLE001 — PARITY: a metadata-parse error is a fatal nak (consumer.go:349)
            self._metrics.pending_events_dec()
            await self._handle_error(e)
            return
        log.trace("msg received - deliveries=%d,pending=%d", m.num_delivered, len(self._pending))
        self._pending.append(msg)  # PARITY: appended BEFORE the sequence check (so it joins the nak)
        if m.consumer_seq != self._sequence + 1:
            self._metrics.pending_events_dec()
            await self._handle_error(f"out of order sequence: {m.consumer_seq}, expected: {self._sequence + 1}")
        self._sequence = m.consumer_seq  # advances even for skipped events

        try:
            evt = DBChangeEvent.from_message(msg.data, m.consumer_seq)
        except Exception as e:  # noqa: BLE001
            self._metrics.pending_events_dec()
            log.error("error creating event: %s", e)
            await self._handle_error(e)

        if self.should_skip(evt):
            log.debug("skipping event")
            await self._ack(msg)  # PARITY: individual ack, decoupled from the batch
            self._remove_from_pending(msg)
            self._metrics.pending_events_dec()
            return

        force_flush = False
        if self._supports_migration:
            try:
                force_flush = self.handle_possible_migration(evt)
            except Exception as e:  # noqa: BLE001
                await self._handle_error(e)

        if evt.operation != "DELETE" and self._registry is not None:
            try:
                schema = self._registry.get_schema(evt.table, evt.model_version)
                obj = evt.get_object()
                if obj is not None:
                    diff = json_diff(obj, schema.columns())
                    if diff:
                        evt.omit_properties(*diff)
            except Exception as e:  # noqa: BLE001
                await self._handle_error(e)

        try:
            flush = self._driver.process(log, evt)
        except Exception as e:  # noqa: BLE001
            self._metrics.pending_events_dec()
            await self._handle_error(e)

        if flush or len(self._pending) >= self._max or force_flush:  # flush trigger #1
            await self.flush(log)
            return
        if self._pending_started is None:
            self._pending_started = self._now()
        try:  # catch-up bypass: keep accumulating under a large backlog
            num_pending = msg.metadata().num_pending
        except Exception:  # noqa: BLE001 — PARITY: Go ignores the catch-up metadata error (zero md → 0)
            num_pending = 0
        if num_pending > self._max and (self._now() - self._pending_started) < self._max_pending_latency * 2:
            return
        if (  # flush trigger #2 (the >= max arm is redundant; the live arm is the latency timeout)
            len(self._pending) >= self._max
            or (self._now() - self._pending_started) >= self._max_pending_latency
        ):
            await self.flush(log)

    async def on_idle(self) -> None:
        """PARITY: bufferer Arm C trigger #3 — flush a partial batch once it reaches min latency."""
        count = len(self._pending)
        if (
            0 < count < self._max
            and self._pending_started is not None
            and (self._now() - self._pending_started) >= self._min_pending_latency
        ):
            await self.flush(self._logger)

    async def flush(self, logger: Logger) -> None:
        """PARITY: flush — driver.Flush then ack the whole batch; metrics fire even when empty."""
        started = self._now()
        try:
            self._driver.flush(logger)
        except DriverStoppedError:
            await self.nack_everything()
            raise ConsumerStoppedError() from None  # clean stop, NOT a surfaced error
        except Exception as e:  # noqa: BLE001
            await self._handle_error(e)

        count = 0.0
        for m in self._pending:
            logger.trace("acknowledged message")
            try:
                await m.ack()
            except Exception as e:  # noqa: BLE001
                self._metrics.pending_events_dec()
                logger.error("error acking message: %s", e)
                await self.nack_everything()
                raise ConsumerStoppedError() from None
            self._metrics.pending_events_dec()
            count += 1
        if self._pending_started is not None:
            self._metrics.observe_processing_duration(self._now() - self._pending_started)
        self._metrics.observe_flush_duration(self._now() - started)
        self._metrics.observe_flush_count(count)
        self._pending = []
        self._pending_started = None

    def should_skip(self, evt: DBChangeEvent) -> bool:
        """PARITY: shouldSkip — export cutoff + schema validation (all skip+ack, never nak)."""
        if self._table_timestamps is not None:
            tt = self._table_timestamps.get(evt.table)
            if tt is not None:
                event_ts = datetime.fromtimestamp(evt.timestamp / 1000, tz=timezone.utc)
                if event_ts < tt:
                    return True
        if self._validator is not None:
            try:
                found, valid, path = self._validator.validate(evt)
            except SchemaValidationError as e:
                self._logger.debug(
                    "skipping %s, schema did not validate (%s)", evt.table, str(e).replace("\n", " ").strip()
                )
                return True
            except Exception as e:  # noqa: BLE001
                self._logger.error("error validating schema: %s", e)
                return True
            if not found:
                self._logger.trace("skipping %s, no schema found", evt.table)
                return True
            if not valid:
                self._logger.trace("skipping %s, schema did not validate", evt.table)
                return True
            if path != "":
                evt.schema_validated_path = path
                self._logger.trace("schema validated %s", path)
        return False

    def handle_possible_migration(self, evt: DBChangeEvent) -> bool:
        """PARITY: handlePossibleMigration — returns True (force a flush) when a migration ran."""
        found, version = self._registry.get_table_version(evt.table)
        if found and version == evt.model_version:
            return False
        newschema = self._registry.get_schema(evt.table, evt.model_version)
        if not found:
            self._driver.migrate_new_table(self._ctx, self._logger, newschema)
            self._registry.set_table_version(evt.table, evt.model_version)
            self._logger.info("migrated new table %s", evt.table)
            return True
        oldschema = self._registry.get_schema(evt.table, version)
        old_cols = oldschema.columns()
        columns = [c for c in newschema.columns() if c not in old_cols]
        if columns:
            self._driver.migrate_new_columns(self._ctx, self._logger, newschema, columns)
            if evt.diff is None:
                evt.diff = []
            for col in columns:
                if col not in evt.diff:
                    evt.diff.append(col)
            self._registry.set_table_version(evt.table, evt.model_version)
            self._logger.info("migrated table %s", evt.table)
            return True
        # QUIRK: version differs but no new columns → do NOT set_table_version (re-diffed every event).
        self._logger.info("new table %s but no new columns added", evt.table)
        return False

    async def nack_everything(self) -> None:
        """PARITY: nackEverything — nak all pending, clear the batch."""
        for m in self._pending:
            try:
                await m.nak()
            except Exception as e:  # noqa: BLE001
                self._logger.error("error naking message: %s", e)
        self._pending = []
        self._pending_started = None

    async def _handle_error(self, err: object) -> None:
        self._logger.error("error: %s", err)
        await self.nack_everything()
        raise ConsumerFatalError(str(err))

    async def _ack(self, msg: Msg) -> None:
        try:
            await msg.ack()
        except Exception as e:  # noqa: BLE001
            self._logger.error("error acking message: %s", e)

    def _remove_from_pending(self, msg: Msg) -> None:
        for i, m in enumerate(self._pending):
            if m is msg:
                del self._pending[i]
                return
