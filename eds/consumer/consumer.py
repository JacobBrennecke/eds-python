"""PARITY: internal/consumer/consumer.go — the NATS/JetStream I/O around the Bufferer + BatchProcessor.

DEVIATION (pull-fetch-loop): nats-py has no jetstream.Consume callback, so the producer is a manual pull
fetch loop (still a pull consumer — the push-vs-pull decision is preserved). The dbchange data path is raw
JSON via DBChangeEvent.from_message (NOT decode_nats_msg). The consumer does NOT Start/Stop the driver (the
runner's job); it only Process/Flush/MaxBatchSize (+ migration) and SetSessionID once.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import datetime, timezone
from typing import Any

import msgpack
import nats.errors
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError

from eds.consumer.batch_processor import BatchProcessor, MsgMetadata
from eds.consumer.bufferer import Bufferer
from eds.consumer.config import (
    ConsumerConfig as EdsConsumerConfig,
)
from eds.consumer.config import (
    batch_max,
    durable_name,
    earliest_timestamp,
    filter_subjects,
    validate_company_ids,
)
from eds.consumer.connection import new_nats_connection
from eds.driver import DriverMigration, DriverSessionHandler
from eds.util.hash import hash as eds_hash
from eds.util.logger import Logger

_STREAM = "dbchange"
# DEVIATION (pull-fetch-loop): Go's jetstream.Consume streams msgs as they arrive within a 30s pull window;
# nats-py's fetch() instead BLOCKS up to its timeout trying to fill `batch`, so a short expiry is used to keep
# a partial batch flowing promptly (the BatchProcessor's min/max latency still governs flush batching).
_FETCH_TIMEOUT = 1.0


class ConsumerAlreadyRunningError(Exception):
    """PARITY: ErrConsumerAlreadyRunning — another consumer is already bound to the durable (NumWaiting > 0)."""


def update_destination_schema(logger: Logger, registry: Any, driver: Any, ctx: Any = None) -> None:
    """PARITY: internal.UpdateDestinationSchema — reconcile the driver's destination schema with the registry
    once at startup (create missing tables, add missing columns)."""
    try:
        schema = registry.get_latest_schema()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"error getting latest schema: {e}") from e
    dest = driver.get_destination_schema(ctx, logger)
    for table, sch in schema.items():
        try:
            found, version = registry.get_table_version(table)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"error getting table version for table: {table}: {e}") from e
        if not found:
            raise RuntimeError(f"error getting table version for table: {table}: not found")
        logger.trace("updating destination schema for table: %s, version: %s", table, version)
        if table not in dest:
            driver.migrate_new_table(ctx, logger, sch)
            continue
        new_columns = [c for c in sch.columns() if c not in dest[table]]
        if not new_columns:
            logger.trace("no new columns to migrate for table: %s", table)
            continue
        driver.migrate_new_columns(ctx, logger, sch, new_columns)


class NatsMsg:
    """Adapts a nats-py JetStream message to the BatchProcessor Msg seam."""

    def __init__(self, msg: Any) -> None:
        self._msg = msg

    @property
    def data(self) -> bytes:
        return self._msg.data

    def metadata(self) -> MsgMetadata:
        md = self._msg.metadata
        return MsgMetadata(
            consumer_seq=md.sequence.consumer,
            stream_seq=md.sequence.stream,
            num_delivered=md.num_delivered,
            num_pending=md.num_pending or 0,
        )

    async def ack(self) -> None:
        await self._msg.ack()

    async def nak(self) -> None:
        await self._msg.nak()


class Consumer:
    """PARITY: the EDS consumer — NATS connection + durable + the producer/bufferer/heartbeat tasks."""

    def __init__(self, config: EdsConsumerConfig, logger: Logger, metrics: Any) -> None:
        self._config = config
        self._logger = logger
        self._metrics = metrics
        self._nc: Any = None
        self._js: Any = None
        self._psub: Any = None
        self._session_id = ""
        self._company_ids: list[str] = []
        self._durable = ""
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.effective_max_ack_pending())
        self._processor: BatchProcessor | None = None
        self._cancel = asyncio.Event()
        self._stopping = False
        self._running = False
        self._stop_lock = asyncio.Lock()
        self._consume_task: asyncio.Task | None = None
        self._bufferer_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._error: BaseException | None = None
        self._fatal = asyncio.Event()
        self._disconnected = asyncio.Event()
        self._offset = 0
        self._started_at = 0.0
        self._paused_at: Any = None

    # ---- accessors ----
    @property
    def session_id(self) -> str:
        return self._session_id

    def error(self) -> BaseException | None:
        return self._error

    def fatal(self) -> asyncio.Event:
        """An awaitable set when a fatal consumer error occurs (the error is on error())."""
        return self._fatal

    def disconnected(self) -> asyncio.Event:
        return self._disconnected

    # ---- bootstrap ----
    async def create(self) -> None:
        """PARITY: CreateConsumer — connect, resolve the durable (create/update), wire the driver + processor."""
        self._started_at = time.monotonic()  # PARITY: uptime measured from creation

        async def _disconnected_cb() -> None:
            if not self._stopping:
                self._logger.error("nats disconnected: %s", self._config.url)
                self._disconnected.set()
                asyncio.create_task(self.stop())

        async def _closed_cb() -> None:
            if not self._stopping:
                self._logger.info("nats closed: %s", self._config.url)
                self._disconnected.set()
                asyncio.create_task(self.stop())

        async def _reconnected_cb() -> None:
            self._logger.info("nats reconnect: %s", self._config.url)

        self._nc, info = await new_nats_connection(
            self._logger, self._config.url, self._config.credentials,
            disconnected_cb=_disconnected_cb, closed_cb=_closed_cb, reconnected_cb=_reconnected_cb,
        )
        self._session_id = info.session_id
        # PARITY: consumer.go:771 — log the resolved credential info at startup (always fires).
        self._logger.info(
            "using info from credentials, server: %s companies: %s, session: %s",
            info.server_id, info.company_ids, info.session_id,
        )
        # PARITY: company-id overrides are validated strictly against the credentials (every override must be
        # present; no "*" special-case) — Go errors otherwise rather than silently widening/narrowing.
        if self._config.company_ids:
            try:
                self._company_ids = validate_company_ids(self._config.company_ids, info.company_ids)
            except ValueError:
                await self._nc.close()
                raise
            self._logger.debug("using override company IDs: %s", self._company_ids)
        else:
            self._company_ids = info.company_ids

        driver = self._config.driver
        if isinstance(driver, DriverSessionHandler):
            driver.set_session_id(self._session_id)

        self._js = self._nc.jetstream()
        self._durable = durable_name(info.server_id, self._config.suffix)
        max_ = batch_max(self._config)
        # DEVIATION: Go sets MaxRequestBatch on the consumer config (server caps the pull batch); nats-py's
        # ConsumerConfig has no such field, so the pull batch is bounded client-side via fetch(batch=) below.
        cfg = ConsumerConfig(
            durable_name=self._durable, max_ack_pending=max_, max_deliver=20, ack_wait=300,
            filter_subjects=filter_subjects(self._company_ids),
            ack_policy=AckPolicy.EXPLICIT, inactive_threshold=259200, max_waiting=1,
        )
        try:
            existing = await self._js.consumer_info(_STREAM, self._durable)
            cfg.deliver_policy = existing.config.deliver_policy  # immutable on update — preserve
            cfg.opt_start_time = existing.config.opt_start_time
            cfg.max_waiting = existing.config.max_waiting
        except NotFoundError:
            start_at = earliest_timestamp(self._config.export_table_timestamps)
            if self._config.deliver_all:
                cfg.deliver_policy = DeliverPolicy.ALL
            elif start_at is not None:
                cfg.deliver_policy = DeliverPolicy.BY_START_TIME
                cfg.opt_start_time = start_at  # nats-py ConsumerConfig.opt_start_time is a datetime
            else:
                cfg.deliver_policy = DeliverPolicy.NEW
                self._logger.warn("no import timestamp found, starting data stream from now")

        await self._js.add_consumer(_STREAM, config=cfg)
        ci = await self._js.consumer_info(_STREAM, self._durable)
        if ci.num_waiting and ci.num_waiting > 0:
            await self._nc.close()
            raise ConsumerAlreadyRunningError()
        sequence = ci.delivered.consumer_seq
        # PARITY: consumer.go:893 — log the connected url last, after consumer setup succeeds.
        self._logger.info("nats connected: %s", self._config.url)

        self._processor = BatchProcessor(
            driver, self._config.registry, self._config.schema_validator, self._metrics, self._logger,
            max_batch=max_,
            min_pending_latency=self._config.effective_min_pending_latency(),
            max_pending_latency=self._config.effective_max_pending_latency(),
            table_timestamps=self._config.export_table_timestamps,
            supports_migration=isinstance(driver, DriverMigration),
            sequence=sequence,
        )
        # PARITY: reconcile the destination schema once at startup for migration-capable drivers.
        if isinstance(driver, DriverMigration) and self._config.registry is not None:
            update_destination_schema(self._logger, self._config.registry, driver)
        if self._config.session_id_callback is not None:
            self._config.session_id_callback(self._session_id)

    # ---- run / pause ----
    async def start(self) -> None:
        """PARITY: start — unpause + spawn the bufferer and heartbeat tasks."""
        if self._running:
            raise RuntimeError("consumer already started")
        self._running = True
        await self.unpause()
        self._bufferer_task = asyncio.create_task(self._run_bufferer())
        self._heartbeat_task = asyncio.create_task(self._send_heartbeats())

    async def unpause(self) -> None:
        if self._psub is not None:
            raise RuntimeError("consumer already started")
        self._psub = await self._js.pull_subscribe_bind(durable=self._durable, stream=_STREAM)
        self._consume_task = asyncio.create_task(self._consume_loop())
        self._paused_at = None

    async def pause(self) -> None:
        if self._consume_task is not None:
            self._consume_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._consume_task
            self._consume_task = None
        if self._psub is not None:
            with contextlib.suppress(Exception):
                await self._psub.unsubscribe()
            self._psub = None
        self._paused_at = datetime.now(timezone.utc)

    async def _consume_loop(self) -> None:
        batch = self._config.effective_max_pending_buffer()
        while True:
            try:
                msgs = await self._psub.fetch(batch=batch, timeout=_FETCH_TIMEOUT)
            except asyncio.CancelledError:
                return
            except (nats.errors.TimeoutError, asyncio.TimeoutError):
                continue  # PARITY: empty pull window — refill (PullExpiry=30s, PullMaxMessages=4096)
            except Exception as e:  # noqa: BLE001 — Go's ConsumeErrHandler logs at warn and continues
                self._logger.warn("fetch error: %s", e)
                continue
            for m in msgs:
                self._metrics.pending_events_inc()
                self._metrics.total_events_inc()
                await self._queue.put(NatsMsg(m))

    async def _run_bufferer(self) -> None:
        assert self._processor is not None
        buf = Bufferer(
            self._queue, self._processor,
            empty_buffer_pause=self._config.effective_empty_buffer_pause(),
            logger=self._logger, cancel=self._cancel, on_fatal=self._set_fatal,
        )
        await buf.run()

    def _set_fatal(self, exc: BaseException) -> None:
        # DEVIATION (consumer-self-stops-on-fatal): Go surfaces the error on Error() and lets the OWNER (fork.go)
        # call Stop(); the Python consumer self-stops AND sets an awaitable fatal() so a future runner can react.
        self._error = exc
        self._fatal.set()
        asyncio.create_task(self.stop())

    # ---- heartbeat ----
    async def _send_heartbeats(self) -> None:
        interval = self._config.effective_heartbeat_interval()
        while not self._stopping:
            try:
                await self._publish_heartbeat()
            except Exception as e:  # noqa: BLE001 — Go logs heartbeat errors and continues
                self._logger.error("error sending heartbeat: %s", e)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return

    async def _publish_heartbeat(self) -> None:
        # PARITY: a stats-collection failure SKIPS the whole beat (it raises before the offset advances / publish),
        # it does not publish a beat without stats.
        stats = self._metrics.get_system_stats() if hasattr(self._metrics, "get_system_stats") else None
        hb: dict[str, Any] = {
            "sessionId": self._session_id,
            "offset": self._offset,
            "uptime": int(time.monotonic() - self._started_at),  # PARITY: seconds-as-int quirk
        }
        if stats is not None:
            hb["stats"] = json.loads(stats.__gojson__())
        if self._paused_at is not None:
            hb["paused"] = self._paused_at  # PARITY: native msgpack Timestamp (not a pre-stringified RFC3339)
        self._offset += 1
        msg_id = eds_hash(time.time_ns(), self._offset)
        await self._nc.publish(
            f"eds.client.{self._session_id}.heartbeat",
            msgpack.packb(hb, use_bin_type=True, datetime=True),
            headers={"Nats-Msg-Id": msg_id, "content-encoding": "msgpack"},
        )
        # PARITY: consumer.go:538 — trace the sent heartbeat.
        self._logger.trace("heartbeat sent %s with: %s", msg_id, json.dumps(hb, default=str))

    # ---- shutdown ----
    async def stop(self, graceful: bool = True) -> None:
        """PARITY: Stop (C# ordering) — cancel consume, drain (graceful: sentinel → final flush+ack; non-graceful:
        cancel event → nak residual), cancel heartbeat, close. Idempotent.

        Robust against a dead bufferer (the fatal path): the sentinel is only enqueued when the bufferer is still
        running, so a full queue + an already-exited bufferer can never deadlock the final put."""
        async with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        if self._consume_task is not None:
            self._consume_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._consume_task
        if self._psub is not None:
            with contextlib.suppress(Exception):
                await self._psub.unsubscribe()
        if self._bufferer_task is not None:
            if not self._bufferer_task.done():
                if graceful:
                    # bounded put so we never block forever if the bufferer dies between the check and the put
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._queue.put(None), timeout=5)
                else:
                    self._cancel.set()  # hard abort → bufferer naks the residual batch
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._bufferer_task, timeout=10)  # on timeout wait_for cancels the task
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._heartbeat_task
        if self._nc is not None:
            with contextlib.suppress(Exception):
                await self._nc.close()
