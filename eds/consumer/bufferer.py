"""PARITY: the consumer's drain pump (internal/consumer/consumer.go bufferer).

Single async task draining the buffer queue into the BatchProcessor, mirroring Go's select(msg / default-idle /
cancel) loop. Two stop modes (matching Go): a graceful-drain SENTINEL (None enqueued after the producer stops)
→ final flush + ack of the residual batch; a hard CANCEL event (ctx.Done) → nak the residual batch. A clean
stop (driver stopped / ack failure) or a surfaced fatal (out-of-order/decode/process/flush) ends the loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from eds.consumer.batch_processor import BatchProcessor, ConsumerFatalError, ConsumerStoppedError
from eds.util.logger import Logger


class Bufferer:
    """PARITY: bufferer() — the single drain task."""

    def __init__(
        self,
        queue: asyncio.Queue,
        processor: BatchProcessor,
        *,
        empty_buffer_pause: float,
        logger: Logger,
        cancel: asyncio.Event | None = None,
        on_fatal: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._queue = queue
        self._processor = processor
        self._empty_buffer_pause = empty_buffer_pause
        self._logger = logger
        self._cancel = cancel or asyncio.Event()
        self._on_fatal = on_fatal

    async def run(self) -> None:
        while True:
            # PARITY: Arm B — drain everything currently buffered (Go loops on `case msg := <-c.buffer`).
            while True:
                try:
                    item = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if await self._handle(item):
                    return

            # PARITY: Arm A — hard cancel (ctx.Done) → nak the residual batch. Checked BEFORE the idle flush so a
            # hard abort naks promptly rather than flushing+acking a min-latency-aged partial batch first.
            if self._cancel.is_set():
                await self._processor.nack_everything()
                return

            # PARITY: Arm C (default) — idle min-latency flush.
            try:
                await self._processor.on_idle()
            except ConsumerStoppedError:
                return
            except ConsumerFatalError as ex:
                if self._on_fatal is not None:
                    self._on_fatal(ex)
                return

            # Wait for the next item or the idle window (DEVIATION: asyncio wait_for replaces Go's select+default
            # busy-spin so the event loop is not blocked while a partial batch waits for min/max latency).
            # The awaited item is handled DIRECTLY (not re-enqueued) — re-enqueueing would move it behind any
            # items that arrived during the wait and break the strict consumer-sequence ordering.
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=self._empty_buffer_pause)
            except asyncio.TimeoutError:
                continue  # idle window elapsed — loop and tick idle again
            if await self._handle(item):
                return

    async def _handle(self, item: Any) -> bool:
        """Process one queue item; returns True when the loop should stop."""
        if item is None:  # graceful-drain sentinel: final flush of the residual batch
            await self._final_flush()
            return True
        try:
            await self._processor.process_message(item)
        except ConsumerStoppedError:
            return True  # clean stop (driver stopped / ack failure); pending already nak'd
        except ConsumerFatalError as ex:
            if self._on_fatal is not None:
                self._on_fatal(ex)
            return True
        return False

    async def _final_flush(self) -> None:
        try:
            await self._processor.flush(self._logger)  # PARITY: graceful Stop flushes+acks the residual batch
        except (ConsumerStoppedError, ConsumerFatalError):
            pass  # best-effort on graceful shutdown
