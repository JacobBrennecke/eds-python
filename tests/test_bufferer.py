"""PARITY: the Bufferer drain pump — graceful-drain (flush+ack) vs hard-cancel (nak) vs fatal."""

from __future__ import annotations

import asyncio

from eds.consumer.batch_processor import BatchProcessor, MsgMetadata
from eds.consumer.bufferer import Bufferer

_EVT = b'{"operation":"INSERT","table":"user","key":["u1"],"after":{"id":"u1"},"modelVersion":"v1"}'


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


class _FakeMsg:
    def __init__(self, seq: int) -> None:
        self._meta = MsgMetadata(consumer_seq=seq)
        self.acked = False
        self.naked = False

    @property
    def data(self) -> bytes:
        return _EVT

    def metadata(self) -> MsgMetadata:
        return self._meta

    async def ack(self) -> None:
        self.acked = True

    async def nak(self) -> None:
        self.naked = True


class _FakeDriver:
    def __init__(self) -> None:
        self.flushed = 0

    def max_batch_size(self) -> int:
        return 10

    def process(self, logger, evt) -> bool:
        return False

    def flush(self, logger) -> None:
        self.flushed += 1


class _FakeMetrics:
    def pending_events_inc(self): ...
    def pending_events_dec(self): ...
    def total_events_inc(self): ...
    def observe_flush_duration(self, s): ...
    def observe_flush_count(self, c): ...
    def observe_processing_duration(self, s): ...


def _processor(driver) -> BatchProcessor:
    return BatchProcessor(
        driver, None, None, _FakeMetrics(), _QuietLogger(),
        max_batch=10, min_pending_latency=2.0, max_pending_latency=30.0,
    )


async def test_graceful_drain_flushes_and_acks() -> None:
    q: asyncio.Queue = asyncio.Queue()
    d = _FakeDriver()
    m1, m2 = _FakeMsg(1), _FakeMsg(2)
    await q.put(m1)
    await q.put(m2)
    await q.put(None)  # graceful-drain sentinel
    await Bufferer(q, _processor(d), empty_buffer_pause=0.01, logger=_QuietLogger()).run()
    assert d.flushed == 1
    assert m1.acked and m2.acked


async def test_fatal_invokes_on_fatal_and_naks() -> None:
    q: asyncio.Queue = asyncio.Queue()
    m = _FakeMsg(2)  # out of order (expected 1) -> fatal
    await q.put(m)
    fatals: list = []
    await Bufferer(
        q, _processor(_FakeDriver()), empty_buffer_pause=0.01, logger=_QuietLogger(),
        on_fatal=fatals.append,
    ).run()
    assert len(fatals) == 1
    assert m.naked and not m.acked


async def test_preserves_order_when_items_arrive_during_wait() -> None:
    # Regression: items that arrive while the bufferer is awaiting the queue must still be processed in
    # strict order (an earlier bug re-enqueued the awaited item behind later arrivals → out-of-order seq).
    q: asyncio.Queue = asyncio.Queue()
    d = _FakeDriver()
    fatals: list = []
    buf = Bufferer(q, _processor(d), empty_buffer_pause=0.05, logger=_QuietLogger(), on_fatal=fatals.append)
    task = asyncio.create_task(buf.run())
    await asyncio.sleep(0.12)  # let it reach the empty-queue wait
    m1, m2 = _FakeMsg(1), _FakeMsg(2)
    await q.put(m1)
    await q.put(m2)
    await q.put(None)  # graceful-drain sentinel
    await asyncio.wait_for(task, timeout=2)
    assert fatals == []  # no out-of-order fatal
    assert m1.acked and m2.acked
    assert d.flushed >= 1


async def test_hard_cancel_naks_residual() -> None:
    q: asyncio.Queue = asyncio.Queue()
    m = _FakeMsg(1)
    await q.put(m)
    cancel = asyncio.Event()
    cancel.set()  # hard cancel (ctx.Done)
    await Bufferer(q, _processor(_FakeDriver()), empty_buffer_pause=0.01, logger=_QuietLogger(), cancel=cancel).run()
    assert m.naked and not m.acked  # residual nak'd, not flushed
