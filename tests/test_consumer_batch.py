"""PARITY: BatchProcessor — the consumer's decision matrix (flush triggers, skip, migration, ack/nak)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from eds.consumer.batch_processor import (
    BatchProcessor,
    ConsumerFatalError,
    ConsumerStoppedError,
    MsgMetadata,
)
from eds.schema import Schema, SchemaProperty

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
    def __init__(self, seq: int, data: bytes = _EVT, num_pending: int = 0) -> None:
        self._data = data
        self._meta = MsgMetadata(consumer_seq=seq, num_pending=num_pending)
        self.acked = False
        self.naked = False

    @property
    def data(self) -> bytes:
        return self._data

    def metadata(self) -> MsgMetadata:
        return self._meta

    async def ack(self) -> None:
        self.acked = True

    async def nak(self) -> None:
        self.naked = True


class _FakeDriver:
    def __init__(self, max_batch=10, process_returns=False, process_raises=None, flush_raises=None) -> None:
        self._mb = max_batch
        self.process_returns = process_returns
        self.process_raises = process_raises
        self.flush_raises = flush_raises
        self.processed: list = []
        self.flushed = 0
        self.migrated_tables: list = []
        self.migrated_columns: list = []

    def max_batch_size(self) -> int:
        return self._mb

    def process(self, logger, evt) -> bool:
        if self.process_raises is not None:
            raise self.process_raises
        self.processed.append(evt)
        return self.process_returns

    def flush(self, logger) -> None:
        if self.flush_raises is not None:
            raise self.flush_raises
        self.flushed += 1

    def migrate_new_table(self, ctx, logger, schema) -> None:
        self.migrated_tables.append(schema.table)

    def migrate_new_columns(self, ctx, logger, schema, cols) -> None:
        self.migrated_columns.append((schema.table, list(cols)))


class _FakeMetrics:
    def __init__(self) -> None:
        self.inc = self.dec = self.total = 0
        self.flush_durations: list = []
        self.flush_counts: list = []
        self.proc_durations: list = []

    def pending_events_inc(self): self.inc += 1
    def pending_events_dec(self): self.dec += 1
    def total_events_inc(self): self.total += 1
    def observe_flush_duration(self, s): self.flush_durations.append(s)
    def observe_flush_count(self, c): self.flush_counts.append(c)
    def observe_processing_duration(self, s): self.proc_durations.append(s)


class _Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _bp(driver, metrics, *, max_batch=10, registry=None, validator=None, table_timestamps=None,
        supports_migration=False, clock=None, min_lat=2.0, max_lat=30.0) -> BatchProcessor:
    return BatchProcessor(
        driver, registry, validator, metrics, _QuietLogger(),
        max_batch=max_batch, min_pending_latency=min_lat, max_pending_latency=max_lat,
        table_timestamps=table_timestamps, supports_migration=supports_migration,
        now=(clock or _Clock()),
    )


async def test_flush_on_max_batch() -> None:
    d, mx = _FakeDriver(), _FakeMetrics()
    bp = _bp(d, mx, max_batch=2)
    m1, m2 = _FakeMsg(1), _FakeMsg(2)
    await bp.process_message(m1)
    assert d.flushed == 0 and not m1.acked  # buffered
    await bp.process_message(m2)
    assert d.flushed == 1  # len(pending) >= max -> flush
    assert m1.acked and m2.acked
    assert bp.pending_count == 0
    assert mx.dec == 2 and mx.flush_counts == [2.0] and len(mx.flush_durations) == 1


async def test_flush_when_process_returns_true() -> None:
    d, mx = _FakeDriver(process_returns=True), _FakeMetrics()
    bp = _bp(d, mx)
    m = _FakeMsg(1)
    await bp.process_message(m)
    assert d.flushed == 1 and m.acked


async def test_out_of_order_naks_and_raises() -> None:
    d, mx = _FakeDriver(), _FakeMetrics()
    bp = _bp(d, mx)
    m = _FakeMsg(2)  # expected 1
    with pytest.raises(ConsumerFatalError, match="out of order"):
        await bp.process_message(m)
    assert m.naked and not m.acked
    assert d.flushed == 0 and mx.dec == 1


async def test_decode_error_naks_and_raises() -> None:
    d, mx = _FakeDriver(), _FakeMetrics()
    bp = _bp(d, mx)
    m = _FakeMsg(1, data=b"{not json")
    with pytest.raises(ConsumerFatalError):
        await bp.process_message(m)
    assert m.naked and mx.dec == 1


async def test_process_error_naks_and_raises() -> None:
    d, mx = _FakeDriver(process_raises=RuntimeError("boom")), _FakeMetrics()
    bp = _bp(d, mx)
    m = _FakeMsg(1)
    with pytest.raises(ConsumerFatalError):
        await bp.process_message(m)
    assert m.naked and mx.dec == 1


async def test_flush_error_naks_and_raises() -> None:
    d, mx = _FakeDriver(max_batch=1, flush_raises=RuntimeError("db down")), _FakeMetrics()
    bp = _bp(d, mx, max_batch=1)
    m = _FakeMsg(1)  # len>=max=1 -> flush -> driver.flush raises
    with pytest.raises(ConsumerFatalError):
        await bp.process_message(m)
    assert m.naked and not m.acked


async def test_driver_stopped_is_clean_stop() -> None:
    from eds.driver import DriverStoppedError

    d, mx = _FakeDriver(max_batch=1, flush_raises=DriverStoppedError()), _FakeMetrics()
    bp = _bp(d, mx, max_batch=1)
    m = _FakeMsg(1)
    with pytest.raises(ConsumerStoppedError):
        await bp.process_message(m)
    assert m.naked  # nak'd, but a clean stop (not fatal)


async def test_skip_export_cutoff_acks_individually() -> None:
    d, mx = _FakeDriver(), _FakeMetrics()
    # event timestamp is 0 (epoch) by default -> before any 2026 cutoff -> skip
    cutoff = {"user": datetime(2026, 1, 1, tzinfo=timezone.utc)}
    bp = _bp(d, mx, table_timestamps=cutoff)
    m = _FakeMsg(1)
    await bp.process_message(m)
    assert m.acked  # individually acked
    assert d.processed == []  # never reached the driver
    assert bp.pending_count == 0 and mx.dec == 1
    assert bp.sequence == 1  # sequence still advanced


async def test_skip_validator_not_found() -> None:
    class _Validator:
        def validate(self, evt):
            return (False, False, "")  # not found -> skip

    d, mx = _FakeDriver(), _FakeMetrics()
    bp = _bp(d, mx, validator=_Validator())
    m = _FakeMsg(1)
    await bp.process_message(m)
    assert m.acked and d.processed == []


async def test_migration_forces_flush() -> None:
    class _Reg:
        def __init__(self) -> None:
            self.set_versions: list = []
            self._schema = Schema(table="user", model_version="v1", primary_keys=["id"],
                                  properties={"id": SchemaProperty(type="string")})

        def get_table_version(self, table):
            return (False, "")  # new table

        def get_schema(self, table, version):
            return self._schema

        def set_table_version(self, table, version):
            self.set_versions.append((table, version))

    d, mx, reg = _FakeDriver(), _FakeMetrics(), _Reg()
    bp = _bp(d, mx, registry=reg, supports_migration=True)
    m = _FakeMsg(1)
    await bp.process_message(m)
    assert d.migrated_tables == ["user"]
    assert reg.set_versions == [("user", "v1")]
    assert d.flushed == 1 and m.acked  # migration forced a flush


async def test_time_based_flush_trigger() -> None:
    clk = _Clock(1000.0)
    d, mx = _FakeDriver(), _FakeMetrics()
    bp = _bp(d, mx, max_batch=10, max_lat=30.0, clock=clk)
    await bp.process_message(_FakeMsg(1))  # buffered, pending_started=1000
    assert d.flushed == 0
    clk.t = 1031.0  # > 30s later
    await bp.process_message(_FakeMsg(2))  # trigger #2: latency timeout -> flush
    assert d.flushed == 1


async def test_idle_flush_after_min_latency() -> None:
    clk = _Clock(1000.0)
    d, mx = _FakeDriver(), _FakeMetrics()
    bp = _bp(d, mx, max_batch=10, min_lat=2.0, clock=clk)
    await bp.process_message(_FakeMsg(1))  # buffered, pending_started=1000
    await bp.on_idle()
    assert d.flushed == 0  # not yet min latency
    clk.t = 1002.5
    await bp.on_idle()
    assert d.flushed == 1  # partial batch flushed at min latency


async def test_ack_failure_is_clean_stop() -> None:
    class _BadAckMsg(_FakeMsg):
        async def ack(self) -> None:
            raise RuntimeError("ack failed")

    d, mx = _FakeDriver(process_returns=True), _FakeMetrics()
    bp = _bp(d, mx)
    m = _BadAckMsg(1)
    with pytest.raises(ConsumerStoppedError):
        await bp.process_message(m)
    assert m.naked  # ack failed -> nak everything, clean stop
