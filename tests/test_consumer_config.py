"""PARITY: consumer config defaulting + the pure setup helpers."""

from __future__ import annotations

from datetime import datetime, timezone

from eds.consumer.config import (
    ConsumerConfig,
    batch_max,
    durable_name,
    earliest_timestamp,
    filter_subjects,
)


class _Driver:
    def __init__(self, mb: int) -> None:
        self._mb = mb

    def max_batch_size(self) -> int:
        return self._mb


def test_defaults() -> None:
    c = ConsumerConfig()
    assert c.effective_max_ack_pending() == 25_000
    assert c.effective_max_pending_buffer() == 4096
    assert c.effective_heartbeat_interval() == 60.0
    assert c.effective_min_pending_latency() == 2.0
    assert c.effective_max_pending_latency() == 30.0
    assert c.effective_empty_buffer_pause() == 0.010


def test_max_ack_pending_defaults_when_non_positive() -> None:
    assert ConsumerConfig(max_ack_pending=-5).effective_max_ack_pending() == 25_000
    assert ConsumerConfig(max_ack_pending=100).effective_max_ack_pending() == 100


def test_latency_defaults_only_when_zero() -> None:
    # negative is left as-is (Go defaults durations only when == 0)
    assert ConsumerConfig(min_pending_latency=-1.0).effective_min_pending_latency() == -1.0
    assert ConsumerConfig(max_pending_latency=5.0).effective_max_pending_latency() == 5.0


def test_batch_max_clamps_to_positive_driver_batch() -> None:
    assert batch_max(ConsumerConfig(max_ack_pending=1000, driver=_Driver(500))) == 500  # driver clamps
    assert batch_max(ConsumerConfig(max_ack_pending=1000, driver=_Driver(5000))) == 1000  # driver larger -> no clamp
    assert batch_max(ConsumerConfig(max_ack_pending=1000, driver=_Driver(-1))) == 1000  # -1 = no limit
    assert batch_max(ConsumerConfig(max_ack_pending=0, driver=_Driver(500))) == 500  # default 25000 then clamp


def test_durable_name() -> None:
    assert durable_name("srv1", "") == "eds-srv1"
    assert durable_name("srv1", "x") == "eds-srv1-x"


def test_filter_subjects() -> None:
    assert filter_subjects(["c1", "c2"]) == [
        "dbchange.*.*.c1.*.PUBLIC.>",
        "dbchange.*.*.c2.*.PUBLIC.>",
    ]


def test_earliest_timestamp() -> None:
    assert earliest_timestamp(None) is None
    assert earliest_timestamp({}) is None
    assert earliest_timestamp({"a": None, "b": None}) is None
    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
    assert earliest_timestamp({"a": t2, "b": t1, "c": None}) == t1
