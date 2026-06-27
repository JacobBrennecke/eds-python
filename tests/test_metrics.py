"""PARITY: internal/metrics.go — vectors from the C# MetricsTests + consumer heartbeat parity."""

from __future__ import annotations

import pytest

from eds.metrics import EdsMetrics, LoadStat, MemoryStat, MetricsSnapshot, SystemStats
from eds.util.gojson import stringify


class _Fixed:
    def get_memory(self) -> MemoryStat:
        return MemoryStat(total=100, available=50, used=40, used_percent=40.0, free=10)

    def get_load(self) -> LoadStat:
        return LoadStat(load1=0.5, load5=0.6, load15=0.7)


class _ThrowMemory:
    def get_memory(self) -> MemoryStat:
        raise RuntimeError("mem fail")

    def get_load(self) -> LoadStat:
        return LoadStat()


class _ThrowLoad:
    def get_memory(self) -> MemoryStat:
        return MemoryStat()

    def get_load(self) -> LoadStat:
        raise RuntimeError("load fail")


def _m() -> EdsMetrics:
    return EdsMetrics(resources=_Fixed())


def test_instrument_names_and_help() -> None:  # V1
    text = _m().scrape()
    assert "# HELP eds_pending_events The number of pending events" in text
    assert "# HELP eds_flush_duration_seconds The duration of driver flushes" in text
    assert "# HELP eds_flush_count The count of events flushed" in text
    # PARITY: the "receving" typo is preserved.
    assert (
        "# HELP eds_processing_duration_seconds The latency in duration of processing events "
        "from receving them to flushing them" in text
    )
    # DEVIATION: prometheus-client appends _total to the counter name (Go scrapes eds_total_events).
    assert "eds_total_events" in text
    assert "The total number of events processed" in text


def test_histogram_buckets() -> None:  # V2
    text = _m().scrape()
    # Boundaries match Go; integer le labels render as "X.0" (prometheus-client) vs Go's "X" — cosmetic.
    assert 'eds_flush_duration_seconds_bucket{le="0.005"}' in text
    assert 'eds_flush_duration_seconds_bucket{le="10.0"}' in text
    assert 'eds_flush_count_bucket{le="1.0"}' in text
    assert 'eds_flush_count_bucket{le="10000.0"}' in text
    assert 'eds_processing_duration_seconds_bucket{le="3600.0"}' in text


def test_pending_events_always_zero() -> None:  # V3
    m = _m()
    m.pending_events_inc()
    m.pending_events_inc()
    m.pending_events_inc()
    assert m.get_system_stats().metrics.pending_events == 0.0


def test_histogram_fields_are_sample_counts() -> None:  # V4
    m = _m()
    m.observe_flush_count(5)
    m.observe_flush_count(10)
    m.observe_flush_duration(0.1)
    m.observe_flush_duration(0.2)
    m.observe_flush_duration(0.3)
    m.observe_processing_duration(1)
    stats = m.get_system_stats().metrics
    assert stats.flush_count == 2.0  # count of observations, NOT 15
    assert stats.flush_duration == 3.0  # NOT 0.6
    assert stats.processing_duration == 1.0


def test_total_events_is_counter_value() -> None:  # V5
    m = _m()
    m.total_events_inc()
    m.total_events_inc()
    assert m.get_system_stats().metrics.total_events == 2.0


def test_memory_and_load_present_with_default_provider() -> None:  # V6
    stats = EdsMetrics().get_system_stats()
    assert stats.memory is not None
    assert stats.load is not None


def test_memory_error_propagates() -> None:  # V7
    with pytest.raises(RuntimeError, match="mem fail"):
        EdsMetrics(resources=_ThrowMemory()).get_system_stats()


def test_load_error_propagates() -> None:  # V8
    with pytest.raises(RuntimeError, match="load fail"):
        EdsMetrics(resources=_ThrowLoad()).get_system_stats()


def test_isolated_instances_do_not_collide() -> None:  # V9
    a = _m()
    b = _m()
    a.total_events_inc()
    assert a.get_system_stats().metrics.total_events == 1.0
    assert b.get_system_stats().metrics.total_events == 0.0


def test_system_stats_gojson() -> None:
    assert stringify(SystemStats(metrics=MetricsSnapshot(), memory=None, load=None)) == (
        '{"metrics":{"flushCount":0,"flushDuration":0,"processingDuration":0,"pendingEvents":0,'
        '"totalEvents":0},"memory":null,"load":null}'
    )
    full = SystemStats(
        metrics=MetricsSnapshot(flush_count=3.0, total_events=2.0),
        memory=MemoryStat(total=100, available=50, used=40, used_percent=40.0, free=10),
        load=LoadStat(load1=0.5, load5=0.6, load15=0.7),
    )
    assert stringify(full) == (
        '{"metrics":{"flushCount":3,"flushDuration":0,"processingDuration":0,"pendingEvents":0,'
        '"totalEvents":2},"memory":{"total":100,"available":50,"used":40,"usedPercent":40,"free":10},'
        '"load":{"load1":0.5,"load5":0.6,"load15":0.7}}'
    )
