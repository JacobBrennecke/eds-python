"""PARITY: internal/metrics.go — Prometheus instruments + the SystemStats snapshot.

Two load-bearing quirks (SPEC §8.7): histogram snapshot fields carry the SAMPLE COUNT (not sum), and
pendingEvents is ALWAYS 0 (Go reads it through the counter accessor, which yields 0 for a gauge). The
``receving`` typo in the processing-duration help is preserved.

DEVIATIONS (see DEVIATIONS.md): each EdsMetrics owns a CollectorRegistry (replaces Go's global
DefaultRegisterer + MetricsReset); in the SCRAPE TEXT prometheus-client appends ``_total`` to the counter
name (HELP + sample; Go scrapes ``eds_total_events``) and renders integer histogram bucket ``le`` labels as
``"10.0"`` vs Go's ``"10"`` — both cosmetic (a Prometheus scraper parses them identically; the snapshot
values read via collect() and the gojson serialization used by the heartbeat are byte-exact). Memory/load
is a documented subset (metrics-memory-load-partial); get_system_stats raises on a provider error (Go
returns (nil,err)/(ptr,err); the heartbeat caller discards the snapshot either way).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from eds.util.gojson import marshal

_FLUSH_DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
_FLUSH_COUNT_BUCKETS = [1, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
_PROCESSING_DURATION_BUCKETS = [1, 2, 3, 5, 10, 60, 300, 600, 1800, 3600]


@dataclass
class MemoryStat:
    """PARITY: gopsutil mem.VirtualMemoryStat (subset — see metrics-memory-load-partial)."""

    total: int = 0
    available: int = 0
    used: int = 0
    used_percent: float = 0.0
    free: int = 0

    def __gojson__(self) -> str:
        return (
            '{"total":' + marshal(self.total)
            + ',"available":' + marshal(self.available)
            + ',"used":' + marshal(self.used)
            + ',"usedPercent":' + marshal(self.used_percent)
            + ',"free":' + marshal(self.free)
            + "}"
        )


@dataclass
class LoadStat:
    """PARITY: gopsutil load.AvgStat."""

    load1: float = 0.0
    load5: float = 0.0
    load15: float = 0.0

    def __gojson__(self) -> str:
        return (
            '{"load1":' + marshal(self.load1)
            + ',"load5":' + marshal(self.load5)
            + ',"load15":' + marshal(self.load15)
            + "}"
        )


@dataclass
class MetricsSnapshot:
    """PARITY: the anonymous Metrics struct in SystemStats (declaration order = JSON order)."""

    flush_count: float = 0.0
    flush_duration: float = 0.0
    processing_duration: float = 0.0
    pending_events: float = 0.0
    total_events: float = 0.0

    def __gojson__(self) -> str:
        return (
            '{"flushCount":' + marshal(self.flush_count)
            + ',"flushDuration":' + marshal(self.flush_duration)
            + ',"processingDuration":' + marshal(self.processing_duration)
            + ',"pendingEvents":' + marshal(self.pending_events)
            + ',"totalEvents":' + marshal(self.total_events)
            + "}"
        )


@dataclass
class SystemStats:
    """PARITY: metrics.go SystemStats."""

    metrics: MetricsSnapshot = field(default_factory=MetricsSnapshot)
    memory: MemoryStat | None = None
    load: LoadStat | None = None

    def __gojson__(self) -> str:
        return (
            '{"metrics":' + marshal(self.metrics)
            + ',"memory":' + marshal(self.memory)
            + ',"load":' + marshal(self.load)
            + "}"
        )


class SystemResourceProvider(Protocol):
    """Seam over the OS memory/load reads (so tests can inject fixed/throwing providers)."""

    def get_memory(self) -> MemoryStat: ...
    def get_load(self) -> LoadStat: ...


class DefaultSystemResourceProvider:
    """Production provider over psutil (the gopsutil analog)."""

    def get_memory(self) -> MemoryStat:
        import psutil

        vm = psutil.virtual_memory()
        return MemoryStat(
            total=vm.total, available=vm.available, used=vm.used, used_percent=vm.percent, free=vm.free
        )

    def get_load(self) -> LoadStat:
        import psutil

        try:
            one, five, fifteen = psutil.getloadavg()  # emulated on Windows
        except (AttributeError, OSError):
            return LoadStat()  # PARITY: zeros where load average is unavailable
        return LoadStat(load1=one, load5=five, load15=fifteen)


def get_metric_value(collector: Any) -> float:
    """PARITY: metrics.go getMetricValue — histogram → SampleCount; otherwise the counter value
    (a gauge has no counter value, so it resolves to 0, exactly like Go)."""
    total = 0.0
    for metric in collector.collect():
        if metric.type == "histogram":
            for s in metric.samples:
                if s.name.endswith("_count"):
                    total += s.value
        else:
            for s in metric.samples:
                if s.name.endswith("_total"):  # counter value sample (gauge has none → 0)
                    total += s.value
    return total


class EdsMetrics:
    """PARITY: metrics.go counters + GetSystemStats, on an instance-scoped registry."""

    def __init__(
        self, registry: CollectorRegistry | None = None, resources: SystemResourceProvider | None = None
    ) -> None:
        self._registry = registry or CollectorRegistry()
        self._resources = resources or DefaultSystemResourceProvider()
        self._pending = Gauge("eds_pending_events", "The number of pending events", registry=self._registry)
        self._total = Counter(
            "eds_total_events", "The total number of events processed", registry=self._registry
        )
        self._flush_duration = Histogram(
            "eds_flush_duration_seconds", "The duration of driver flushes",
            buckets=_FLUSH_DURATION_BUCKETS, registry=self._registry,
        )
        self._flush_count = Histogram(
            "eds_flush_count", "The count of events flushed",
            buckets=_FLUSH_COUNT_BUCKETS, registry=self._registry,
        )
        self._processing_duration = Histogram(
            "eds_processing_duration_seconds",
            # PARITY: the "receving" typo is in the Go source — keep it.
            "The latency in duration of processing events from receving them to flushing them",
            buckets=_PROCESSING_DURATION_BUCKETS, registry=self._registry,
        )

    # ---- mutators (PARITY: the consumer's IConsumerMetrics call sites; durations in seconds) ----
    def pending_events_inc(self) -> None:
        self._pending.inc()

    def pending_events_dec(self) -> None:
        self._pending.dec()

    def total_events_inc(self) -> None:
        self._total.inc()

    def observe_processing_duration(self, seconds: float) -> None:
        self._processing_duration.observe(seconds)

    def observe_flush_duration(self, seconds: float) -> None:
        self._flush_duration.observe(seconds)

    def observe_flush_count(self, count: float) -> None:
        self._flush_count.observe(count)

    def get_system_stats(self) -> SystemStats:
        """PARITY: GetSystemStats — snapshot the instruments, then read memory + load."""
        metrics = MetricsSnapshot(
            flush_count=get_metric_value(self._flush_count),
            flush_duration=get_metric_value(self._flush_duration),
            processing_duration=get_metric_value(self._processing_duration),
            pending_events=get_metric_value(self._pending),  # PARITY: always 0 (gauge via counter accessor)
            total_events=get_metric_value(self._total),
        )
        # PARITY: Go returns (nil,err) on a memory error and (&s,err) on a load error; the heartbeat caller
        # discards the snapshot on either, so raising here is faithful.
        memory = self._resources.get_memory()
        load = self._resources.get_load()
        return SystemStats(metrics=metrics, memory=memory, load=load)

    def scrape(self) -> str:
        """Prometheus exposition text for this instance's registry."""
        return generate_latest(self._registry).decode()
