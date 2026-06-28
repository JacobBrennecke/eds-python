"""PARITY: internal/consumer/consumer.go — consumer config + the pure setup helpers.

Defaulting is asymmetric (matching Go): max_ack_pending defaults when <= 0; the latencies default only when
== 0 (a negative value is left as-is). The batch threshold clamps to the driver's MaxBatchSize (when positive),
but the NATS buffer capacity uses the RAW (unclamped) max_ack_pending.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from eds.schema import SchemaRegistry, SchemaValidator

_DEFAULT_MAX_ACK_PENDING = 25_000
_DEFAULT_MAX_PENDING_BUFFER = 4096
_DEFAULT_HEARTBEAT_INTERVAL = 60.0
_DEFAULT_MIN_PENDING_LATENCY = 2.0
_DEFAULT_MAX_PENDING_LATENCY = 30.0
_DEFAULT_EMPTY_BUFFER_PAUSE = 0.010


@dataclass
class ConsumerConfig:
    """PARITY: ConsumerConfig."""

    url: str = ""
    credentials: str = ""
    company_ids: list[str] = field(default_factory=list)
    suffix: str = ""
    max_ack_pending: int = 0
    max_pending_buffer: int = 0
    driver: Any = None
    registry: SchemaRegistry | None = None
    schema_validator: SchemaValidator | None = None
    export_table_timestamps: dict[str, datetime | None] | None = None
    deliver_all: bool = False
    heartbeat_interval: float = 0.0
    min_pending_latency: float = 0.0
    max_pending_latency: float = 0.0
    empty_buffer_pause_time: float = 0.0
    session_id_callback: Any = None

    # ---- effective values (Go's defaulting) ----
    def effective_max_ack_pending(self) -> int:
        return self.max_ack_pending if self.max_ack_pending > 0 else _DEFAULT_MAX_ACK_PENDING

    def effective_max_pending_buffer(self) -> int:
        return self.max_pending_buffer if self.max_pending_buffer > 0 else _DEFAULT_MAX_PENDING_BUFFER

    def effective_heartbeat_interval(self) -> float:
        return self.heartbeat_interval if self.heartbeat_interval != 0 else _DEFAULT_HEARTBEAT_INTERVAL

    def effective_min_pending_latency(self) -> float:
        return self.min_pending_latency if self.min_pending_latency != 0 else _DEFAULT_MIN_PENDING_LATENCY

    def effective_max_pending_latency(self) -> float:
        return self.max_pending_latency if self.max_pending_latency != 0 else _DEFAULT_MAX_PENDING_LATENCY

    def effective_empty_buffer_pause(self) -> float:
        return self.empty_buffer_pause_time if self.empty_buffer_pause_time != 0 else _DEFAULT_EMPTY_BUFFER_PAUSE


def batch_max(config: ConsumerConfig) -> int:
    """PARITY: consumer.max — min(effective_max_ack_pending, driver.MaxBatchSize() when > 0)."""
    m = config.effective_max_ack_pending()
    driver = config.driver
    if driver is not None:
        driver_max = driver.max_batch_size()
        if 0 < driver_max < m:
            m = driver_max
    return m


def validate_company_ids(override: list[str], allowed: list[str]) -> list[str]:
    """PARITY: consumer.go:730-741 — strict company-id override validation (every override must be present in the
    credentials; no "*" special-case). Raises ValueError otherwise."""
    validated: list[str] = []
    for cid in override:
        if cid not in allowed:
            raise ValueError(f"provided company ID {cid} not in credentials")
        validated.append(cid)
    if not validated:
        raise ValueError("no valid company IDs provided")
    return validated


def durable_name(server_id: str, suffix: str) -> str:
    """PARITY: ConsumerSetup.DurableName."""
    return f"eds-{server_id}-{suffix}" if suffix else f"eds-{server_id}"


def filter_subjects(company_ids: list[str]) -> list[str]:
    """PARITY: ConsumerSetup.FilterSubjects."""
    return [f"dbchange.*.*.{cid}.*.PUBLIC.>" for cid in company_ids]


def earliest_timestamp(export_table_timestamps: dict[str, datetime | None] | None) -> datetime | None:
    """PARITY: ConsumerSetup.EarliestTimestamp — the minimum non-None cutoff, or None."""
    if not export_table_timestamps:
        return None
    stamps = [ts for ts in export_table_timestamps.values() if ts is not None]
    return min(stamps) if stamps else None
