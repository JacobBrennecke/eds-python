"""PARITY: SIGINT/SIGTERM shutdown handling (≈ Go signal.Notify + context cancel; C# ShutdownSignal).

Sets a ``threading.Event`` when an interrupt/terminate signal arrives. The asyncio bridge (awaiting the
event, scheduling on the loop via call_soon_threadsafe) is wired with the consumer/control-plane at M5/M9;
on Windows asyncio cannot ``add_signal_handler``, so the cross-platform ``signal.signal`` + Event approach
is the base. Signal registration must happen on the main thread (guarded).
"""

from __future__ import annotations

import signal
import threading
from collections.abc import Iterable
from types import FrameType
from typing import Any


def _default_signals() -> list[int]:
    sigs: list[int] = [signal.SIGINT]
    if hasattr(signal, "SIGTERM"):
        sigs.append(signal.SIGTERM)
    sigbreak = getattr(signal, "SIGBREAK", None)  # Windows Ctrl-Break
    if sigbreak is not None:
        sigs.append(sigbreak)
    return sigs


class ShutdownSignal:
    def __init__(self, signals: Iterable[int] | None = None) -> None:
        self._event = threading.Event()
        self._previous: dict[int, Any] = {}
        for sig in (_default_signals() if signals is None else list(signals)):
            try:
                self._previous[sig] = signal.signal(sig, self._handler)
            except (ValueError, OSError, RuntimeError):
                # Not the main thread, or the signal is unsupported on this OS — skip it.
                pass

    def _handler(self, signum: int, frame: FrameType | None) -> None:
        self._event.set()

    def trigger(self) -> None:
        """Programmatic shutdown (also used by tests)."""
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)

    @property
    def event(self) -> threading.Event:
        return self._event

    def close(self) -> None:
        """Restore the previous signal handlers."""
        for sig, prev in self._previous.items():
            try:
                signal.signal(sig, prev)
            except (ValueError, OSError, RuntimeError):
                pass
        self._previous = {}

    def __enter__(self) -> ShutdownSignal:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
