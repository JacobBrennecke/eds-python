"""PARITY: shutdown signal abstraction."""

from __future__ import annotations

from eds.util.shutdown import ShutdownSignal


def test_trigger_sets_event() -> None:
    s = ShutdownSignal(signals=[])  # no real signal registration in tests
    assert not s.is_set()
    s.trigger()
    assert s.is_set()
    assert s.wait(0) is True


def test_handler_sets_event() -> None:
    s = ShutdownSignal(signals=[])
    s._handler(2, None)  # simulate SIGINT delivery
    assert s.is_set()


def test_registers_and_restores_without_error() -> None:
    # Default signals register on the main thread; close() restores the previous handlers.
    with ShutdownSignal() as s:
        assert not s.is_set()
    # no exception means register + restore worked
