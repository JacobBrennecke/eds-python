"""Regression: the CLI entry point must register drivers for EVERY command.

The `server` parent process validates `--url` via `get_driver_metadata_for_url` during session start. Before the
fix, `register_all()` ran only for `import` and the forked consumer — NOT the `server` parent — so `server` saw an
EMPTY driver registry and rejected every URL with "invalid driver URL". The fix registers at the main entry
(`eds/__main__.py`), matching C#'s `Program.cs:24` / Go's package `init()`.
"""

from __future__ import annotations

from eds.driver import get_driver_metadata_for_url, reset_registries

_URL = "postgres://u:p@127.0.0.1:5432/db"


def test_entry_point_registers_drivers_for_all_commands(capsys) -> None:
    from eds.__main__ import main

    reset_registries()  # simulate a fresh process with no drivers registered
    assert get_driver_metadata_for_url(_URL) is None  # precondition: empty registry

    # ANY CLI invocation (even `version`) must register drivers at the entry, so `server` can resolve --url.
    main(["version"])
    capsys.readouterr()  # swallow the version output

    assert get_driver_metadata_for_url(_URL) is not None  # drivers registered → `server` would accept the URL
