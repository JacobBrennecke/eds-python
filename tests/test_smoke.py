"""M0 smoke tests — the package imports and the entry point runs."""

from __future__ import annotations


def test_package_imports() -> None:
    import eds  # noqa: F401


def test_main_help_runs() -> None:
    from eds.__main__ import COMMANDS, main

    assert main([]) == 0
    # PARITY: the Go CLI exposes exactly these subcommands.
    assert COMMANDS == ("version", "publickey", "enroll", "server", "fork", "import", "download")
