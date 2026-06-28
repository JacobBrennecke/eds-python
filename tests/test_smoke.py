"""M0 smoke tests — the package imports and the entry point runs."""

from __future__ import annotations


def test_package_imports() -> None:
    import eds  # noqa: F401


def test_main_help_runs() -> None:
    from eds.__main__ import main

    assert main([]) == 0  # bare invocation prints help and exits 0
