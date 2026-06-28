"""PARITY: main.go version resolution — baked ldflags version wins; $GIT_SHA only when baked is absent/"dev"."""

from __future__ import annotations

import sys
import types

from eds.cmd import root


def test_baked_version_wins_over_git_sha(monkeypatch) -> None:
    fake = types.ModuleType("eds._version")
    fake.VERSION = "1.2.3"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "eds._version", fake)
    monkeypatch.setenv("GIT_SHA", "deadbeef")
    assert root._resolve_version() == "1.2.3"  # PARITY: a baked non-dev version wins over GIT_SHA


def test_git_sha_used_when_no_baked_version(monkeypatch) -> None:
    monkeypatch.delitem(sys.modules, "eds._version", raising=False)
    monkeypatch.setenv("GIT_SHA", "deadbeef")
    assert root._resolve_version() == "deadbeef"  # no eds._version → fall back to GIT_SHA


def test_dev_when_nothing_set(monkeypatch) -> None:
    monkeypatch.delitem(sys.modules, "eds._version", raising=False)
    monkeypatch.delenv("GIT_SHA", raising=False)
    assert root._resolve_version() == "dev"
