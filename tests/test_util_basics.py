"""PARITY: internal/util/json_test.go + util_test.go — JSONDiff, SliceContains-via-membership,
IsLocalhost, ListDir, GetFreePort."""

from __future__ import annotations

import os

from eds.util.file import get_free_port, is_localhost, list_dir
from eds.util.json import json_diff


def test_json_diff() -> None:
    # PARITY: json_test.go TestJSONDiff vectors.
    assert json_diff({"a": 1, "b": 2, "c": 3}, ["a", "b", "d"]) == ["c"]
    assert json_diff({"a": 1, "b": 2, "c": 3}, ["a", "b", "c"]) == []


def test_is_localhost() -> None:
    # PARITY: util_test.go TestIsLocalhost.
    assert is_localhost("localhost")
    assert is_localhost("0.0.0.0")
    assert is_localhost("127.0.0.1")
    assert not is_localhost("google.com")


def test_list_dir_recurses_and_skips_ds_store(tmp_path) -> None:
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / ".DS_Store").write_text("x")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.txt").write_text("b")
    got = [os.path.relpath(p, tmp_path).replace(os.sep, "/") for p in list_dir(str(tmp_path))]
    assert got == ["a.txt", "sub/b.txt"]  # sorted, .DS_Store skipped, recursive


def test_get_free_port() -> None:
    port = get_free_port()
    assert isinstance(port, int)
    assert 0 < port < 65536
