"""PARITY: internal/tracker/tracker_test.go (+ delete/prefix/set_keys coverage)."""

from __future__ import annotations

import time

from eds.tracker import new_tracker, tracker_filename_from_dir


def test_get_set_expire(tmp_path) -> None:
    t = new_tracker(str(tmp_path))
    try:
        assert t.get_key("foo") == (False, "")
        t.set_key("foo", "bar", 0.001)  # 1ms TTL
        time.sleep(0.05)
        assert t.get_key("foo") == (False, "")  # expired + lazily evicted
        t.set_key("foo", "bar", 0)  # no expiry
        assert t.get_key("foo") == (True, "bar")
    finally:
        t.close()


def test_set_keys_and_delete(tmp_path) -> None:
    t = new_tracker(str(tmp_path))
    try:
        t.set_keys(["a", "b", "c"], "v", 0)
        assert t.get_key("a") == (True, "v")
        assert t.get_key("c") == (True, "v")
        t.delete_key("a", "b")
        assert t.get_key("a") == (False, "")
        assert t.get_key("c") == (True, "v")
        t.delete_key("missing")  # no-op, no error
    finally:
        t.close()


def test_delete_keys_with_prefix(tmp_path) -> None:
    t = new_tracker(str(tmp_path))
    try:
        t.set_keys(["user:1", "user:2", "user:3"], "v", 0)
        t.set_key("order:1", "v", 0)
        assert t.delete_keys_with_prefix("user:") == 3
        assert t.get_key("user:1") == (False, "")
        assert t.get_key("order:1") == (True, "v")  # different prefix untouched
    finally:
        t.close()


def test_filename_from_dir(tmp_path) -> None:
    assert tracker_filename_from_dir(str(tmp_path)).endswith("eds-data.db")
