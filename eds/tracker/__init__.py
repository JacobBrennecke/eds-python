"""PARITY: internal/tracker/tracker.go — the on-disk key/value state store.

Go uses BuntDB; this uses stdlib ``sqlite3``. The default ``TEXT PRIMARY KEY`` collation is BINARY
(memcmp), which reproduces BuntDB's ordinal key ordering — important for the prefix scans. TTL is stored
as a wall-clock ``expires_at`` (so it survives process restarts, like BuntDB) and evicted lazily on get
plus on prefix-delete.

DEVIATIONS (see DEVIATIONS.md):
- #tracker-deletekey-noop — deleting a missing key is a no-op (Go's BuntDB Delete returns ErrNotFound).
- #tracker-prefix-literal — DeleteKeysWithPrefix uses a literal ordinal range, not a glob (BuntDB's
  ``prefix+"*"`` is a glob; identical for the glob-free keys EDS uses).
- #tracker-durability — sqlite ``synchronous=NORMAL`` vs BuntDB's every-second fsync (the tracker holds
  rebuildable local state).
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any


def tracker_filename_from_dir(directory: str) -> str:
    """PARITY: TrackerFilenameFromDir."""
    return os.path.join(directory, "eds-data.db")


def _prefix_upper_bound(prefix: str) -> str | None:
    """Smallest string greater than every string starting with ``prefix`` (ordinal). None means no upper
    bound (prefix is empty, or ends at the max code point)."""
    if prefix == "":
        return None
    last = ord(prefix[-1])
    if last >= 0x10FFFF:
        return None
    return prefix[:-1] + chr(last + 1)


class Tracker:
    """PARITY: tracker.go Tracker (over sqlite3). Operations are serialized by a lock; the connection is
    opened with check_same_thread=False so it can be shared across the asyncio/worker threads."""

    def __init__(self, directory: str, logger: Any = None) -> None:
        self._logger = logger.with_prefix("[tracker]") if logger is not None else None
        self._lock = threading.Lock()
        self._closed = False
        self._db = sqlite3.connect(tracker_filename_from_dir(directory), check_same_thread=False)
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at REAL)"
        )
        self._db.commit()

    def get_key(self, key: str) -> tuple[bool, str]:
        """PARITY: GetKey — (found, value). Lazily evicts an expired key."""
        with self._lock:
            row = self._db.execute("SELECT value, expires_at FROM kv WHERE key=?", (key,)).fetchone()
            if row is None:
                return False, ""
            value, expires_at = row
            if expires_at is not None and expires_at < time.time():
                self._db.execute("DELETE FROM kv WHERE key=?", (key,))
                self._db.commit()
                return False, ""
            return True, value

    def set_key(self, key: str, value: str, expires: float = 0.0) -> None:
        """PARITY: SetKey — set with a TTL (seconds); expires<=0 means no expiry."""
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO kv (key, value, expires_at) VALUES (?, ?, ?)",
                (key, value, _deadline(expires)),
            )
            self._db.commit()

    def set_keys(self, keys: list[str], value: str, expires: float = 0.0) -> None:
        """PARITY: SetKeys — set multiple keys to the same value/TTL in one transaction."""
        deadline = _deadline(expires)
        with self._lock:
            self._db.executemany(
                "INSERT OR REPLACE INTO kv (key, value, expires_at) VALUES (?, ?, ?)",
                [(k, value, deadline) for k in keys],
            )
            self._db.commit()

    def delete_key(self, *keys: str) -> None:
        """PARITY: DeleteKey — delete keys (no-op on a missing key; see #tracker-deletekey-noop)."""
        with self._lock:
            self._db.executemany("DELETE FROM kv WHERE key=?", [(k,) for k in keys])
            self._db.commit()

    def delete_keys_with_prefix(self, prefix: str) -> int:
        """PARITY: DeleteKeysWithPrefix — delete all keys with the prefix (ordinal range); return the count."""
        upper = _prefix_upper_bound(prefix)
        with self._lock:
            if upper is None:
                cur = self._db.execute("DELETE FROM kv WHERE key >= ?", (prefix,))
            else:
                cur = self._db.execute("DELETE FROM kv WHERE key >= ? AND key < ?", (prefix, upper))
            self._db.commit()
            return cur.rowcount

    def close(self) -> None:
        """PARITY: Close — close the database (idempotent)."""
        if self._logger is not None:
            self._logger.debug("closing")
        with self._lock:
            if not self._closed:
                self._closed = True
                self._db.close()
        if self._logger is not None:
            self._logger.debug("closed")


def _deadline(expires: float) -> float | None:
    return time.time() + expires if expires > 0 else None


def new_tracker(directory: str, logger: Any = None) -> Tracker:
    """PARITY: NewTracker."""
    return Tracker(directory, logger)
