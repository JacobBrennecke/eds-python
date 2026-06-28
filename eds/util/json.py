"""PARITY: internal/util/json.go — JSON helpers + the NDJSON(.gz) stream decoder.

The byte-critical Go ``json.Marshal`` reproduction (sorted keys, HTML + U+2028/9 escaping, Go float formatting)
lives in ``eds.util.gojson``. NDJSONDecoder is consumed by the importer run-loop.
"""

from __future__ import annotations

import gzip
import io
import json
import os
from typing import Any


def _reject_constant(token: str) -> object:
    # PARITY: Go's json.Decoder rejects NaN/Infinity/-Infinity (Python's json.loads accepts them by default).
    raise ValueError(f"invalid JSON literal: {token}")


class NDJSONDecoder:
    """PARITY: util.NewNDJSONDecoder / the JSONDecoder interface (Decode/More/Count/Close).

    DEVIATION (ndjson-rawdecode): Go uses an incremental json.Decoder over the (optionally gunzipped) stream;
    this reads line-by-line (the CRDB changefeed invariant is one JSON value per line) and returns the raw
    value bytes (stripped of surrounding whitespace, like Go's json.RawMessage). .gz is auto-detected by
    the last extension (Go filepath.Ext)."""

    def __init__(self, fp: Any, owns: bool) -> None:
        self._fp = fp
        self._owns = owns
        self._peeked: str | None = None
        self._has_peeked = False
        self._count = 0

    @classmethod
    def open(cls, fn: str) -> NDJSONDecoder:
        is_gz = os.path.splitext(fn)[1] == ".gz"
        try:
            # gzip.open / open returns an object that OWNS + closes the underlying file on close().
            raw: Any = gzip.open(fn, "rb") if is_gz else open(fn, "rb")
        except OSError as e:
            raise OSError(f"error opening: {fn}. {e}") from e  # PARITY: os.Open error
        if is_gz:
            try:
                raw.peek(1)  # PARITY: Go gzip.NewReader reads/validates the header eagerly
            except (OSError, EOFError) as e:
                raw.close()
                raise OSError(f"gzip: error opening: {fn}. {e}") from e
        return cls(io.TextIOWrapper(raw, encoding="utf-8", newline=""), owns=True)

    def count(self) -> int:
        return self._count

    def more(self) -> bool:
        """PARITY: More — True while another JSON value remains (blank/whitespace-only lines skipped)."""
        if self._has_peeked:
            return self._peeked is not None
        while True:
            try:
                line = self._fp.readline()
            except (OSError, EOFError):
                # PARITY: Go's json.Decoder.More() swallows a refill/decompress error and returns false, so a
                # corrupt/truncated gzip after a valid header ends the loop gracefully (silent success).
                self._peeked = None
                self._has_peeked = True
                return False
            if line == "":  # EOF
                self._peeked = None
                self._has_peeked = True
                return False
            stripped = line.strip()
            if stripped == "":  # blank line — json.Decoder skips inter-value whitespace
                continue
            self._peeked = stripped
            self._has_peeked = True
            return True

    def decode_raw(self) -> str:
        """PARITY: Decode(&RawMessage) — the raw value text (syntax-validated), count incremented."""
        if not self.more() or self._peeked is None:
            raise ValueError("no more JSON values")
        value = self._peeked
        self._peeked = None
        self._has_peeked = False
        json.loads(value, parse_constant=_reject_constant)  # validate (Go rejects NaN/Infinity)
        self._count += 1
        return value

    def close(self) -> None:
        if self._owns and self._fp is not None:
            self._fp.close()
            self._fp = None

    def __enter__(self) -> NDJSONDecoder:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def json_diff(obj: dict[str, object], found: list[str]) -> list[str]:
    """PARITY: util.JSONDiff — the keys in ``obj`` not present in ``found``.

    Go iterates a map (random order); Python preserves dict insertion order. Order is not relied upon
    here — callers that need determinism sort the result."""
    return [key for key in obj if key not in found]
