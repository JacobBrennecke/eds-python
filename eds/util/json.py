"""PARITY: internal/util/json.go — JSON helpers.

The NDJSON(.gz) stream decoder (NewNDJSONDecoder) is deferred to M2/M5 (it is consumed by the
importer). The byte-critical Go ``json.Marshal`` reproduction (sorted keys, HTML + U+2028/9 escaping,
Go float formatting) lives in ``eds.util.gojson`` and is exercised by the driver golden tests.
"""

from __future__ import annotations


def json_diff(obj: dict[str, object], found: list[str]) -> list[str]:
    """PARITY: util.JSONDiff — the keys in ``obj`` not present in ``found``.

    Go iterates a map (random order); Python preserves dict insertion order. Order is not relied upon
    here — callers that need determinism sort the result."""
    return [key for key in obj if key not in found]
