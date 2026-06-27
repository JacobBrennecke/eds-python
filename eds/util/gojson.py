"""PARITY: Go ``encoding/json`` Marshal reproduction (the byte-critical streaming/SQL JSON encoder).

Differs from Python's ``json.dumps`` in every way that matters: sorted map keys, compact separators,
HTML escaping (``<``/``>``/``&`` → ``\\u003c``/``\\u003e``/``\\u0026``), U+2028/U+2029 escaping,
``\\u00XX`` (not ``\\b``/``\\f``) for the other control chars, non-ASCII left verbatim, and Go's float
formatting. Vectors verified against Go by the C# port (JsonTests).
"""

from __future__ import annotations

import math

from eds.util.gofloat import format_json


class RawJson:
    """PARITY: encoding/json.RawMessage — a pre-encoded JSON fragment emitted verbatim."""

    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value

    def __repr__(self) -> str:  # pragma: no cover
        return f"RawJson({self.value!r})"


def stringify(val: object) -> str:
    """PARITY: util.JSONStringify — ``buf, _ := json.Marshal(val); return string(buf)``.

    The marshal error is ignored, so non-finite floats (and other unmarshalable values) yield ``""``."""
    try:
        return marshal(val)
    except ValueError:
        return ""


def marshal(val: object) -> str:
    """Go ``json.Marshal`` byte output as a str. Raises ValueError where Go returns an error
    (non-finite float), so callers needing the error (the driver path) can surface it."""
    out: list[str] = []
    _encode(val, out)
    return "".join(out)


def _encode(v: object, out: list[str]) -> None:
    if v is None:
        out.append("null")
    elif isinstance(v, RawJson):
        out.append(v.value)
    elif isinstance(v, bool):
        # MUST precede int — bool is an int subclass.
        out.append("true" if v else "false")
    elif isinstance(v, int):
        out.append(str(v))
    elif isinstance(v, float):
        if not math.isfinite(v):
            raise ValueError("json: unsupported value: non-finite float")
        out.append(format_json(v))
    elif isinstance(v, str):
        _encode_string(v, out)
    elif isinstance(v, (list, tuple)):
        out.append("[")
        for i, e in enumerate(v):
            if i:
                out.append(",")
            _encode(e, out)
        out.append("]")
    elif isinstance(v, dict):
        out.append("{")
        # PARITY: Go sorts map[string] keys; Python str sort == UTF-8 byte order for valid Unicode.
        for i, key in enumerate(sorted(v.keys())):
            if i:
                out.append(",")
            _encode_string(str(key), out)
            out.append(":")
            _encode(v[key], out)
        out.append("}")
    else:
        raise ValueError(f"json: unsupported type: {type(v).__name__}")


def _encode_string(s: str, out: list[str]) -> None:
    """PARITY: encodeState.string with escapeHTML=true."""
    out.append('"')
    for ch in s:
        o = ord(ch)
        if o == 0x22:
            out.append('\\"')
        elif o == 0x5C:
            out.append("\\\\")
        elif o == 0x0A:
            out.append("\\n")
        elif o == 0x0D:
            out.append("\\r")
        elif o == 0x09:
            out.append("\\t")
        elif o < 0x20:
            out.append(f"\\u{o:04x}")  # other controls: \u00XX (Go uses no \b/\f short forms)
        elif o == 0x3C:
            out.append("\\u003c")  # <
        elif o == 0x3E:
            out.append("\\u003e")  # >
        elif o == 0x26:
            out.append("\\u0026")  # &
        elif o == 0x2028:
            out.append("\\u2028")
        elif o == 0x2029:
            out.append("\\u2029")
        else:
            out.append(ch)  # ASCII printable or non-ASCII, verbatim (Go leaves UTF-8 raw)
    out.append('"')
