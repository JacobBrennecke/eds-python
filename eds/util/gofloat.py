"""Go float formatting parity (``strconv.FormatFloat`` + ``encoding/json`` floatEncoder).

Risk #1: Python's ``repr``/``%g`` and Go's ``strconv`` choose exponential-vs-decimal at different
thresholds, so float bytes must be built deliberately. Python's ``repr(float)`` gives the same shortest
round-tripping digits as Go's ``strconv(..., -1, 64)``; this module reshapes those digits into Go's
chosen format. Vectors verified against Go by the C# port (GoFloatTests / JsonTests).
"""

from __future__ import annotations

import math
from decimal import Decimal


def _zero(f: float) -> str:
    # PARITY: strconv keeps the sign of negative zero ("-0").
    return "-0" if math.copysign(1.0, f) < 0 else "0"


def format_f(f: float) -> str:
    """PARITY: strconv.FormatFloat(f, 'f', -1, 64) — shortest plain decimal, never exponential.

    Used by the PostgreSQL/Snowflake value quoters (e.g. 1e-7 → "0.0000001")."""
    if f == 0:
        return _zero(f)
    s = format(Decimal(repr(f)), "f")  # repr = shortest digits; Decimal 'f' = no exponent
    if "." in s:
        s = s.rstrip("0").rstrip(".")  # strconv -1 emits no trailing zeros / dot
    return s


def format_json(f: float) -> str:
    """PARITY: encoding/json floatEncoder — 'f' for abs in [1e-6, 1e21), else lowercase 'e' with the
    ``e-0X`` → ``e-X`` exponent cleanup (Go only strips the leading zero on NEGATIVE 2-digit exponents;
    positive exponents in the 'e' range are >= 21 and never leading-zero). NaN/Inf raise (json.Marshal errors)."""
    if not math.isfinite(f):
        raise ValueError("json: unsupported value: non-finite float")
    if f == 0:
        return _zero(f)
    abs_f = abs(f)
    if abs_f < 1e-6 or abs_f >= 1e21:
        s = repr(f)  # repr is exponential in this range, with shortest mantissa + lowercase 'e'
        if len(s) >= 4 and s[-4] == "e" and s[-3] == "-" and s[-2] == "0":
            s = s[:-2] + s[-1]
        return s
    return format_f(f)
