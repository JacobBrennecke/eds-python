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


def format_g(f: float) -> str:
    """PARITY: strconv.FormatFloat(f, 'g', -1, 64) — shortest round-trip; exponential when the leading-digit
    decimal exponent E < -4 or E >= 21 (else plain). Used by the MySQL value quoter (distinct from format_f,
    which never goes exponential)."""
    if math.isnan(f):
        return "NaN"
    if f == math.inf:
        return "+Inf"
    if f == -math.inf:
        return "-Inf"
    if f == 0:
        return _zero(f)
    neg = f < 0
    _, digs, exp = Decimal(repr(abs(f))).as_tuple()
    digits = list(digs)
    while len(digits) > 1 and digits[-1] == 0:  # shortest: strip trailing zeros
        digits.pop()
        exp = int(exp) + 1
    sig = "".join(map(str, digits))
    e = int(exp) + (len(sig) - 1)  # exponent of the leading significant digit
    if e < -4 or e >= 21:
        mant = sig if len(sig) == 1 else sig[0] + "." + sig[1:]
        s = f"{mant}e{'+' if e >= 0 else '-'}{abs(e):02d}"  # lowercase e, signed, >=2 exp digits
    else:
        pp = e + 1  # count of integer digits
        if pp <= 0:
            s = "0." + "0" * (-pp) + sig
        elif pp >= len(sig):
            s = sig + "0" * (pp - len(sig))
        else:
            s = sig[:pp] + "." + sig[pp:]
    return "-" + s if neg else s


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
