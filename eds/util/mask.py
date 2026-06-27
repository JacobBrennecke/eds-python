"""PARITY: internal/util/mask.go — mask sensitive info in URLs / emails / JWTs / CLI args.

``cstr.Mask`` (go-common) is reproduced from the Go test vectors: keep the first ``len // 2``
characters, replace the remaining ``len - len // 2`` with ``*`` (verified against every vector in
mask_test.go, e.g. ``"password"`` → ``"pass****"``, ``"TEST/PUBLIC"`` → ``"TEST/******"``).
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, unquote, urlsplit

# RE2 parity: Go's `\w` is ASCII-only (use re.ASCII), and `$` (no multiline) is end-of-text (use \Z;
# Python's `$` would also match before a trailing newline). See DEVIATIONS#regex-re2-vs-python.
_IS_URL = re.compile(r"^(\w+)://", re.ASCII)
_IS_EMAIL = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\Z")
_IS_JWT = re.compile(r"^[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\Z")


def mask(s: str) -> str:
    """PARITY: go-common cstr.Mask — keep the first half, star out the rest.

    Go measures length in bytes; this uses character length, identical for the ASCII credentials/URLs
    EDS masks (DEVIATION only for non-ASCII input, which these never are)."""
    keep = len(s) // 2
    return s[:keep] + "*" * (len(s) - keep)


def mask_url(url_string: str) -> str:
    """PARITY: util.MaskURL — mask userinfo, the path tail, and each query value; sort the query.

    Raises ValueError on a parse failure (Go returns an error there; MaskArguments falls back to mask())."""
    try:
        sp = urlsplit(url_string)
    except ValueError as e:  # pragma: no cover - urlsplit rarely raises
        raise ValueError(f"failed to parse URL: {e}") from e

    out = [sp.scheme, "://"]
    userinfo, sep, host = sp.netloc.rpartition("@")
    if sep:  # PARITY: u.User != nil
        user, colon, pw = userinfo.partition(":")
        out.append(mask(unquote(user)))
        if colon:  # PARITY: password "ok" = a ":" was present
            out.append(":")
            out.append(mask(unquote(pw)))
        out.append("@")
    out.append(host)  # PARITY: u.Host preserves case (do NOT lowercase like .hostname)

    p = sp.path
    if p not in ("/", ""):
        out.append("/")
        if len(p) > 1 and p[0] == "/":
            out.append(mask(p[1:]))

    # PARITY: u.Query() groups repeated keys + URL-decodes; values joined by "," then masked; lines sorted.
    qs = parse_qs(sp.query, keep_blank_values=True, separator="&")
    pairs = [f"{k}={mask(','.join(v))}" for k, v in qs.items()]
    pairs.sort()
    if pairs:
        out.append("?")
        out.append("&".join(pairs))
    return "".join(out)


def mask_email(val: str) -> str:
    """PARITY: util.MaskEmail — mask the local part and the first domain label."""
    tok = val.split("@")
    dot = tok[1].split(".")
    return mask(tok[0]) + "@" + mask(dot[0]) + "." + ".".join(dot[1:])


def mask_arguments(args: list[str]) -> list[str]:
    """PARITY: util.MaskArguments — URL → MaskURL (fallback mask on error), email → MaskEmail,
    JWT → mask, else unchanged."""
    masked: list[str] = []
    for arg in args:
        if _IS_URL.match(arg):
            try:
                masked.append(mask_url(arg))
            except ValueError:
                masked.append(mask(arg))
        elif _IS_EMAIL.match(arg):
            masked.append(mask_email(arg))
        elif _IS_JWT.match(arg):
            masked.append(mask(arg))
        else:
            masked.append(arg)
    return masked
