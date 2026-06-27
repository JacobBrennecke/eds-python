"""PARITY: Go net/url (the Parse / Userinfo / Values / escape subset EDS uses).

Python's urllib.parse diverges from Go net/url on escaping byte-sets, userinfo parsing (last '@', first ':'),
query encoding (Go Values.Encode SORTS keys), and the +/space rules per mode — so this is a faithful port of
Go's algorithm. Ground truth = Go net/url; the C# GoUrl.cs is a reduced parser and is NOT followed where it
diverges (scheme case, bad-%, ';' handling) — see DEVIATIONS.md#gourl. Consumers: the SQL-driver connection
strings, to_user_pass, masking.
"""

from __future__ import annotations

from dataclasses import dataclass

_ENCODE_PATH = 1
_ENCODE_PATH_SEGMENT = 2
_ENCODE_HOST = 3
_ENCODE_ZONE = 4
_ENCODE_USER_PASSWORD = 5
_ENCODE_QUERY_COMPONENT = 6
_ENCODE_FRAGMENT = 7

_RESERVED = "$&+,/:;=?@"
_HOST_KEEP = "!$&'()*+,;=:[]<>\""


class _EscapeError(ValueError):
    pass


def _is_hex(b: int) -> bool:
    return 0x30 <= b <= 0x39 or 0x41 <= b <= 0x46 or 0x61 <= b <= 0x66


def _unhex(b: int) -> int:
    if b <= 0x39:
        return b - 0x30
    if b <= 0x46:
        return b - 0x41 + 10
    return b - 0x61 + 10


def _should_escape(c: int, mode: int) -> bool:
    """PARITY: net/url shouldEscape — order matters."""
    ch = chr(c)
    if ("a" <= ch <= "z") or ("A" <= ch <= "Z") or ("0" <= ch <= "9"):
        return False
    if mode in (_ENCODE_HOST, _ENCODE_ZONE) and ch in _HOST_KEEP:
        return False
    if ch in "-_.~":
        return False
    if ch in _RESERVED:
        if mode == _ENCODE_PATH:
            return ch == "?"
        if mode == _ENCODE_PATH_SEGMENT:
            return ch in "/;,?"
        if mode == _ENCODE_USER_PASSWORD:
            return ch in "@/?:"
        if mode == _ENCODE_QUERY_COMPONENT:
            return True
        if mode == _ENCODE_FRAGMENT:
            return False
        # host/zone: the reserved chars not handled by the host block fall through to escape.
    if mode == _ENCODE_FRAGMENT and ch in "!()*":
        return False
    return True


def _escape(s: str, mode: int) -> str:
    out: list[str] = []
    for byte in s.encode("utf-8"):
        if mode == _ENCODE_QUERY_COMPONENT and byte == 0x20:
            out.append("+")
        elif _should_escape(byte, mode):
            out.append(f"%{byte:02X}")  # PARITY: UPPER hex
        else:
            out.append(chr(byte))
    return "".join(out)


def _unescape(s: str, mode: int) -> str:
    bs = s.encode("utf-8")
    n = len(bs)
    out = bytearray()
    i = 0
    while i < n:
        c = bs[i]
        if c == 0x25:  # '%'
            if i + 2 >= n or not _is_hex(bs[i + 1]) or not _is_hex(bs[i + 2]):
                raise _EscapeError(s[i : i + 3])
            if mode == _ENCODE_HOST and _unhex(bs[i + 1]) < 8 and bs[i : i + 3] != b"%25":
                raise _EscapeError(s[i : i + 3])
            out.append((_unhex(bs[i + 1]) << 4) | _unhex(bs[i + 2]))
            i += 3
        elif c == 0x2B:  # '+'
            out.append(0x20 if mode == _ENCODE_QUERY_COMPONENT else 0x2B)
            i += 1
        else:
            if mode in (_ENCODE_HOST, _ENCODE_ZONE) and c < 0x80 and _should_escape(c, mode):
                raise _EscapeError(s[i : i + 1])
            out.append(c)
            i += 1
    return out.decode("utf-8")


def query_escape(s: str) -> str:
    """PARITY: url.QueryEscape (space→'+', UPPER hex)."""
    return _escape(s, _ENCODE_QUERY_COMPONENT)


def query_unescape(s: str) -> str:
    """PARITY: url.QueryUnescape ('+'→space)."""
    return _unescape(s, _ENCODE_QUERY_COMPONENT)


@dataclass(frozen=True)
class Userinfo:
    """PARITY: url.Userinfo (decoded username/password + the colon-present flag)."""

    username: str = ""
    password: str = ""
    password_set: bool = False

    def __str__(self) -> str:
        s = _escape(self.username, _ENCODE_USER_PASSWORD)
        if self.password_set:
            s += ":" + _escape(self.password, _ENCODE_USER_PASSWORD)
        return s


class Values:
    """PARITY: url.Values — an ordered multimap; Encode() SORTS keys."""

    def __init__(self) -> None:
        self._m: dict[str, list[str]] = {}

    def has(self, key: str) -> bool:
        return key in self._m

    def get(self, key: str) -> str:
        vs = self._m.get(key)
        return vs[0] if vs else ""

    def set(self, key: str, value: str) -> None:
        self._m[key] = [value]

    def add(self, key: str, value: str) -> None:
        self._m.setdefault(key, []).append(value)

    def delete(self, key: str) -> None:
        self._m.pop(key, None)

    def __iter__(self):
        return iter(self._m.items())

    def __len__(self) -> int:
        return len(self._m)

    def encode(self) -> str:
        if not self._m:
            return ""
        parts: list[str] = []
        for k in sorted(self._m.keys()):  # PARITY: keys sorted (code-point order)
            ke = query_escape(k)
            for v in self._m[k]:
                parts.append(ke + "=" + query_escape(v))
        return "&".join(parts)


def _valid_optional_port(port: str) -> bool:
    if port == "":
        return True
    if port[0] != ":":
        return False
    return all("0" <= c <= "9" for c in port[1:])


def _split_host_port(hp: str) -> tuple[str, str]:
    host, port = hp, ""
    colon = hp.rfind(":")
    if colon != -1 and _valid_optional_port(hp[colon:]):
        host, port = hp[:colon], hp[colon + 1 :]
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    return host, port


@dataclass
class GoUrl:
    """PARITY: url.URL (the subset EDS reads/writes)."""

    scheme: str = ""
    opaque: str = ""
    user: Userinfo | None = None
    host: str = ""
    path: str = ""
    raw_path: str = ""
    force_query: bool = False
    raw_query: str = ""
    fragment: str = ""
    raw_fragment: str = ""

    # convenience props mirroring the Go/C# call sites
    @property
    def has_user_info(self) -> bool:
        return self.user is not None

    @property
    def username(self) -> str:
        return self.user.username if self.user else ""

    @property
    def has_password(self) -> bool:
        return self.user is not None and self.user.password_set

    @property
    def password(self) -> str:
        return self.user.password if self.user else ""

    def hostname(self) -> str:
        return _split_host_port(self.host)[0]

    def port(self) -> str:
        return _split_host_port(self.host)[1]

    def query(self) -> Values:
        """PARITY: URL.Query — a FRESH parse of raw_query each call."""
        v = Values()
        q = self.raw_query
        while q:
            seg, _, q = q.partition("&")
            if ";" in seg:  # PARITY: Go records an error and skips ';' segments
                continue
            if seg == "":
                continue
            key, _, val = seg.partition("=")
            try:
                key = query_unescape(key)
                val = query_unescape(val)
            except _EscapeError:
                continue  # PARITY: Go drops the pair on a bad escape
            v.add(key, val)
        return v

    def _escaped_path(self) -> str:
        if self.raw_path and _unescape(self.raw_path, _ENCODE_PATH) == self.path:
            return self.raw_path
        return _escape(self.path, _ENCODE_PATH)

    def _escaped_fragment(self) -> str:
        if self.raw_fragment and _unescape(self.raw_fragment, _ENCODE_FRAGMENT) == self.fragment:
            return self.raw_fragment
        return _escape(self.fragment, _ENCODE_FRAGMENT)

    def __str__(self) -> str:
        """PARITY: URL.String."""
        buf: list[str] = []
        if self.scheme:
            buf.append(self.scheme + ":")
        if self.opaque:
            buf.append(self.opaque)
        else:
            if self.scheme or self.host or self.user is not None:
                if self.host or self.path or self.user is not None:
                    buf.append("//")
                if self.user is not None:
                    buf.append(str(self.user) + "@")
                if self.host:
                    buf.append(_escape(self.host, _ENCODE_HOST))
            path = self._escaped_path()
            if path and path[0] != "/" and self.host:
                buf.append("/")
            buf.append(path)
        if self.force_query or self.raw_query:
            buf.append("?" + self.raw_query)
        if self.fragment:
            buf.append("#" + self._escaped_fragment())
        return "".join(buf)


def _get_scheme(raw: str) -> tuple[str, str]:
    for i, ch in enumerate(raw):
        if ("a" <= ch <= "z") or ("A" <= ch <= "Z"):
            continue
        if ("0" <= ch <= "9") or ch in "+-.":
            if i == 0:
                return "", raw
        elif ch == ":":
            if i == 0:
                raise ValueError("missing protocol scheme")
            return raw[:i].lower(), raw[i + 1 :]  # PARITY: scheme lower-cased
        else:
            return "", raw
    return "", raw


def _parse_authority(authority: str) -> tuple[Userinfo | None, str]:
    i = authority.rfind("@")  # PARITY: LAST '@'
    if i < 0:
        return None, _parse_host(authority)
    host = _parse_host(authority[i + 1 :])
    userinfo = authority[:i]
    if ":" not in userinfo:
        return Userinfo(_unescape(userinfo, _ENCODE_USER_PASSWORD), "", False), host
    uname, _, pword = userinfo.partition(":")  # PARITY: FIRST ':'
    return Userinfo(
        _unescape(uname, _ENCODE_USER_PASSWORD), _unescape(pword, _ENCODE_USER_PASSWORD), True
    ), host


def _parse_host(host: str) -> str:
    if host.startswith("["):
        i = host.rfind("]")
        if i < 0:
            raise ValueError("missing ']' in host")
        colon_port = host[i + 1 :]
        if colon_port and not _valid_optional_port(colon_port):
            raise ValueError(f"invalid port {colon_port} after host")
        return _unescape(host, _ENCODE_HOST)
    colon = host.rfind(":")
    if colon != -1 and not _valid_optional_port(host[colon:]):
        raise ValueError(f"invalid port {host[colon:]} after host")
    return _unescape(host, _ENCODE_HOST)


def parse(raw_url: str) -> GoUrl:
    """PARITY: url.Parse."""
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in raw_url):
        raise ValueError("net/url: invalid control character in URL")
    rest, sep, frag = raw_url.partition("#")
    u = _parse(rest)
    if sep:
        u.fragment = _unescape(frag, _ENCODE_FRAGMENT)
        u.raw_fragment = frag if _escape(u.fragment, _ENCODE_FRAGMENT) != frag else ""
    return u


def _parse(rest: str) -> GoUrl:
    u = GoUrl()
    u.scheme, rest = _get_scheme(rest)

    if rest.endswith("?") and rest.count("?") == 1:
        u.force_query = True
        rest = rest[:-1]
    else:
        rest, _, u.raw_query = rest.partition("?")

    if not rest.startswith("/"):
        if u.scheme != "":
            u.opaque = rest
            return u
        segment = rest.split("/", 1)[0]
        if ":" in segment:
            raise ValueError("first path segment in URL cannot contain colon")

    if (u.scheme != "" or not rest.startswith("///")) and rest.startswith("//"):
        authority, slash, tail = rest[2:].partition("/")
        rest = slash + tail
        u.user, u.host = _parse_authority(authority)

    u.path = _unescape(rest, _ENCODE_PATH)
    u.raw_path = rest if _escape(u.path, _ENCODE_PATH) != rest else ""
    return u
