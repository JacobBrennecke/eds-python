"""PARITY: Go net/url subset — vectors from the GoUrl grounding (mask/connstring/DSN oracle)."""

from __future__ import annotations

import pytest

from eds.util.gourl import GoUrl, Userinfo, Values, parse, query_escape, query_unescape
from eds.util.sql import to_user_pass


@pytest.mark.parametrize(
    ("url", "scheme", "has_ui", "user", "has_pw", "pw", "host", "path", "raw_query"),
    [
        ("postgres://localhost", "postgres", False, "", False, "", "localhost", "", ""),
        ("postgres://localhost:15432", "postgres", False, "", False, "", "localhost:15432", "", ""),
        ("postgres://localhost:15432?application_name=foo&sslmode=disable", "postgres", False, "", False, "",
         "localhost:15432", "", "application_name=foo&sslmode=disable"),
        ("postgres://user:pass@hostname:1234/db", "postgres", True, "user", True, "pass", "hostname:1234", "/db", ""),
        ("postgres://user:@hostname:1234/db", "postgres", True, "user", True, "", "hostname:1234", "/db", ""),
        ("postgres://user@hostname:1234/db", "postgres", True, "user", False, "", "hostname:1234", "/db", ""),
        ("mysql://root:password@localhost:3306/dbname", "mysql", True, "root", True, "password",
         "localhost:3306", "/dbname", ""),
        ("sqlserver://sa:eds@localhost:11433/eds", "sqlserver", True, "sa", True, "eds", "localhost:11433", "/eds", ""),
        # last '@' splits userinfo from host; first ':' splits user from password
        ("scheme://a:b@c:d@host/p", "scheme", True, "a", True, "b@c:d", "host", "/p", ""),
        ("scheme://user:p:w@host", "scheme", True, "user", True, "p:w", "host", "", ""),
        ("scheme://host/p?x=1#frag", "scheme", False, "", False, "", "host", "/p", "x=1"),  # fragment dropped
        ("scheme://host/a%20b", "scheme", False, "", False, "", "host", "/a b", ""),  # path %-decode, no +
        ("scheme://host/a+b", "scheme", False, "", False, "", "host", "/a+b", ""),  # path keeps +
        ("scheme://u%40s%3Aer:p%40ss@host", "scheme", True, "u@s:er", True, "p@ss", "host", "", ""),
    ],
)
def test_parse(url, scheme, has_ui, user, has_pw, pw, host, path, raw_query) -> None:
    u = parse(url)
    assert u.scheme == scheme
    assert u.has_user_info == has_ui
    assert u.username == user
    assert u.has_password == has_pw
    assert u.password == pw
    assert u.host == host
    assert u.path == path
    assert u.raw_query == raw_query


def test_parse_lowercases_scheme() -> None:
    assert parse("Postgres://host/db").scheme == "postgres"  # PARITY: Go lower-cases the scheme


def test_hostname_and_port() -> None:
    u = parse("postgres://localhost:5432/db")
    assert u.hostname() == "localhost"
    assert u.port() == "5432"
    assert parse("postgres://localhost/db").port() == ""


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("pg://host/db", ""),
        ("pg://user@host/db", "user"),
        ("pg://user:@host/db", "user:"),
        ("pg://user:pass@host/db", "user:pass"),
        ("pg://:pass@host/db", ":pass"),
        ("pg://:@host/db", ":"),
    ],
)
def test_to_user_pass(url, expected) -> None:
    assert to_user_pass(parse(url)) == expected


@pytest.mark.parametrize(
    ("s", "expected"),
    [
        ("", ""), (" ", "+"), ("abcXYZ019", "abcXYZ019"), ("-_.~", "-_.~"),
        ("!", "%21"), ('"', "%22"), ("#", "%23"), ("$", "%24"), ("%", "%25"), ("&", "%26"),
        ("'", "%27"), ("+", "%2B"), (",", "%2C"), ("/", "%2F"), (":", "%3A"), ("=", "%3D"),
        ("?", "%3F"), ("@", "%40"), ("[", "%5B"), ("]", "%5D"),
        ("\t", "%09"), ("\n", "%0A"), ("\x7f", "%7F"),
        ("é", "%C3%A9"), ("中", "%E4%B8%AD"), ("💩", "%F0%9F%92%A9"),
        ("app name", "app+name"), ("a b+c", "a+b%2Bc"), ("a%2Fb", "a%252Fb"),
    ],
)
def test_query_escape(s, expected) -> None:
    assert query_escape(s) == expected


def test_query_unescape() -> None:
    assert query_unescape("a+b%2Bc") == "a b+c"
    assert query_unescape("%E4%B8%AD") == "中"


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ([], ""),
        ([("a", ["b"])], "a=b"),
        ([("sslmode", ["disable"]), ("application_name", ["eds"])], "application_name=eds&sslmode=disable"),
        ([("b", ["2"]), ("a", ["1"])], "a=1&b=2"),
        ([("a", ["1", "2"])], "a=1&a=2"),
        ([("a", [""])], "a="),
        ([("z", ["1"]), ("Z", ["1"]), ("a", ["1"])], "Z=1&a=1&z=1"),  # byte sort: upper < lower
        ([("k!", ["v@"])], "k%21=v%40"),
        ([("a b", ["c d"])], "a+b=c+d"),
    ],
)
def test_values_encode(items, expected) -> None:
    v = Values()
    for k, vals in items:
        for val in vals:
            v.add(k, val)
    assert v.encode() == expected


@pytest.mark.parametrize(
    ("raw_query", "expected"),
    [
        ("", []),
        ("a=b", [("a", ["b"])]),
        ("a=b&c=d", [("a", ["b"]), ("c", ["d"])]),
        ("a=1&a=2", [("a", ["1", "2"])]),
        ("a", [("a", [""])]),
        ("a=b&&c=d", [("a", ["b"]), ("c", ["d"])]),  # empty segment skipped
        ("app+name=eds", [("app name", ["eds"])]),
        ("q=a%2Bb", [("q", ["a+b"])]),
        ("a=b;c=d", []),  # PARITY: ';' is not a separator AND the ';' segment is dropped (Go)
    ],
)
def test_query_parse(raw_query, expected) -> None:
    u = GoUrl(raw_query=raw_query)
    assert list(u.query()) == expected


def test_userinfo_string_escapes_vs_to_user_pass_decoded() -> None:
    u = Userinfo("u@x", "p:w", True)
    assert str(u) == "u%40x:p%3Aw"  # escaped (encodeUserPassword)
    assert to_user_pass(GoUrl(user=u)) == "u@x:p:w"  # decoded


def test_control_char_raises() -> None:
    with pytest.raises(ValueError, match="invalid control character"):
        parse("postgres://ho\nst/db")
