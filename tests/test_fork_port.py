"""The fork --port flag: default 8080 (env-independent), overridable by the CLI flag.

DEVIATION: see DEVIATIONS.md#fork-port-default — Go's fork default is the literal 0 and the server always
forwards an explicit --port; the port defaults to a usable 8080 so a directly-invoked fork is functional, and
does NOT read $PORT (Go's fork ignores it)."""

from __future__ import annotations

from eds.cmd.root import build_parser


def test_fork_port_defaults_to_8080_without_flag(monkeypatch) -> None:
    # even with $PORT set in the environment, the fork default is a fixed 8080 (Go's fork ignores $PORT)
    monkeypatch.setenv("PORT", "5555")
    args = build_parser().parse_args(["fork"])
    assert args.port == 8080


def test_fork_port_flag_overrides_default() -> None:
    args = build_parser().parse_args(["fork", "--port", "9999"])
    assert args.port == 9999
