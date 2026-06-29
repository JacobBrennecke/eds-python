"""FEATURE(audit-mode): plumbing tests — IngestMode enum/parser, DriverConfig carrier, --mode flag parsing,
the server resolve+persist precedence, and the SERVER_IGNORE_FLAGS / explicit-forward wiring.

See migration/features/audit-mode.md §1.1 (precedence + persistence) and the SPEC §5 (config plumbing path).
"""

from __future__ import annotations

import pytest

from eds.cmd.args import SERVER_IGNORE_FLAGS, collect_command_args
from eds.cmd.config import Config, load_config, set_config_value
from eds.cmd.exit_codes import EXIT_INCORRECT_USAGE
from eds.cmd.root import build_parser
from eds.cmd.server import resolve_ingest_mode
from eds.driver import DriverConfig, IngestMode, parse_ingest_mode

# ---- enum + parser ----

def test_ingest_mode_is_str_enum() -> None:
    assert IngestMode.UPSERT == "upsert"
    assert IngestMode.APPEND == "append"
    assert IngestMode.UPSERT.value == "upsert"


@pytest.mark.parametrize(
    ("s", "expected"),
    [("upsert", IngestMode.UPSERT), ("append", IngestMode.APPEND),
     ("UPSERT", IngestMode.UPSERT), ("Append", IngestMode.APPEND)],
)
def test_parse_ingest_mode_ok(s, expected) -> None:
    assert parse_ingest_mode(s) == expected


@pytest.mark.parametrize("s", ["", "merge", "Upserts", "appendx"])
def test_parse_ingest_mode_bad_raises(s) -> None:
    with pytest.raises(ValueError):
        parse_ingest_mode(s)


def test_driver_config_default_is_upsert() -> None:
    assert DriverConfig().ingest_mode == IngestMode.UPSERT


# ---- CLI flag wiring ----

def test_server_mode_flag_default_is_none_sentinel() -> None:
    args = build_parser().parse_args(["server"])
    assert args.mode is None  # not explicitly set


def test_server_mode_flag_parses_to_enum() -> None:
    args = build_parser().parse_args(["server", "--mode", "append"])
    assert args.mode == IngestMode.APPEND


def test_fork_mode_flag_default_is_upsert_enum() -> None:
    args = build_parser().parse_args(["fork"])
    assert args.mode == IngestMode.UPSERT  # directly-invoked fork is byte-identical to today


def test_fork_mode_flag_parses_to_enum() -> None:
    args = build_parser().parse_args(["fork", "--mode", "append"])
    assert args.mode == IngestMode.APPEND


def test_bad_mode_value_exits_3() -> None:
    with pytest.raises(SystemExit) as ei:
        build_parser().parse_args(["server", "--mode", "garbage"])
    assert ei.value.code == EXIT_INCORRECT_USAGE


# ---- collect_command_args drops --mode (server forwards the resolved value explicitly) ----

def test_mode_in_server_ignore_flags() -> None:
    assert "--mode" in SERVER_IGNORE_FLAGS


def test_collect_command_args_drops_user_mode() -> None:
    # a user-supplied --mode on the server argv must NOT auto-forward (server appends the RESOLVED value).
    assert collect_command_args(["--mode", "append", "--verbose"]) == ["--verbose"]


# ---- resolve + persist precedence (§1.1) ----

def test_resolve_explicit_flag_wins_and_persists(tmp_path) -> None:
    set_config_value(str(tmp_path), "mode", "upsert")  # config says upsert
    mode = resolve_ingest_mode(IngestMode.APPEND, load_config(str(tmp_path)), str(tmp_path))
    assert mode == IngestMode.APPEND  # explicit flag wins over config
    assert load_config(str(tmp_path)).get_string("mode") == "append"  # persisted


def test_resolve_config_used_when_no_flag(tmp_path) -> None:
    set_config_value(str(tmp_path), "mode", "append")
    set_config_value(str(tmp_path), "token", "tok")  # an unrelated key must survive
    mode = resolve_ingest_mode(None, load_config(str(tmp_path)), str(tmp_path))
    assert mode == IngestMode.APPEND
    assert load_config(str(tmp_path)).get_string("token") == "tok"  # not rewritten away


def test_resolve_default_upsert_persisted_when_absent(tmp_path) -> None:
    mode = resolve_ingest_mode(None, Config(), str(tmp_path))  # no flag, no config key
    assert mode == IngestMode.UPSERT
    assert load_config(str(tmp_path)).get_string("mode") == "upsert"  # written back, self-documenting


def test_resolve_explicit_persist_preserves_other_keys(tmp_path) -> None:
    set_config_value(str(tmp_path), "token", "abc")
    set_config_value(str(tmp_path), "url", "postgres://x")
    resolve_ingest_mode(IngestMode.APPEND, load_config(str(tmp_path)), str(tmp_path))
    c = load_config(str(tmp_path))
    assert c.get_string("token") == "abc"
    assert c.get_string("url") == "postgres://x"
    assert c.get_string("mode") == "append"
