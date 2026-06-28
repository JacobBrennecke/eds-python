"""PARITY: runner CLI foundation — exit codes, arg filtering, env ints, duration parsing, config.toml."""

from __future__ import annotations

import pytest

from eds.cmd import exit_codes
from eds.cmd.args import collect_command_args, get_os_int, parse_duration
from eds.cmd.config import ConfigError, init_config, load_config


def test_exit_code_contract() -> None:
    assert (exit_codes.EXIT_SUCCESS, exit_codes.EXIT_ERROR, exit_codes.EXIT_PANIC) == (0, 1, 2)
    assert exit_codes.EXIT_INCORRECT_USAGE == 3
    assert exit_codes.EXIT_RESTART == 4
    assert exit_codes.EXIT_NATS_DISCONNECTED == 5
    assert exit_codes.MAX_FAILURES == 5


def test_collect_command_args_drops_ignored_and_next_token() -> None:
    # --api-url + value dropped (skip-next); --companyIds + value kept; --verbose kept
    args = ["--api-url", "https://x", "--companyIds", "c1", "--verbose"]
    assert collect_command_args(args) == ["--companyIds", "c1", "--verbose"]


def test_collect_command_args_skip_next_quirk_on_attached_value() -> None:
    # faithful quirk: an ignored flag ALWAYS drops the following token, even when it carried an attached value
    assert collect_command_args(["--port=9", "--consumer-suffix", "s"]) == ["s"]
    # standalone boolean ignored flag also eats the next token
    assert collect_command_args(["--silent", "--verbose"]) == []


def test_get_os_int(monkeypatch) -> None:
    monkeypatch.delenv("EDS_TEST_PORT", raising=False)
    assert get_os_int("EDS_TEST_PORT", 8080) == 8080
    monkeypatch.setenv("EDS_TEST_PORT", "1234")
    assert get_os_int("EDS_TEST_PORT", 8080) == 1234
    monkeypatch.setenv("EDS_TEST_PORT", "notanint")
    assert get_os_int("EDS_TEST_PORT", 8080) == 8080


def test_parse_duration() -> None:
    assert parse_duration("0") == 0.0
    assert parse_duration("500ms") == 0.5
    assert parse_duration("2s") == 2.0
    assert parse_duration("1m") == 60.0
    assert parse_duration("24h") == 86400.0
    assert parse_duration("1h30m") == 5400.0
    assert parse_duration("-2s") == -2.0
    with pytest.raises(ValueError):
        parse_duration("2x")
    with pytest.raises(ValueError):
        parse_duration("2s junk")


def test_load_config_absent_returns_empty(tmp_path) -> None:
    c = load_config(str(tmp_path))
    assert c.get_string("token") == ""
    assert c.get_bool("keep_logs") is False
    assert not c.has("url")


def test_load_config_reads_toml(tmp_path) -> None:
    (tmp_path / "config.toml").write_text(
        'url = "postgres://x"\ntoken = "abc"\nserver_id = "srv1"\nkeep_logs = true\n', encoding="utf-8"
    )
    c = load_config(str(tmp_path))
    assert c.get_string("url") == "postgres://x"
    assert c.get_string("token") == "abc"
    assert c.get_string("server_id") == "srv1"
    assert c.get_bool("keep_logs") is True
    assert c.get_string("missing", "def") == "def"


def test_load_config_bad_toml_raises(tmp_path) -> None:
    (tmp_path / "config.toml").write_text("this is = = not toml", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(str(tmp_path))


def test_init_config_enroll_guard(tmp_path) -> None:
    (tmp_path / "config.toml").write_text("not = = toml", encoding="utf-8")
    # enroll in argv → tolerate the bad config, return empty
    assert init_config(str(tmp_path), ["enroll", "CODE"]).get_string("token") == ""
    # otherwise → exit 3
    with pytest.raises(SystemExit) as ei:
        init_config(str(tmp_path), ["server"])
    assert ei.value.code == exit_codes.EXIT_INCORRECT_USAGE
