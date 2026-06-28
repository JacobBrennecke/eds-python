"""PARITY: the config.toml writer (write_config / set_config_value / _dump_toml)."""

from __future__ import annotations

import tomli

from eds.cmd.config import _dump_toml, load_config, set_config_value, write_config


def test_write_and_read_config(tmp_path) -> None:
    write_config(str(tmp_path), {"token": "abc", "server_id": "srv1", "keep_logs": True})
    c = load_config(str(tmp_path))
    assert c.get_string("token") == "abc"
    assert c.get_string("server_id") == "srv1"
    assert c.get_bool("keep_logs") is True


def test_set_config_value_read_modify_write(tmp_path) -> None:
    write_config(str(tmp_path), {"token": "abc", "server_id": "srv1"})
    set_config_value(str(tmp_path), "url", "postgres://x")  # configure: add url
    set_config_value(str(tmp_path), "server_id", "")  # shutdown: de-enroll
    c = load_config(str(tmp_path))
    assert c.get_string("token") == "abc"  # preserved
    assert c.get_string("url") == "postgres://x"  # added
    assert c.get_string("server_id") == ""  # cleared


def test_set_config_value_on_absent_file(tmp_path) -> None:
    set_config_value(str(tmp_path), "url", "postgres://y")  # no prior config.toml → creates it
    assert load_config(str(tmp_path)).get_string("url") == "postgres://y"


def test_dump_toml_round_trips_via_tomli() -> None:
    values = {"a": 'he said "hi" \\ path', "flag": True, "off": False, "n": 5, "url": "postgres://u:p@h/db"}
    assert tomli.loads(_dump_toml(values)) == values
    assert _dump_toml({}) == ""


def test_dump_toml_escapes_control_chars() -> None:
    # TOML basic strings must escape control chars; a raw newline/tab would otherwise fail to parse back.
    values = {"x": "a\nb\tc\rd", "y": "ctrl\x01char"}
    assert tomli.loads(_dump_toml(values)) == values
