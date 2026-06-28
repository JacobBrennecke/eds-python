"""PARITY: cmd/root.go initConfig + viper config.toml reads.

Go uses viper (config + BindPFlag + env merge). The Python port reads <data-dir>/config.toml with tomli into a
dict and exposes get_string/get_bool. Flag-over-config precedence is applied by the caller
(e.g. `cli_api_key or config.get_string("token")`), mirroring viper.BindPFlag.
"""

from __future__ import annotations

import os
import sys

import tomli

from eds.cmd.exit_codes import EXIT_INCORRECT_USAGE


class ConfigError(Exception):
    """Raised when config.toml exists but cannot be read/parsed."""


class Config:
    """A read-only view of config.toml (viper analog)."""

    def __init__(self, values: dict | None = None) -> None:
        self._v = values or {}

    def has(self, key: str) -> bool:
        return key in self._v

    def get_string(self, key: str, default: str = "") -> str:
        v = self._v.get(key)
        if v is None:
            return default
        return v if isinstance(v, str) else str(v)

    def get_bool(self, key: str, default: bool = False) -> bool:
        v = self._v.get(key)
        return default if v is None else bool(v)

    def as_dict(self) -> dict:
        return dict(self._v)


def config_path(data_dir: str) -> str:
    return os.path.join(data_dir, "config.toml")


def _dump_toml(values: dict) -> str:
    """Serialize a FLAT config dict to TOML (the config.toml is only strings + bools; no nested tables).

    DEVIATION (config-toml-handwritten-writer): Go uses BurntSushi/toml; tomli has no writer and tomli-w is not a
    dep, so a minimal flat encoder is used. Sufficient for token/server_id/url (str) + keep_logs (bool); the value
    is never byte-compared (it is read back by tomli)."""
    lines: list[str] = []
    for key, val in values.items():
        if isinstance(val, bool):
            lines.append(f"{key} = {str(val).lower()}")
        elif isinstance(val, (int, float)):
            lines.append(f"{key} = {val}")
        else:
            lines.append(f'{key} = "{_escape_toml_basic(str(val))}"')
    return ("\n".join(lines) + "\n") if lines else ""


_TOML_ESCAPES = {"\\": "\\\\", '"': '\\"', "\b": "\\b", "\t": "\\t", "\n": "\\n", "\f": "\\f", "\r": "\\r"}


def _escape_toml_basic(s: str) -> str:
    """Escape a TOML basic-string value (backslash, quote, and control chars per the spec)."""
    out: list[str] = []
    for ch in s:
        esc = _TOML_ESCAPES.get(ch)
        if esc is not None:
            out.append(esc)
        elif ord(ch) < 0x20 or ord(ch) == 0x7F:
            out.append(f"\\u{ord(ch):04X}")
        else:
            out.append(ch)
    return "".join(out)


def write_config(data_dir: str, values: dict) -> None:
    """Write the config dict to <data_dir>/config.toml (mode 0644). PARITY: enroll.go toml.NewEncoder + 0644."""
    path = config_path(data_dir)
    data = _dump_toml(values).encode()
    # O_BINARY avoids Windows newline translation (write the TOML bytes verbatim).
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0), 0o644)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)


def set_config_value(data_dir: str, key: str, value: object) -> None:
    """PARITY: viper.Set(key, value) + viper.WriteConfig() — read-modify-write the persisted config.toml.

    DEVIATION (config-write-no-viper-merge): viper.WriteConfig serializes the whole merged config (incl. bound
    flags); this read-modify-writes only the persisted keys + the updated one. The load-bearing persisted values
    (token/server_id/url) round-trip identically; only incidental flag persistence (e.g. keep_logs) differs."""
    values = load_config(data_dir).as_dict()
    values[key] = value
    write_config(data_dir, values)


def load_config(data_dir: str) -> Config:
    """Read <data_dir>/config.toml. Empty Config when absent; raises ConfigError on a parse error."""
    if not data_dir:
        return Config()
    path = config_path(data_dir)
    if not os.path.exists(path):
        return Config()
    try:
        with open(path, "rb") as f:
            return Config(tomli.load(f))
    except Exception as e:  # noqa: BLE001
        raise ConfigError(str(e)) from e


def init_config(data_dir: str, argv: list[str]) -> Config:
    """PARITY: initConfig (root.go:39-48) — exit 3 on a bad config UNLESS 'enroll' is in argv (so a not-yet-
    enrolled server can still run `enroll`)."""
    try:
        return load_config(data_dir)
    except ConfigError as e:
        if "enroll" in argv:
            return Config()
        print(f"error reading config file: {e}", file=sys.stderr)
        sys.exit(EXIT_INCORRECT_USAGE)
