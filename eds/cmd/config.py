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


def config_path(data_dir: str) -> str:
    return os.path.join(data_dir, "config.toml")


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
