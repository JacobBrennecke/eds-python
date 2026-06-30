"""FEATURE(import-config-fallback): `eds import` falls back to config.toml for --url ("url") and --api-key
("token") when not supplied on the CLI/env, mirroring the SERVER command's resolution (server.py:199-200). Only
error when absent from flag/env AND config. The config read is SILENT — the resolved url/api-key (secrets) are
NEVER logged. Cross-port oracle: migration/features/import-config-fallback.md.

Intentional divergence from Go (Go's `import` requires both flags). Mark new sites FEATURE(import-config-fallback).
"""

from __future__ import annotations

import os

import eds.cmd.import_cmd as ic
from eds.cmd.config import write_config
from eds.cmd.exit_codes import EXIT_INCORRECT_USAGE, EXIT_SUCCESS
from eds.cmd.root import build_parser


class _FakeRegistry:
    def close(self) -> None: ...


def _capture_resolution(monkeypatch) -> dict:
    """Test seam: replace _do_import with a probe that records the RESOLVED driver_url + api_key (the values the
    resolution handed downstream) and short-circuits with success — so the test asserts purely on resolution."""
    captured: dict = {}

    def fake_do_import(args, logger, tracker, registry, data_dir, cancel, api_url, api_key, driver_url, *a, **k):
        captured["driver_url"] = driver_url
        captured["api_key"] = api_key
        return EXIT_SUCCESS

    monkeypatch.setattr(ic, "_do_import", fake_do_import)
    monkeypatch.setattr(ic, "new_api_registry", lambda *a, **k: _FakeRegistry())
    return captured


def _import_args(data_dir: str, *, url: str | None, api_key: str | None):
    argv = ["import", "--api-url", "http://localhost", "--no-confirm", "--data-dir", data_dir]
    if url is not None:
        argv += ["--url", url]
    if api_key is not None:
        argv += ["--api-key", api_key]
    return build_parser().parse_args(argv)


# 1. flag supplied → flag value used; config NOT consulted (flag wins) — for url AND api-key.
def test_flag_wins_over_config(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SM_APIKEY", raising=False)
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    write_config(data_dir, {"url": "file:///from-config", "token": "CONFIG-TOKEN"})

    captured = _capture_resolution(monkeypatch)
    rc = ic.run_import_command(_import_args(data_dir, url="file:///from-flag", api_key="FLAG-TOKEN"))

    assert rc == EXIT_SUCCESS
    assert captured["driver_url"] == "file:///from-flag"  # flag wins for url
    assert captured["api_key"] == "FLAG-TOKEN"            # flag wins for api-key


# 2. flag/env absent + config.toml has "url"/"token" → those values used, NO error.
def test_config_fallback_used_when_flag_absent(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SM_APIKEY", raising=False)
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    write_config(data_dir, {"url": "file:///from-config", "token": "CONFIG-TOKEN"})

    captured = _capture_resolution(monkeypatch)
    rc = ic.run_import_command(_import_args(data_dir, url=None, api_key=None))

    assert rc == EXIT_SUCCESS
    assert captured["driver_url"] == "file:///from-config"  # config "url" used
    assert captured["api_key"] == "CONFIG-TOKEN"            # config "token" used


# 3. flag/env absent + config.toml absent/empty → the existing error fires (exit code unchanged).
def test_error_when_both_absent(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.delenv("SM_APIKEY", raising=False)
    data_dir = str(tmp_path / "data")  # no config.toml written

    rc = ic.run_import_command(_import_args(data_dir, url=None, api_key=None))

    assert rc == EXIT_INCORRECT_USAGE  # exit code unchanged
    assert 'required flag "url" not set' in capsys.readouterr().err


# 4. the resolved url/api-key do NOT appear in any captured log output.
def test_resolved_secrets_never_logged(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.delenv("SM_APIKEY", raising=False)
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    secret_url = "file:///SECRET-URL-zzz"
    secret_token = "SECRET-TOKEN-zzz"
    write_config(data_dir, {"url": secret_url, "token": secret_token})

    captured = _capture_resolution(monkeypatch)
    rc = ic.run_import_command(_import_args(data_dir, url=None, api_key=None))
    assert rc == EXIT_SUCCESS
    # sanity: the secrets really were the resolved values (the fallback fired)
    assert captured["driver_url"] == secret_url and captured["api_key"] == secret_token

    out = capsys.readouterr()
    assert secret_url not in out.err and secret_url not in out.out
    assert secret_token not in out.err and secret_token not in out.out
