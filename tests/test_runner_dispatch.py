"""PARITY: the CLI dispatcher (eds.cmd.root.main) — command routing + exit codes."""

from __future__ import annotations

import pytest

from eds.cmd import exit_codes
from eds.cmd.root import main


def test_version(capsys) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() != ""  # prints the version


def test_publickey(capsys) -> None:
    assert main(["publickey"]) == 0
    assert "PGP PUBLIC KEY" in capsys.readouterr().out


def test_no_command_prints_help(capsys) -> None:
    assert main([]) == 0
    assert "usage: eds" in capsys.readouterr().out


def test_unknown_command_exits_3() -> None:
    with pytest.raises(SystemExit) as ei:
        main(["bogus"])
    assert ei.value.code == exit_codes.EXIT_INCORRECT_USAGE


def test_not_yet_implemented_commands(capsys) -> None:
    for cmd in ("enroll", "download"):
        with pytest.raises(SystemExit) as ei:
            main([cmd])  # argparse: not a registered subcommand → exit 3
        assert ei.value.code == exit_codes.EXIT_INCORRECT_USAGE


def test_fork_missing_creds_exits_3(tmp_path) -> None:
    # non-localhost NATS without --creds → required-flag error → exit 3 (before any consumer/driver work)
    rc = main(["fork", "--server", "nats://example.com:4222", "--data-dir", str(tmp_path)])
    assert rc == exit_codes.EXIT_INCORRECT_USAGE


def test_companyids_csv_split() -> None:
    from eds.cmd.root import build_parser

    # PARITY: pflag StringSlice splits each value on commas and accumulates across repeats
    args = build_parser().parse_args(["fork", "--companyIds", "a,b", "--companyIds", "c"])
    assert args.company_ids == ["a", "b", "c"]


def test_no_flag_abbreviation() -> None:
    from eds.cmd.root import build_parser

    # cobra requires exact flags — argparse prefix abbreviation must be disabled
    with pytest.raises(SystemExit) as ei:
        build_parser().parse_args(["server", "--verb"])  # not an accepted abbreviation of --verbose
    assert ei.value.code == exit_codes.EXIT_INCORRECT_USAGE


def test_load_table_export_info_array_shape() -> None:
    from eds.cmd.fork import _load_table_export_info

    class _Tracker:
        def __init__(self, val):
            self._val = val

        def get_key(self, key):
            return (True, self._val) if self._val is not None else (False, "")

    # Go format: a JSON ARRAY of {Table, Timestamp} (not a {table: ts} dict)
    val = '[{"Table":"user","Timestamp":"2026-01-01T00:00:00Z"},{"Table":"order","Timestamp":"0001-01-01T00:00:00Z"}]'
    out = _load_table_export_info(_Tracker(val))
    assert set(out) == {"user", "order"}
    assert out["user"].year == 2026
    assert _load_table_export_info(_Tracker(None)) is None  # not found → None
