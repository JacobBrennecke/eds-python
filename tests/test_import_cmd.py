"""PARITY: the import command dispatch + flag surface + error-path exit codes."""

from __future__ import annotations

from eds.cmd import exit_codes
from eds.cmd.root import build_parser, main


def test_import_flags_parse() -> None:
    args = build_parser().parse_args([
        "import", "--url", "postgres://x", "--api-key", "k", "--companyIds", "a,b",
        "--only", "users", "--schema-only", "--parallel", "8",
    ])
    assert args.command == "import"
    assert args.url == "postgres://x" and args.company_ids == ["a", "b"]
    assert args.only == ["users"] and args.schema_only is True and args.parallel == 8
    assert args.api_url is None  # default sentinel → derive from JWT


def test_import_requires_url(tmp_path) -> None:
    # no --url (and no SM_APIKEY) → required-flag error → exit 3, before any network/DB work
    rc = main(["import", "--api-key", "k", "--api-url", "http://localhost", "--data-dir", str(tmp_path)])
    assert rc == exit_codes.EXIT_INCORRECT_USAGE


def test_import_bad_api_key_jwt(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SM_APIKEY", raising=False)
    # --api-url omitted → derive from the JWT → a non-JWT api key raises → exit 1
    rc = main(["import", "--url", "postgres://x", "--api-key", "notajwt", "--data-dir", str(tmp_path)])
    assert rc == exit_codes.EXIT_ERROR
