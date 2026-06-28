"""PARITY: cmd/root.go + main.go — the CLI dispatcher, persistent flags, and per-command bootstrap.

DEVIATION (cli-argparse): Cobra's auto-registering commands + viper are replaced by an explicit argparse tree
(the C# port likewise hand-rolled a dispatcher — the commands/behaviour are what matter for Stage-1 parity, not
the framework). Persistent flags are shared via a parent parser; bad/missing-required flags exit 3 (mustFlag*),
not argparse's default 2. Durations accept Go strings ("2s","24h"). `version`/`publickey` are fully wired here;
`server`/`fork` dispatch to the runner layers; `enroll`/`import`/`download` are declared but not yet ported.
"""

from __future__ import annotations

import argparse
import os
import sys

from eds.cmd.args import get_os_int, parse_duration
from eds.cmd.exit_codes import EXIT_INCORRECT_USAGE, EXIT_PANIC
from eds.util.file import is_dir_writable
from eds.util.logger import ConsoleLogger, LogLevel, new_console_logger

_DEFAULT_API_URL = "https://api.shopmonkey.cloud"
_DEFAULT_NATS_URL = "nats://connect.nats.shopmonkey.pub"
_DEFAULT_MAX_ACK_PENDING = 25_000
_DEFAULT_MAX_PENDING_BUFFER = 4096

# Package globals injected at startup (PARITY: main.go pushes Version + ShopmonkeyPublicPGPKey into cmd).
VERSION = os.environ.get("GIT_SHA") or "dev"
PUBLIC_PGP_KEY = ""


def _load_public_key() -> str:
    try:
        from importlib.resources import files

        return files("eds").joinpath("shopmonkey.asc").read_text(encoding="utf-8")  # verbatim (PARITY: //go:embed)
    except Exception:  # noqa: BLE001
        return ""


class _Parser(argparse.ArgumentParser):
    """PARITY: mustFlag* — a bad/missing-required flag exits 3 (not argparse's default 2)."""

    def error(self, message: str) -> None:  # type: ignore[override]
        self.print_usage(sys.stderr)
        print(f"error: {message}", file=sys.stderr)
        sys.exit(EXIT_INCORRECT_USAGE)


class _CsvAppend(argparse.Action):
    """PARITY: pflag StringSlice — split each value on commas and accumulate across repeats."""

    def __call__(self, parser, namespace, values, option_string=None):
        current = list(getattr(namespace, self.dest, None) or [])
        current.extend(str(values).split(","))
        setattr(namespace, self.dest, current)


def _persistent_flags() -> _Parser:
    """PARITY: root.go persistent flags (apply to all subcommands)."""
    base = _Parser(add_help=False, allow_abbrev=False)
    base.add_argument("-v", "--verbose", action="store_true", help="turn on verbose logging")
    base.add_argument("-s", "--silent", action="store_true", help="turn off all logging")
    base.add_argument("-t", "--timestamp", action="store_true", help="include timestamps in logs")
    base.add_argument("--log-file-sink", default="", help=argparse.SUPPRESS)
    base.add_argument("--log-label", default="", help=argparse.SUPPRESS)
    base.add_argument("--schema-validator", default="", help="schema validator directory")
    base.add_argument("-d", "--data-dir", default=os.path.join(os.getcwd(), "data"), help="data directory")
    return base


def _add_consumer_tuning(p: argparse.ArgumentParser) -> None:
    p.add_argument("--consumer-suffix", default="", help=argparse.SUPPRESS)
    p.add_argument("--maxAckPending", dest="max_ack_pending", type=int, default=_DEFAULT_MAX_ACK_PENDING,
                   help=argparse.SUPPRESS)
    p.add_argument("--maxPendingBuffer", dest="max_pending_buffer", type=int, default=_DEFAULT_MAX_PENDING_BUFFER,
                   help=argparse.SUPPRESS)
    p.add_argument("--minPendingLatency", dest="min_pending_latency", type=parse_duration, default=2.0,
                   help=argparse.SUPPRESS)
    p.add_argument("--maxPendingLatency", dest="max_pending_latency", type=parse_duration, default=30.0,
                   help=argparse.SUPPRESS)
    p.add_argument("--restart", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--companyIds", dest="company_ids", action=_CsvAppend, default=None, help=argparse.SUPPRESS)


def build_parser() -> _Parser:
    base = _persistent_flags()
    parser = _Parser(
        prog="eds", description="Shopmonkey Enterprise Data Streaming (EDS) consumer", allow_abbrev=False
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("version", parents=[base], help="print the current version", allow_abbrev=False)
    sub.add_parser("publickey", parents=[base], help="print the Shopmonkey Public PGP Key", allow_abbrev=False)

    srv = sub.add_parser("server", parents=[base], help="run the EDS server", allow_abbrev=False)
    srv.add_argument("--url", default="", help="the connection string")
    srv.add_argument("--api-key", default=os.environ.get("SM_APIKEY", ""), help="the API key")
    srv.add_argument("--port", type=int, default=get_os_int("PORT", 8080), help="the health/metrics port")
    srv.add_argument("--eds-id", default="", help="the EDS server id")
    srv.add_argument("--keep-logs", action="store_true", help="keep logs after exit")
    srv.add_argument("--health-port", type=int, default=0, help=argparse.SUPPRESS)  # deprecated
    # PARITY: default None is the "--api-url not changed" sentinel → derive the api url from the JWT.
    srv.add_argument("--api-url", default=None, help=argparse.SUPPRESS)
    srv.add_argument("--server", dest="nats_url", default=_DEFAULT_NATS_URL, help=argparse.SUPPRESS)
    srv.add_argument("--renew-interval", type=parse_duration, default=parse_duration("24h"), help=argparse.SUPPRESS)
    srv.add_argument("--wrapper", action="store_true", help=argparse.SUPPRESS)
    srv.add_argument("--parent", type=int, default=-1, help=argparse.SUPPRESS)
    srv.add_argument("--no-restart", action="store_true", help=argparse.SUPPRESS)
    _add_consumer_tuning(srv)

    fork = sub.add_parser("fork", parents=[base], help=argparse.SUPPRESS, allow_abbrev=False)  # hidden
    fork.add_argument("--logs-dir", default="", help=argparse.SUPPRESS)
    fork.add_argument("--creds", default="", help=argparse.SUPPRESS)
    fork.add_argument("--url", default="", help=argparse.SUPPRESS)
    fork.add_argument("--api-url", default=_DEFAULT_API_URL, help=argparse.SUPPRESS)
    fork.add_argument("--server", dest="nats_url", default=_DEFAULT_NATS_URL, help=argparse.SUPPRESS)
    fork.add_argument("--port", type=int, default=get_os_int("PORT", 8080), help=argparse.SUPPRESS)
    _add_consumer_tuning(fork)

    return parser


def new_logger(args: argparse.Namespace) -> ConsoleLogger:
    """PARITY: newLogger (root.go:160-185) — level from --silent/--verbose, optional --log-label prefix."""
    if getattr(args, "silent", False):
        level = LogLevel.ERROR
    elif getattr(args, "verbose", False):
        level = LogLevel.TRACE
    else:
        level = LogLevel.INFO
    logger: ConsoleLogger = new_console_logger(level, timestamps=getattr(args, "timestamp", False))
    label = getattr(args, "log_label", "")
    if label:
        logger = logger.with_prefix(f"[{label}]")
    return logger


def get_data_dir(args: argparse.Namespace, logger: ConsoleLogger) -> str:
    """PARITY: getDataDir (root.go:208-228) — abs+clean, mkdir 0700, writability check."""
    data_dir = os.path.abspath(os.path.normpath(args.data_dir))
    if not os.path.exists(data_dir):
        ok, err = is_dir_writable(os.path.dirname(data_dir))
        if not ok:
            logger.fatal("data directory parent is not writable: %s", err)
        try:
            os.makedirs(data_dir, mode=0o700, exist_ok=True)
        except OSError as e:
            logger.fatal("failed to create data directory: %s", e)
        logger.debug("making data directory: %s", data_dir)
    else:
        ok, err = is_dir_writable(data_dir)
        if not ok:
            logger.fatal("data directory is not writable: %s", err)
    logger.debug("using data directory: %s", data_dir)
    return data_dir


def main(argv: list[str] | None = None) -> int:
    """PARITY: cmd.Execute — dispatch the subcommand; panics → exit 2 (RecoverPanic)."""
    global PUBLIC_PGP_KEY
    PUBLIC_PGP_KEY = _load_public_key()
    argv = sys.argv[1:] if argv is None else argv
    parser = build_parser()
    args = parser.parse_args(argv)
    command = getattr(args, "command", None)
    if command is None:
        parser.print_help()
        return 0

    try:
        if command == "version":
            print(VERSION)
            return 0
        if command == "publickey":
            print(PUBLIC_PGP_KEY)
            return 0
        if command == "server":
            from eds.cmd.server import run_server

            return run_server(args, argv)
        if command == "fork":
            from eds.cmd.fork import run_fork

            return run_fork(args)
        if command in ("enroll", "import", "download"):
            print(f"error: command '{command}' is not yet implemented in the Python port", file=sys.stderr)
            return EXIT_INCORRECT_USAGE
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 — PARITY: RecoverPanic → exit 2
        print(f"panic: {e}", file=sys.stderr)
        return EXIT_PANIC
    return 0
