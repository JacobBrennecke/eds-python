"""EDS consumer entry point (= edsGolang/main.go).

The full argparse dispatcher (= cmd/root.go + subcommands) lands in M9. This M0 stub
establishes the `python -m eds` / `eds` entry point and the command surface.
"""

from __future__ import annotations

import sys

# PARITY: cmd/root.go subcommands (the user-facing CLI surface).
COMMANDS = ("version", "publickey", "enroll", "server", "fork", "import", "download")


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    # M0 stub — the real dispatcher (cmd/root.py) arrives in M9.
    print(f"usage: eds <command> [flags]  ({', '.join(COMMANDS)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
