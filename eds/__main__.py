"""EDS consumer entry point (= edsGolang/main.go).

Dispatches to the CLI in eds.cmd.root (= cmd/root.go). Exposed as `python -m eds` and the `eds` console script.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from eds.cmd.root import main as _main
    from eds.drivers import register_all

    # FIX: register drivers at the entry point for EVERY command (matches C# Program.cs:24 / Go's package init()).
    # Previously register_all() was called only by `import` and the forked consumer — NOT the `server` parent
    # process — so `server` validated --url against an EMPTY driver registry and rejected every URL with
    # "invalid driver URL". Registering here is idempotent and cheap (driver DB/cloud libs stay lazy).
    register_all()
    return _main(argv)


if __name__ == "__main__":
    sys.exit(main())
