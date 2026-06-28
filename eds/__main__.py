"""EDS consumer entry point (= edsGolang/main.go).

Dispatches to the CLI in eds.cmd.root (= cmd/root.go). Exposed as `python -m eds` and the `eds` console script.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from eds.cmd.root import main as _main

    return _main(argv)


if __name__ == "__main__":
    sys.exit(main())
