"""PyInstaller entry point for the single-binary EDS build (= edsGolang/main.go).

PyInstaller needs a concrete script (not a module); this just calls eds.__main__.main(). The frozen exe IS the
`eds` binary, so the runner's self-exec (process._self_invocation) re-invokes `[sys.executable, <subcommand>...]`.
"""

import sys

from eds.__main__ import main

if __name__ == "__main__":
    sys.exit(main())
