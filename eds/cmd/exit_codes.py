"""PARITY: the EDS exit-code contract (cmd/fork.go:30-33 + internal/util/errors.go RecoverPanic).

The SAME code means different things at the two parent layers (see cmd/server.py): Layer-2 has the full
decision tree (don't-upload-on-3, no-failures++-on-4, 5s-retry-on-5); Layer-1 only knows 0|1 → stop, else
failures++ + linear backoff.
"""

from __future__ import annotations

EXIT_SUCCESS = 0  # clean shutdown (SIGTERM / /control/shutdown / clean ctx done)
EXIT_ERROR = 1  # generic error (consumer create / Error() / validator / cobra flag errors)
EXIT_PANIC = 2  # RecoverPanic
EXIT_INCORRECT_USAGE = 3  # exitCodeIncorrectUsage — bad flags / setup failure / driver test fail
EXIT_RESTART = 4  # exitCodeRestart — intentional restart (SIGHUP / /control/restart)
EXIT_NATS_DISCONNECTED = 5  # exitCodeNatsDisconnected — NATS dropped

MAX_FAILURES = 5  # server.go:40 — bounds both Layer-1 and Layer-2 retry loops
