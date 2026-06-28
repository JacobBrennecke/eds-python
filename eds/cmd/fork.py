"""PARITY: cmd/fork.go forkCmd — Layer 3, the data-consumer child (driver + Consumer + control loop).

Concurrency model (DEVIATION fork-asyncio-loop): the Consumer is asyncio, so the worker runs an event loop; the
loopback control server (threading) and OS shutdown bridge push commands onto an asyncio.Queue via
call_soon_threadsafe. Exit codes follow the contract (1 = error, 4 = restart, 5 = nats-disconnect, 0 = clean).
The driver is built on a non-cancellable context (Go fork.go:118) so it can flush during shutdown.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import threading
from datetime import datetime
from typing import Any

from eds.cmd.exit_codes import (
    EXIT_ERROR,
    EXIT_INCORRECT_USAGE,
    EXIT_NATS_DISCONNECTED,
    EXIT_RESTART,
    EXIT_SUCCESS,
)
from eds.cmd.import_client import parse_rfc3339
from eds.cmd.loopback import LoopbackServer
from eds.consumer.config import ConsumerConfig
from eds.consumer.consumer import Consumer
from eds.driver import new_driver
from eds.drivers import register_all
from eds.metrics import EdsMetrics
from eds.registry import new_api_registry
from eds.tracker import new_tracker
from eds.util.file import is_localhost
from eds.util.logger import LogFileSink, new_log_file_sink
from eds.util.shutdown import ShutdownSignal


def _load_table_export_info(tracker: Any) -> dict[str, datetime | None] | None:
    """PARITY: loadTableExportInfo (root.go:230) + fork.go:106-115 — the tracker "table-export" key holds a JSON
    ARRAY of TableExportInfo ([{"Table":..,"Timestamp":..}], capitalized field names, no json tags) → built into
    {table: cutoff}. A malformed value raises (→ exit 3, matching Go)."""
    found, val = tracker.get_key("table-export")
    if not found:
        return None
    data = json.loads(val)
    out: dict[str, datetime | None] = {}
    for item in data:
        table = item.get("Table", "")
        ts = item.get("Timestamp")
        if not ts:
            out[table] = None
            continue
        try:
            out[table] = parse_rfc3339(str(ts))  # robust to Go's trimmed-fraction RFC3339Nano
        except ValueError:
            out[table] = None
    return out


def _build_config(args: argparse.Namespace, driver: Any, registry: Any, validator: Any,
                  table_timestamps: dict[str, datetime | None] | None) -> ConsumerConfig:
    return ConsumerConfig(
        url=args.nats_url,
        credentials=args.creds,
        company_ids=args.company_ids or [],
        suffix=args.consumer_suffix,
        max_ack_pending=args.max_ack_pending,
        max_pending_buffer=args.max_pending_buffer,
        driver=driver,
        registry=registry,
        schema_validator=validator,
        export_table_timestamps=table_timestamps,
        deliver_all=args.restart,
        min_pending_latency=args.min_pending_latency,
        max_pending_latency=args.max_pending_latency,
    )


def run_fork(args: argparse.Namespace) -> int:
    return asyncio.run(_run_fork_async(args))


async def _run_fork_async(args: argparse.Namespace) -> int:  # noqa: C901 — faithful to fork.go's single Run
    from eds.cmd import root as _root

    base_logger = _root.new_logger(args)
    # PARITY: fork.go:61 — tee all log records to a rotating per-session log file BEFORE the [fork] prefix.
    sink: LogFileSink | None = None
    if args.logs_dir:
        try:
            sink = new_log_file_sink(args.logs_dir)
        except OSError as e:
            base_logger.error("error creating log file sink: %s", e)
            return EXIT_INCORRECT_USAGE
        base_logger.trace("using log file sink: %s", args.logs_dir)
        base_logger = base_logger.with_sink(sink)
    logger = base_logger.with_prefix("[fork]")
    register_all()
    nats_url = args.nats_url
    if not args.creds and not is_localhost(nats_url):
        logger.error('error: required flag "creds" not set')
        return EXIT_INCORRECT_USAGE
    data_dir = os.path.abspath(os.path.normpath(args.data_dir))

    validator = None
    if args.schema_validator:
        # DEVIATION (fork-schema-validator-deferred): the schema-validator directory loader is not yet ported;
        # the fork runs without schema validation when one is requested.
        logger.warn("schema-validator directory loading is not yet ported; running without validation")

    try:
        tracker = new_tracker(data_dir, logger)
    except Exception as e:  # noqa: BLE001
        logger.error("error creating tracker: %s", e)
        return EXIT_INCORRECT_USAGE
    try:
        registry = new_api_registry(logger, args.api_url, _root.VERSION, tracker)
        table_timestamps = _load_table_export_info(tracker)
        # PARITY: the driver gets a NON-cancellable context so it can flush during shutdown (fork.go:118).
        driver = new_driver(None, logger, args.url, registry, tracker, data_dir)
    except Exception as e:  # noqa: BLE001
        logger.error("error during setup: %s", e)
        tracker.close()
        return EXIT_INCORRECT_USAGE

    metrics = EdsMetrics()
    loop = asyncio.get_running_loop()
    control: asyncio.Queue = asyncio.Queue()

    def _ctl(cmd: str):
        def handler() -> tuple[int, str]:
            loop.call_soon_threadsafe(control.put_nowait, cmd)
            return 200, ""

        return handler

    def _logfile() -> tuple[int, str]:
        # PARITY: fork.go:155 — rotate the sink and return the just-closed log path for the parent to upload.
        if sink is None:
            return 200, ""
        try:
            return 200, sink.rotate()
        except OSError as e:
            logger.error("error rotating log file: %s", e)
            return 500, ""

    routes = {
        "/": lambda: (200, "OK"),
        "/metrics": lambda: (200, metrics.scrape()),
        "/control/pause": _ctl("pause"),
        "/control/unpause": _ctl("unpause"),
        "/control/restart": _ctl("restart"),
        "/control/shutdown": _ctl("ctl_shutdown"),
        "/control/logfile": _logfile,
    }
    try:
        server = LoopbackServer(args.port, routes)
        server.start()
    except OSError as e:  # PARITY: runHealthCheckServerFork bind failure → Fatal (exit 1)
        logger.error("error starting health server: %s", e)
        driver.stop()
        tracker.close()
        return EXIT_ERROR

    shutdown_evt = asyncio.Event()
    shutdown_sig = ShutdownSignal()

    def _wait_shutdown() -> None:
        shutdown_sig.wait()
        loop.call_soon_threadsafe(shutdown_evt.set)

    threading.Thread(target=_wait_shutdown, daemon=True, name="fork-shutdown").start()

    exit_code = EXIT_SUCCESS
    consumer: Consumer | None = None
    paused = False
    completed = False
    control_task: asyncio.Future = asyncio.ensure_future(control.get())
    logger.info("server is running version: %s", _root.VERSION)
    try:
        while not completed:
            if not paused and consumer is None:
                consumer = Consumer(_build_config(args, driver, registry, validator, table_timestamps), logger, metrics)
                try:
                    await consumer.create()
                    await consumer.start()
                except Exception as e:  # noqa: BLE001 — PARITY: consumer create failure → exit 1
                    logger.error("error creating consumer: %s", e)
                    with contextlib.suppress(Exception):
                        await consumer.stop()  # clean up a half-open NATS connection (Go NewConsumer is atomic)
                    exit_code = EXIT_ERROR
                    consumer = None
                    break

            waiters: list[asyncio.Future] = [control_task]
            fatal_t = disc_t = None
            if consumer is not None:
                fatal_t = asyncio.ensure_future(consumer.fatal().wait())
                disc_t = asyncio.ensure_future(consumer.disconnected().wait())
                waiters += [fatal_t, disc_t]
            shutdown_t = asyncio.ensure_future(shutdown_evt.wait())
            waiters.append(shutdown_t)

            done, _pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            for t in (fatal_t, disc_t, shutdown_t):
                if t is not None and not t.done():
                    t.cancel()

            if control_task in done:
                cmd = control_task.result()
                control_task = asyncio.ensure_future(control.get())
                if cmd == "restart":
                    exit_code = EXIT_RESTART
                    completed = True
                    if consumer is not None:
                        await consumer.stop()
                        consumer = None
                elif cmd == "ctl_shutdown":
                    completed = True
                    if consumer is not None:
                        await consumer.stop()
                        consumer = None
                elif cmd == "pause" and consumer is not None:
                    await consumer.pause()
                    paused = True
                elif cmd == "unpause" and consumer is not None:
                    await consumer.unpause()
                    paused = False
            elif fatal_t is not None and fatal_t in done:
                exit_code = EXIT_ERROR
                completed = True
            elif disc_t is not None and disc_t in done:
                logger.warn("nats server disconnected")
                exit_code = EXIT_NATS_DISCONNECTED
                completed = True
            elif shutdown_t in done:
                completed = True  # OS SIGINT/SIGTERM → clean exit 0
        # PARITY: log the clean-exit marker BEFORE the sink closes, so it lands in the uploaded log (fork.go:281).
        logger.info("👋 Bye")
    finally:
        control_task.cancel()
        if consumer is not None:
            await consumer.stop()
        driver.stop()
        tracker.close()
        server.stop()
        if sink is not None:
            sink.close()  # PARITY: defer sink.Close() (fork.go:67)
    return exit_code
