# DEVIATIONS — Go → Python port

Intentional, justified divergences from the Go `edsGolang` behavior. Every entry is referenced
from code with `# DEVIATION: see DEVIATIONS.md#<anchor>`. Faithfulness is the default; this file
records the exceptions (language/runtime/package differences) for review. Quirks that are
*reproduced* (not deviated) live as `# PARITY:` markers in code, not here.

---

### python-310-64bit
**Decision:** target CPython **3.10 (64-bit)** (`py -3.10` on this machine), not the default
`python` (3.8.1, 32-bit). **Why:** several required packages (`snowflake-connector-python`,
`confluent-kafka`, `azure-eventhub`, `psycopg[binary]`, `pyodbc`) ship 64-bit wheels and target
≥3.8/3.9 with practical support on 64-bit; 32-bit 3.8 (EOL) has scarce/no wheels for them. No
behavioral impact — purely the runtime the faithful port runs on. 3.10 (not 3.11+) because that is
the newest 64-bit interpreter installed; consequence: no `tomllib`/`match`-everywhere reliance, so
TOML uses `tomli`/`tomli-w`.

### asyncio-concurrency-model
**Go:** goroutines + channels + `context.Context` + `sync.*`. **Python:** asyncio is the
concurrency model (the NATS client `nats-py` is async-only). `context.Context`→cancellation +
`asyncio.Event`; goroutines→tasks; channels→`asyncio.Queue`; `sync.WaitGroup`→`asyncio.gather`;
`sync.Once`→guard. **Ordering and decisions are preserved**, not the primitives (same convention the
C# port used). Synchronous one-shot CLI paths (enroll/version/download/import) stay synchronous.

### no-embedded-nats-server
**Go:** imports `nats-server/v2` (can embed a server). **Python:** has no embeddable NATS server;
the consumer connects to an external NATS as a client (`nats-py`). Tests use a real `nats` container
(testcontainers), mirroring how the C# port validated against a real server. To verify: confirm the
Go embedded server is only used by dev/e2e tooling, not the production consumer path (grounding TBD in M5).

### module-naming-keywords
`import.go`→`import_cmd.py` (`import` is a Python keyword). The user-facing CLI command stays
`import`. Other Go files map 1:1 to modules where practical.

### single-binary-pyinstaller
**Go:** `go build` → one static `eds` binary. **Python:** PyInstaller (one-file) → one `eds`
executable bundling the interpreter + deps, matching Go's single-binary distribution (the C# port
used single-file self-contained publish for the same reason). Native deps (librdkafka, ODBC) bundle
where supported; documented at M9.

## M1–M2 utility/infra deviations

### regex-re2-vs-python
Go uses RE2: `\d`/`\w` are ASCII-only and `$` is end-of-text (no multiline). Python `re` defaults to Unicode
`\w`/`\d` and `$` also matches before a trailing newline. Ported regexes use `re.ASCII` and `\Z` to match
Go (mask isURL, sql scalarValue, credentials company/session IDs). **Risk:** none — verified by golden tests
(e.g. fullwidth digits don't match `[0-9]`; a trailing `\n` blocks `(true|false)\Z`).

### rawjson-reconstruct
`DBChangeEvent.from_message` reconstructs the raw `before`/`after` (Go `json.RawMessage`) via
`gojson.marshal(parsed, sort_keys=False)` rather than capturing the exact original bytes. Byte-identical for
the Go-marshaled upstream (compact, Go-escaped); re-validate against the File/S3/Kafka goldens at M4/M7.

### cache-monotonic-clock
`util.cache.InMemoryCache` uses `time.monotonic()` for TTL (Go uses wall clock). More robust against
clock changes; behaviorally equivalent for durations. (The registry's seed asymmetry — TTL 0 dead-on-arrival
in the cache vs persistent in the tracker — is reproduced exactly under either clock.)

### tracker-deletekey-noop / tracker-prefix-literal / tracker-durability
sqlite3 replaces BuntDB: deleting a missing key is a no-op (Go's BuntDB Delete returns ErrNotFound);
`delete_keys_with_prefix` uses a literal ordinal range (`key >= p AND key < p⁺`) not a glob (identical for the
glob-free keys EDS uses); durability is `PRAGMA synchronous=NORMAL` vs BuntDB's every-second fsync (the tracker
holds rebuildable local state). `TEXT PRIMARY KEY` uses sqlite's default BINARY collation = BuntDB ordinal order.

### logger-format
go-common's logger uses fatih/color; its exact wire format is not in the repo. Like the C# port, the logger is
a clean equivalent — `[ts ]LEVEL [prefix] message [k=v …]` to stderr, no ANSI colors. Level
filtering/ordering, prefix chaining, fields, fatal→exit, and printf message formatting are faithful (Go
`%v`→Python `%s`).

### http-conn-error-detection
`HttpRetry` classifies a retryable connection error by message substring ("connection reset"/"refused") on the
Python/OS exception, not Go's runtime error strings. The loop is iterative (Go recurses) — behavior-neutral.

### fork-forwardinterrupt-no-signal-relay / fork-kill-direct-child
`util.process.fork` traps (does not relay) interrupts so the child runs its own graceful shutdown; cancellation
kills the direct child via `proc.kill()` (full process-tree kill via psutil/taskkill — revisited at M9 with the
frozen PyInstaller fork). Re-invocation: frozen → `[eds.exe, …]`; dev → `[python, -m, eds, …]`.

### gzip-bytes-not-identical
`util.compress.gzip_file` output is not byte-identical to Go's gzip (different compressor) but decompresses
identically; the `.gz` is never byte-compared (read back via gunzip).

## M3 deviations

### registry-sorttable-collision-order
`sortTable`'s by-table re-key is last-write-wins; Go map iteration order is random (nondeterministic winner),
Python dict order is deterministic. Behavior-neutral — table names are unique in practice.

### schema-nil-slice-coerced
`Schema.from_dict` coerces a missing/`null` `properties`/`required`/`primaryKeys` to `{}`/`[]`, so
`gojson.stringify(schema)` emits `{}`/`[]` where Go (nil map/slice, no omitempty) emits `null`. **Latent**: the
divergent bytes are only written to the tracker and re-normalized on read, so no registry API result differs;
the happy path (non-empty fields, which every real source schema has) is byte-identical. Matches the C# port.

### registry-decode-error-text
Go's `encoding/json` decode-error and transport-error MESSAGES can't be reproduced verbatim. The faithful parts
ARE reproduced: the contract prefixes (`error fetching schema:` / `error decoding schema…`) and the exception
TYPE (`ValueError`, so callers catching it still catch it). A `null` body decodes to an empty map / zero Schema
like Go; a valid-JSON wrong-type body raises the wrapped `error decoding schema` (not a raw `AttributeError`).

### go-json-leniency-not-reproduced
Go's `encoding/json` is case-insensitive on field names and `Decoder.Decode` ignores trailing bytes after the
value. Python `from_dict` matches exact (camelCase) tags and `json.loads` rejects trailing data. Safe — the
Shopmonkey backend emits canonical, single-value JSON; no path/test exercises these.

### metrics-memory-load-partial
`MemoryStat`/`LoadStat` are a subset of gopsutil (total/available/used/usedPercent/free; load zeros where
unavailable). In the SCRAPE TEXT only, prometheus-client appends `_total` to the counter name (HELP + sample;
Go scrapes `eds_total_events`) and renders integer bucket `le` labels as `"10.0"` vs Go's `"10"` — cosmetic
(a scraper parses them identically; the snapshot values via `collect()` and the gojson serialization are exact).
`get_system_stats` raises on a provider error (Go returns `(nil,err)`/`(ptr,err)`; the heartbeat caller discards
the snapshot either way).

### sysinfo-hostinfo-partial / sysinfo-go-version
`HostInfo` is partially populated (stdlib + psutil best-effort; kernelVersion/platformFamily/virtualization*
left empty); `go_version` is substituted with the Python version. Informational telemetry (osinfo). PARITY note:
gopsutil's `HostID` json tag is the lowercase `hostid` (inconsistent with its camelCase siblings) — reproduced.

### osinfo-struct-order
`SessionStart.os_info` must be a `__gojson__` struct (e.g. `SystemInfo`) to keep Go's declaration-order bytes;
a plain `dict` would be sorted by `gojson.marshal`. (Only `os_info=None`→`null` is tested at the api layer; the
real value comes from `get_system_info`, which is a struct.)

## M4 SQL-driver deviations

### gourl
`eds/util/gourl.py` ports the subset of Go `net/url` that EDS uses, taking **Go as ground truth** rather than
the reviewed-but-reduced C# `GoUrl.cs`. Where the two differ, Go wins (none is exercised by an existing golden,
so adopting Go cannot regress and only makes future connstrings faithful): the scheme is lower-cased
(`Postgres://` → `postgres`, matching the lowercase registry keys); a bad `%`-escape raises in host/path (Go)
instead of being kept raw (C#); query parsing drops `;`-bearing segments and bad-`%` pairs (Go). IPv6 host
literals are parsed minimally (validated + kept verbatim) — EDS never uses them; `validUserinfo` is not
enforced (harmless for EDS credentials). `Values.Encode` sorts keys; per-char escaping matches Go's
`QueryEscape` exactly (hand-rolled, not `urllib`, which doesn't sort and differs on byte-sets).

### postgres-remote-sslmode
`get_connection_string_from_url` emits no `sslmode` for remote hosts (byte-parity with Go). At connect time Go
lib/pq defaults to `require` while libpq/psycopg default to `prefer` — a TLS-default divergence (mirrors the C#
`postgres-connstring-params-subset`). The emitted string is identical; only the unspecified-remote connect
behavior differs. Not yet forced in the psycopg connect (revisit if remote TLS matters in deployment).

### sql-driver-help-deferred
The SQL drivers' `help()` returns `""` pending the help-rendering util (Go `util.GenerateHelpSection` — green
title + bold body with fatih/color ANSI). That util + the CLI that displays it land at M8; the ANSI codes are
grounded there alongside the logger's color decision. `help()` feeds only the CLI metadata commands, not the
data path. (The C# port did render it; revisit when porting the CLI.)

### sql-driver-quote-value-unreachable-branches
`quote_value`'s `datetime`, `bytes`/`_binary`, and non-string-`id` paths are not byte-faithful to Go for direct
calls (datetime tz/zero detection; latin-1 vs raw bytes; `str()` of a numeric id vs Go's string type-assert
panic). All are UNREACHABLE from the JSON streaming path (get_object yields only str/float/bool/None/dict/list;
ids are strings), so they don't affect emitted SQL; implemented for completeness only.

## Consumer (NATS streaming) deviations

### pull-fetch-loop
Go's live loop uses `jetstream.Consume(handler, PullExpiry(30s), PullMaxMessages(4096))` — a continuous,
callback-driven pull that streams messages as they arrive. nats-py has no `Consume` callback; its JetStream
surface is `pull_subscribe_bind` + an explicit `fetch(batch, timeout)`. Still a **pull** consumer (the
push-vs-pull decision is preserved), but `fetch()` BLOCKS up to its timeout trying to fill `batch` rather than
streaming, so a partial batch isn't returned until the timeout. `eds/consumer/consumer.py` uses
`_FETCH_TIMEOUT = 1.0` (not Go's 30s expiry) so a partial batch flows promptly; the BatchProcessor's
min/max-pending-latency still governs flush batching, so throughput batching is unchanged.

### consumer-max-request-batch-client-side
Go sets `MaxRequestBatch` on the JetStream consumer config (server caps the pull batch). nats-py's
`ConsumerConfig` has no such field, so the pull batch is bounded client-side via `fetch(batch=max_pending_buffer)`
instead. Same effective cap (4096), enforced on the client rather than the server.

### consumer-opt-start-time-datetime
nats-py's `ConsumerConfig.opt_start_time` is a `datetime`, not an RFC3339 string; the by-start-time deliver
policy passes the `datetime` directly (the JS API still serializes RFC3339 on the wire).

### nats-reconnect-defaults
go-common's `cnats.NewNats` reconnect options aren't vendored (not in the local module cache), so nats-py's
library reconnect defaults are used (allow_reconnect, max_reconnect_attempts, reconnect_time_wait, ping_interval).
The C# port also used library defaults. Revisit if go-common's values are recovered.

### consumer-self-stops-on-fatal
Go's `handleError` only naks + pushes the error onto the `subError` channel; the consumer's connection and
heartbeat stay alive, and the OWNER (fork.go) selects on `Error()` and calls `Stop()`. The asyncio consumer
instead self-stops on a fatal (`_set_fatal` schedules `stop()`) AND sets an awaitable `fatal()` event so a future
runner can react. Decision preserved (a fatal naks the residual batch and surfaces the error on `error()`); the
stop is self-driven rather than owner-driven. Revisit when the runner/main (cmd/server.go) is ported.

### consumer-bufferer-no-busy-spin
Go's bufferer is a `select` with a `default` (non-blocking) arm that busy-spins when a partial batch is waiting
for min/max latency. The asyncio Bufferer uses `asyncio.wait_for(queue.get(), timeout=empty_buffer_pause)` so the
event loop is not blocked while waiting. Same idle/flush decisions; no CPU busy-spin. The hard-cancel (nak
residual) path is reachable via `stop(graceful=False)` (sets the cancel event); the graceful path uses the None
sentinel (final flush+ack).

<!-- Add further deviations below as they arise (carry over the C# port's where they recur:
     file-uri-windows-drive-letter, download-zip-extract). -->
