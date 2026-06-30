# DEVIATIONS — Go → Python port

Intentional, justified divergences from the Go `edsGolang` behavior. Every entry is referenced
from code with `# DEVIATION: see DEVIATIONS.md#<anchor>`. Faithfulness is the default; this file
records the exceptions (language/runtime/package differences) for review. Quirks that are
*reproduced* (not deviated) live as `# PARITY:` markers in code, not here.

**Three markers (read before auditing):** `# PARITY:` = reproduces Go; `# DEVIATION:` = a justified port
difference *while implementing a Go behavior* (still references Go). `# FEATURE(<name>):` = **net-new behavior
with NO Go counterpart** — a deliberate, intentional divergence. `FEATURE`-marked code is OUT of scope for Go-parity
audits (do not hunt for a Go equivalent); it is IN scope for its own contract + the cross-port twin. The first such
feature is **audit-mode** — see `migration/features/audit-mode.md` (the cross-port oracle, since Go is not).

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
used single-file self-contained publish for the same reason). **M10 realization** (`eds.spec` +
`packaging/build.py`): `collect_submodules("eds")` bundles the dynamically-registered drivers;
`collect_data_files("jsonschema_specifications")` ships the draft meta-schemas (else the schema
validator's `check_schema` can't find draft-07); `shopmonkey.asc` ships via `datas` (read with
`importlib.resources`); the lazily-imported DB/runtime libs are listed as hidden imports. Version is
baked into a gitignored `eds/_version.py` at build time (`root._resolve_version`: `$GIT_SHA` →
`eds._version` → `"dev"`); the artifact is renamed `eds_<Platform>_<arch>[.exe]` to match
`build_release_urls` / the `download` command. The frozen exe re-execs itself for the fork/download/
enroll subcommands (`process._self_invocation` → `[sys.executable]` when `sys.frozen`). **Gaps:**
`snowflake-connector-python` is not a hard dep, so the snowflake driver bundles only when that lib is
installed in the build env (it is optional + lazily imported); `pymssql`/`psycopg` native deps bundle
via the contrib hooks where present, and `nats-py[nkeys]` (→ `nkeys` + `pynacl`) is a hard dep + an
explicit hidden import so the production NATS credential-signing path works in the frozen binary
(nats-py imports `nkeys` lazily and has no PyInstaller hook). The self-upgrade `apply()` works
mechanically on the packaged binary, but the end-to-end self-upgrade closure stays gated because the
published GitHub release artifacts are **Go** binaries — no Python release artifacts exist (see
`upgrade-apply-only-for-frozen-binary`).

**Accepted one-file tradeoffs:** a one-file build re-extracts (~18 MB) to a fresh `%TEMP%/_MEIxxxxxx`
on *every* process launch, and the runner is a self-re-exec supervisor (Layer-1 wrapper → Layer-2
control plane → per-session consumer forks, plus re-forks on 24h credential renewal / restart /
error backoff). So vs the Go native binary there is (1) a multi-second extract cost per re-exec and
(2) a temp-dir leak when the supervisor *hard-kills* a child (Windows `TerminateProcess` bypasses the
bootloader's atexit cleanup, orphaning that child's `_MEI` dir). A fixed `runtime_tmpdir` does NOT
fix either; the real mitigation for high-restart Windows deployments is a one-DIR build (`COLLECT`
instead of one-file `EXE`) — kept one-file here to match Go's single-binary distribution, documented
as the tradeoff. **Snowflake:** `register_all` registers the snowflake driver unconditionally, so
without `snowflake-connector-python` bundled a `snowflake://` URL fails at connect with a bare
`ModuleNotFoundError` rather than a clean "driver unavailable" (accepted — the lib is optional/lazy).

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

## Upgrade module deviations

### upgrade-pgp-pgpy
Go verifies the release with ProtonMail/gopenpgp v3 (`crypto.Auto` detached verify). The Python port uses **pgpy**
(pure-Python, added to deps; pulls `cryptography`) — `PGPKey.from_blob` (armored key) + `PGPSignature.from_blob`
(armored-or-binary sig) + `key.verify` over the whole archive. `shopmonkey.asc` is a v4 Ed25519/EdDSA (algo 22)
key, which pgpy supports. Chosen over `python-gnupg` (needs a shipped `gpg` binary, breaks the M10 one-file build)
and `cryptography`-only (no OpenPGP packet/armor parsing). pgpy emits `CryptographyDeprecationWarning`s, so the
verify is wrapped in `warnings.catch_warnings()` (the test suite has `filterwarnings=error`).

### download-arch-goreleaser-mapping
`.goreleaser.yaml` names assets uname-style (`amd64`→`x86_64`, `386`→`i386`). Go's gopsutil `KernelArch` already
returns the uname form, but Python's `platform.machine()` returns `amd64` on Windows — a naive port builds a 404.
`build_release_urls` applies the explicit map (amd64/x86_64→x86_64, 386/i386→i386, arm64/aarch64→arm64, else the
lowercased machine), title-cases the OS via `platform.system()`, and uses `zip` on Windows else `tar.gz`.

### upgrade-apply-only-for-frozen-binary
`apply()` swaps the RUNNING executable (the inconshreveable rename-dance). That is only coherent for a single
packaged binary (the M10 PyInstaller `eds.exe`); under `python -m eds`, `sys.executable` is the interpreter. The
`upgrade()` module + the `eds download` command are ported and usable now; the `upgrade` notification closure does
the real docker guard then returns `Success=false "self-upgrade requires the packaged binary"` (the
download→version-check→apply→parent-`/restart` self-upgrade is deferred to M10 — and is further blocked because the
current GitHub release assets are Go binaries, not the Python port). The untestable, can't-yet-run frozen flow was
deliberately NOT shipped.

### upgrade-hidefile-ctypes / upgrade-archive-missing-member-raises
`hideFile` (Windows `SetFileAttributesW(path, 0x2)`) is done via `ctypes` (no-op off Windows) — same FFI semantics.
On extraction, a zip with no `.exe` / a tar.gz with no `eds` member RAISES (harden, matching the C# port) rather
than Go's silent fall-through (zip: chmod a 0-byte file + success) / wrapped-EOF. The HTTP download also buffers the
archive into memory rather than streaming to the temp file (binaries are small; retry-correct) — same on-disk result.

## Config writer + enroll deviations

### config-toml-handwritten-writer
Go writes config.toml via BurntSushi/toml (enroll) and viper (configure/shutdown). tomli has no writer and tomli-w
is not a dep, so `eds/cmd/config.py::_dump_toml` is a minimal flat encoder (bool→lowercase, str→backslash/quote-
escaped + quoted, int/float verbatim). The config is only `token`/`server_id`/`url` (str) + `keep_logs` (bool), and
the file is never byte-compared (it is read back by tomli) — round-trip via tomli is the test contract.

### config-write-no-viper-merge
Go's `viper.Set(k,v)` + `viper.WriteConfig()` serializes the WHOLE merged config (config file + Set + bound flags +
defaults), so a configure/shutdown write also re-persists flag-bound keys (e.g. `keep_logs`). `set_config_value`
instead read-modify-writes only the keys already in config.toml plus the updated one. The load-bearing persisted
values (`token`/`server_id`/`url`) round-trip identically; only the incidental persistence of flag-bound keys
differs. WriteConfig errors are logged non-fatal (matching Go).

### enroll-forward-data-dir
The server's interactive enroll loop forks `eds enroll <code> --silent` and **forwards `--data-dir`** so the enroll
writes config.toml to the same directory the server reads. Go omits `--data-dir` on the fork and relies on a shared
default cwd/data (a latent inconsistency if `--data-dir` was set); forwarding it is the correct behavior.

## Log-file sink deviations

### logsink-clean-text-format
Go tees fork logs to the file via `newLoggerWithSink` = a MultiLogger of the console logger + `NewJSONLoggerWithSink
(sink, LevelTrace)`, so the file holds go-common JSON lines. The Python `LogFileSink` instead receives the same
clean-text lines the console logger produces (an extension of the existing `logger-format` deviation; the C# port
did likewise). The file is gzipped + uploaded and read as text, so the on-wire format is not parity-critical. Two
sub-points: (1) the sink captures ALL levels (>=TRACE) regardless of the console `min_level`, matching Go's
Trace-level sink; (2) sink lines ALWAYS carry a timestamp (an archived log needs one), whereas the console honors
`--timestamp`.

### logsink-flush-and-naming
`LogFileSink.write` flushes after every line (Go's `os.File` writes are unbuffered) so a hard kill before
`close()`/rotate still leaves the logs on disk for `getRemainingLog` to upload at session end. The rotated file is
named `eds-<unixMilli>.log` exactly as Go (`time.Now().UnixMilli()`); a sub-millisecond double rotation could
collide and truncate the older file just as in Go — no disambiguating counter was added, since rotations are
seconds-to-hours apart in practice (newLogFileSink at startup, then the hourly sendlogs ticker / on-demand).

## Schema-validator deviations

### schema-jsonschema-lib
Go validates with `santhosh-tekuri/jsonschema/v5` (`compiler.AddResource` per file, `compiler.Compile`). The Python
port uses the `jsonschema` package + a `referencing.Registry`: every schema file is registered under the SAME three
URIs Go uses — `file://<rel>`, `file:///<rel>`, `file://<abs>` (forward-slashed on Windows) — so cross `$ref`s like
`file:///models/labor.json` resolve, and each table's root schema is compiled via `validator_for(root,
registry=...)`. `referencing`/`jsonschema` added as deps. Validation behavior (draft-07, `$ref`, required/enum/etc.)
matches; exact error *messages* differ between the two libraries (not parity-critical — only logged on skip).

### schema-path-gotemplate
The per-table `path` is a Go `html/template` (e.g. `{{.table}}/received/{{.table}}_{{.mvccTimestamp}}_{{.id}}.json`).
The Python port renders it with a minimal regex substituting `{{.field}}` / `{{.nested.field}}` against the schema
event map. Two sub-points: (1) no HTML escaping (Go's html/template escapes, but the substituted values —
table/id/mvccTimestamp — are path-safe so it is a no-op); (2) the template is not parsed/validated at load time (Go
errors in NewSchemaValidator on a malformed template). `validate` also returns a 3-tuple `(found, valid, path)` and
RAISES `SchemaValidationError` on a schema mismatch, instead of Go's 4-tuple `(found, valid, path, err)` with
`ErrSchemaValidation` — the consumer + importer already consume that raise-based contract.

## Final parity-audit deviations (surfaced by the full Go→Python verification fan-out)

A comprehensive adversarial audit (10 subsystems × find + verify) confirmed the port is FAITHFUL — the byte-spine
(JSON/float/struct), all SQL generation, NATS subjects + the msgpack/JSON reply split, the consumer flush/skip/ack
trees, the 3-layer runner exit-code trees, and the HTTP retry policy are parity-exact. One genuine bug was FIXED
(timeOffset RFC3339 parsing — now uses parse_rfc3339 + rejects naive datetimes, matching Go time.Parse(RFC3339)).
The remaining audit findings are edge/telemetry/threading/framework divergences, accepted + recorded here:

### heartbeat-stats-int-vs-float
Go msgpack-encodes the heartbeat's nested SystemStats directly, so integral float64 stats (load1/5/15, usedPercent,
the counters) pack as msgpack float64. Python builds the heartbeat's `stats` sub-tree via `json.loads(stats.
__gojson__())`, so an integral float (e.g. `40.0` → JSON `40`) becomes a Python int → msgpack int. The DECODED value
HQ receives is identical (msgpack int → a Go float64 field decodes fine); only the wire type byte differs, on a
fire-and-forget telemetry frame. Accepted (not worth restructuring the heartbeat to preserve the float type).

### control-closures-gate-fork-running
Go's pause/unpause/restart/shutdown notification closures gate on the sticky `configured` bool (set once for the
process). The Python closures gate on `ctx.fork_running` (a live fork exists). These agree except in the sub-ms
inter-session window (a fork has exited, the next has not yet started) while configured: there Go attempts the
loopback (which fails against the dead fork) and a pause/unpause replies Success=false, whereas Python skips it and
replies Success=true. Deliberate: the loopback only exists while a fork runs, so gating on fork_running avoids a
guaranteed-failing call; the difference is confined to a negligible timing window.

### worker-thread-no-process-exit
Go's `shutdown` closure does `logger.Fatal` (→ os.Exit(1)) on a failed loopback, and runImport does `os.Exit(ec)`
under `--no-restart` — both terminate the whole process immediately, from the notification handler. In Python those
handlers run on the async notification thread (NotificationRunner), where `sys.exit`/`logger.fatal` only raise
SystemExit IN that thread and do NOT terminate the process (and `os._exit` would skip all cleanup). So the Python
shutdown handler logs + returns, and the `--no-restart` import path's `sys.exit(ec)` is best-effort. Deliberate
threading-model deviation (a worker thread cannot faithfully reproduce Go's process-wide os.Exit).

### cli-parse-errors-exit-3
Go's cobra `Execute()` returns parse-level errors (unknown flag/command, bad value, wrong positional count) and
`main` does `os.Exit(1)`. The Python hand-rolled argparse dispatcher's `_Parser.error` exits 3 (EXIT_INCORRECT_USAGE)
for those — a deliberate cross-port convention (the C# port likewise maps CLI-misuse to its incorrect-usage code).
Extends `cli-argparse`; the runner's L2 tree handles both exit 3 and exit-1-with-required-flag-text identically, so
the supervisor behavior is unaffected; only a direct `eds <bad-cli>` shell exit code differs (3 vs 1).

### initconfig-not-global
Go registers `cobra.OnInitialize(initConfig)`, so config.toml is loaded (and a corrupt one → exit 3) before EVERY
command. Python calls `init_config` only inside the server path. Effects: with a corrupt config.toml,
`version`/`publickey`/`download` exit 0 (Python) vs 3 (Go); and a standalone `eds import` after `enroll` (without
re-passing `--api-key`) does not fall back to the config.toml token the way Go's viper does. The
notification-driven import always passes `--api-key`, so only the rare standalone-after-enroll path differs.
Accepted (the config-fallback pattern exists for `server`; extending it to every command is deferred).

### mask-url-urllib
`util.mask_url` is built on urllib (urlsplit/parse_qs/unquote) rather than the repo's faithful `eds.util.gourl`.
For normal DB URLs the masked output is identical; for percent-encoded path tails and `;`/malformed-`%` query
segments the masked bytes differ (the masked `DriverMeta.url` wire field). NOT a secret-leak (userinfo/path/query
are still masked, just with different masked bytes for exotic inputs). Accepted (reimplementing on gourl is deferred;
no normal connection string is affected).

## Streaming-driver deviations (s3 / kafka / eventhub)

### help-generate-section
`util.generate_help_section` (Go `util.GenerateHelpSection`) emits PLAIN `title + "\n\n" + body`. Go colorizes
via fatih/color (green title + white-bold body), but fatih/color AUTO-DISABLES on a non-TTY, so on every
non-interactive run Go already emits exactly this plain text; the C# port made the identical choice (Help.cs).
The output is byte-identical after `ansi_strip` (which the driver-configurations metadata path applies). The
SQL/Snowflake `help()` bodies stay `""` (see `sql-driver-help-deferred`); only the s3/kafka/eventhub drivers
populate a help section. (Terminal coloring can be layered later behind a TTY check.)

### s3-buffered-upload
Go's s3 driver streams each event through a worker-pool channel during `Process` (uploadTasks goroutines upload
concurrently; `Flush` waits on the job WaitGroup and joins the buffered errors). The port buffers `(key, event)`
per batch and uploads them at `Flush` with bounded concurrency (a `ThreadPoolExecutor(max_workers=uploadTasks)`),
joining errors there. SAME object bytes, SAME keys, and the SAME error-surfacing point (`Flush`). This mirrors
the C# port (`DEVIATIONS.md#s3-buffered-upload` there). `maxBatchSize` (which only sized Go's channel) is still
parsed/validated at connect — a non-empty unparseable value still aborts startup — but is otherwise unused.

### s3-gcs-resign-not-ported
Go re-signs GCS requests with a `RecalculateV4Signature` RoundTripper to work around aws-sdk-go-v2 mutating the
`Accept-Encoding` header AFTER signing. That is an aws-sdk-go-v2-specific bug; boto3/botocore do not exhibit it,
so the re-sign transport is not reproduced (the C# port reached the same conclusion). For the Google provider the
port instead sets `request_checksum_calculation = "when_required"` (the botocore analog of Go's
`RequestChecksumCalculation = WhenRequired`) so GCS's S3-interop layer does not reject flexible-checksum headers.
S3/GCS connect paths are behind the lazy boto3 import and are exercised only by the Docker-gated e2e.

### kafka-explicit-partition
Go uses segmentio/kafka-go's pluggable `Balancer`, which the broker driver calls with the live partition list at
send time. librdkafka (confluent-kafka) cannot host a managed partitioner, so the port resolves the topic's
partition count from metadata (`Producer.list_topics`) and computes the partition itself
(`balance(header, key, count)` = `Modulo(Hash(input), count)`), then produces to that explicit partition —
preserving Go's header-based ordering. If the partition count cannot be resolved the failure propagates so
`Flush` preserves `_pending` and NAKs (matching Go's `WriteMessages` error path). Same approach as the C# port.

### kafka-leader-retry
Go decides the 10s leader-not-available retry via `strings.Contains(err.Error(), "Leader Not Available")` —
segmentio/kafka-go's title-cased message. confluent-kafka/librdkafka instead surfaces a `KafkaError` whose
`code()` is `LEADER_NOT_AVAILABLE` and whose text is the lowercase `"Broker: Leader not available"`, so the Go
substring would NEVER match (immediate NAK, losing Go's grace). `is_leader_not_available` therefore matches the
error CODE first (the robust signal) and falls back to a case-insensitive substring (covers wrapped/string
errors); `_resolve_partition_count` re-raises a topic-metadata error as a `KafkaException` so a leader-not-available
metadata failure is retryable too. Mirrors the C# port (KafkaDriver.cs:138-142 — code check + substring fallback).

### fork-port-default
Go's hidden `fork --port` flag defaults to the literal `0` (cmd/fork.go:306); the server ALWAYS forwards an
explicit `--port` to the fork (cmd/server.go:539), so the default is rarely hit. The port defaults `fork --port`
to the literal `8080` (not `$PORT` — Go's fork ignores the env var) so a directly-invoked fork has a usable
health/metrics port; the CLI `--port` overrides it. The server path is unaffected (it passes `--port` explicitly).

## Features (net-new behavior; Go is NOT the oracle)

### import-recovery
**This is a FEATURE, not a deferral.** Net-new import-level recovery/retry with NO Go counterpart (Go's import
`Fatal`s on the first failed table; its only retry is the per-HTTP-request `HttpRetry`). The cross-port oracle
is `migration/features/import-recovery.md` (NOT the Go source) — both the Python and .NET ports MUST behave
identically and assert against the contract there; Go-parity audits SKIP `FEATURE(import-recovery)`-marked code.
The binding cross-port decisions live in §1c (REVIEW RECONCILIATION) and override §2's older prose. Summary of
what landed in this port (all tagged `# FEATURE(import-recovery):`):
- `is_recoverable(err)` (`eds/cmd/import_cmd.py`) — the §1c.4 matrix, classify by EXCEPTION TYPE first then the
  canonical shared substring set (`connection reset | connection refused | broken pipe | timed out | EOF | no
  such host | tls handshake | dns`). Retry: builtin `ConnectionError`/`TimeoutError` (+ requests' network errors
  routed by substring), HTTP `408/429/500/502/503/504`, download HTTP/network IO. FATAL: malformed URL
  (`ValueError`→exit 3), `PermissionError`, disk-full/other LOCAL `OSError` (errno in `_FATAL_ERRNOS`),
  `401/403/400/404/422`, schema/config, cancellation, unknown. (FIX: no longer blanket-retries ALL `OSError`.)
  Backed by `ApiStatusError` (`eds/cmd/session.py`), which carries the HTTP status while keeping the message
  byte-identical so the `--max-retries 0` path is unchanged.
- `failed_tables(job)` (`eds/cmd/import_client.py`) — additive sibling of the raise-on-first `check_export_job`
  (the raise wrapper is kept for the `--max-retries 0` / disabled path). Export-stage `Failed` is handled INLINE
  in the recovery loop (never raised), so there is NO `Export*Failed` exception type (removed per §1c.10).
- `run_with_recovery(...)` (`eds/cmd/import_cmd.py`) — wraps export→poll→download→import with the LOCKED ladder
  `[30,60,120,240,480]` (`backoff_ladder`, 5 retries / 6 attempts == `30*2^n`), reusing the injectable `sleep`
  seam. The recall (§1c.1): export-stage `Failed` ⇒ POST `/v3/export/bulk {tables:[failed], companyIds,
  timeOffset}` minting a NEW jobId; a TRANSIENT download / load-stage failure ⇒ GET re-download the SAME job; a
  download URL-EXPIRY (HTTP 403/410, `DownloadStageError.expired`) ⇒ POST a new job. The original run's
  `timeOffset` is captured in `ImportPlan` and reused in every recall POST (§1c.10). §1c.3: when the concrete
  table set was never learned (poll failed) on a FULL import, the scope is the FULL set — a recoverable failure
  NEVER collapses to "0 tables → EXIT_SUCCESS"; exhaustion there records `["*"]` and exits 1.
- `run_id` = §1c.2 canonical formula `eds_hash("|".join([driver_url, sorted(only), sorted(companyIds),
  sorted(locationIds), str(timeOffset)]))` (driver_url FIRST, lists SORTED, `"|"` sep) — pinned by a golden vector.
- Per-table durability + cross-restart resume (`eds/importer/__init__.py` `ImportFlusher` + the gated
  table-grouped loop; `eds/drivers/sql_base.py` `flush_imported`): `import-progress:{run_id}:{table}` markers
  written after each table durably flushes; a (re)started import resumes only the not-yet-completed tables and
  resets the markers on whole-run success. Gated behind `ImporterConfig.recovery_enabled`. §1c.5: re-truncate
  (`create_datasource`) is gated on NOT `--no-delete` — a `--no-delete`/audit retry RE-APPENDS the in-flight table
  (PK-safe at-least-once) rather than dropping the audit trail. §1c.9: streaming sinks (kafka/eventhub/s3/file)
  get per-RUN resume only (at-least-once on retry — downstream must tolerate dupes).
- Soft after-exhaustion (§1b OQ-4): a permanently-failed set is logged + recorded (`import-failed:{run_id}`),
  the rest of the run continues, and the run exits 1 (never 3). The server-triggered fork maps a non-usage
  exit-1 to "start the consumer" (faithful to Go), so a partial soft-exhaustion does NOT block streaming.
- `--max-retries` flag (`eds/cmd/root.py`, default 5; 0 = exact Go; bad/negative clamps to 0 per §1c.6) +
  `import_max_retries` config (`resolve_max_retries`, precedence flag > config.toml > 5, persisted via
  `set_config_value`, like `--mode`).
- Tests: `tests/test_import_recovery.py`.

### import-log-verbosity
**This is a FEATURE, not a deferral.** Verbose-only per-table import detail; NO Go oracle — IDENTICAL Python↔C#;
intentional (more troubleshooting data). The cross-port oracle is `migration/features/import-log-verbosity.md`
(byte-identical in both repos). `eds import` emits the SAME log MESSAGES (text + level + emit-timing) at BOTH
levels:
- DEFAULT (no `--verbose`): the Go once-per-batch shape — INFO `Importing data to tables <joined>` once, INFO
  `imported <R> records from <F> files in <dur>` once (batch totals), terminal INFO `👋 Loaded <N> tables in
  <dur>` (the recovery path's dropped seconds are restored to match the legacy path + Go). Tagged `# PARITY:`.
- `--verbose` (DEBUG): the above PLUS a per-table detail layer (`# FEATURE(import-log-verbosity):`) emitted in
  `eds/importer/__init__.py` `_run_recovery` — per table, DEBUG `importing table <t>` then DEBUG `imported <r>
  records from <f> files for table <t> in <dur>` with PER-TABLE counts (that table's files/records, NOT the
  all-files batch total).
- Recovery-only lines (`# PARITY(import-log-verbosity §5):`, no Go oracle, aligned to the C# twin word-for-word):
  INFO `recovering: retrying <tables> in <delay>s (attempt <n>/<max>)`, WARN `giving up on tables <tables> after
  <max> retries` (precedes soft-exhaustion exit 1), INFO `resuming import: skipping already-completed tables
  <tables>` (cross-restart).
- ORDERING DEVIATION (recovery path): `_run_recovery` replays tables in the order their files first appear in the
  sorted directory listing, then any zero-file configured table (so every table still gets a flush + completion
  marker). This is table-GROUPED, not Go's pure single-pass interleaved directory order — pure directory order is
  incompatible with the per-table flush boundary required for crash recovery. Identical Python↔C# (both keep the
  per-table flush). Recovery behavior (markers / cross-restart resume / soft-exhaustion exit 1) is UNCHANGED.
- The verbose layer is added only on the recovery (default `eds import`) path; the `--max-retries 0` / `--dir`
  legacy single-pass path stays exact-Go.
- Tests: `tests/test_import_recovery.py` (`test_import_log_default_is_once_per_batch_no_per_table_detail`,
  `test_import_log_verbose_adds_per_table_detail_with_per_table_counts`, `test_recovery_only_wording_matches_contract`,
  `test_resuming_wording_matches_contract`).

<!-- Add further deviations below as they arise (carry over the C# port's where they recur:
     file-uri-windows-drive-letter, download-zip-extract). -->
