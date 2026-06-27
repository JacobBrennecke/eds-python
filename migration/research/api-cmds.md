# Behavioral Specification: `api-cmds` Subsystem

## 1. Purpose

This subsystem comprises the Shopmonkey EDS API data contracts (`internal/api/api.go`) and the small, self-contained CLI sub-commands built on the Cobra command framework: `enroll`, `download`, `publickey`, `version`, and `integrationtest`. Together they (a) define the JSON/TOML shapes exchanged with the Shopmonkey control-plane API, (b) exchange a one-time enrollment code for a server API key persisted to disk, (c) download a release binary from GitHub and cryptographically verify it with an embedded PGP public key before extracting it, (d) print the embedded PGP key and the build version, and (e) provide developer-only NATS load/test-data publishing commands. These commands are leaves in the larger EDS application; the heavyweight `server` command (out of scope here) consumes the same `api` types for session start/end.

---

## 2. Public surface

### 2.1 `internal/api/api.go` (package `api`)

All structs are plain DTOs. JSON tags are exact; note `omitempty` presence/absence carefully (it changes the wire shape).

```go
type DriverMeta struct {
    ID          string `json:"id"`
    Name        string `json:"name"`
    Description string `json:"description"`
    URL         string `json:"url"` // masked before sending (may contain secrets)
}

type SessionStart struct {
    Version    string      `json:"version"`
    Hostname   string      `json:"hostname"`
    IPAddress  string      `json:"ipAddress"`
    MachineId  string      `json:"machineId"`
    OsInfo     any         `json:"osinfo"`            // arbitrary object (see SystemInfo below)
    Driver     *DriverMeta `json:"driver,omitempty"`
    ServerID   string      `json:"serverId"`
    CompanyIDs []string    `json:"companyIds,omitempty"`
}

type EdsSession struct {
    SessionId  string  `json:"sessionId"`
    Credential *string `json:"credential"`           // NOTE: no omitempty -> serializes as null when nil
}

type SessionStartResponse struct {
    Success bool       `json:"success"`
    Message string     `json:"message"`
    Data    EdsSession `json:"data"`
}

type SessionEnd struct {
    Errored bool `json:"errored"`
}

type SessionEndURLs struct {
    URL      string `json:"url"`
    ErrorURL string `json:"errorUrl"`
}

type SessionEndResponse struct {
    Success bool           `json:"success"`
    Message string         `json:"message"`
    Data    SessionEndURLs `json:"data"`
}

type EnrollTokenData struct {
    Token    string `json:"token" toml:"token"`
    ServerID string `json:"serverId" toml:"server_id"`   // NOTE: toml key is snake_case server_id
}

type EnrollResponse struct {
    Success bool            `json:"success"`
    Message string          `json:"message"`
    Data    EnrollTokenData `json:"data"`
}

func GetAPIURL(firstLetter string) (*string, error)
```

`GetAPIURL` maps the first character of an enrollment code to an environment base URL:

| key | URL |
|-----|-----|
| `"P"` | `https://api.shopmonkey.cloud` |
| `"S"` | `https://sandbox-api.shopmonkey.cloud` |
| `"E"` | `https://edge-api.shopmonkey.cloud` |
| `"L"` | `http://localhost:3101` |

Any other key returns `(nil, errors.New("invalid code"))`. The match is **case-sensitive** and exact (single uppercase letter).

### 2.2 Cobra commands (package `cmd`)

- `enrollCmd` — `Use: "enroll [code]"`, `Args: cobra.ExactArgs(1)`. Hidden flag `--api-url` (string, default `""`, help text: `"the for testing again preview environment"`). Inherits persistent `--data-dir` (`-d`), `--verbose`, `--silent`, `--timestamp`, etc. from root.
- `downloadCmd` — `Use: "download [version] [filename]"`, `Args: cobra.ExactArgs(2)`. No own flags.
- `publicKeyCmd` — `Use: "publickey"`. No args, no flags.
- `versionCmd` — `Use: "version"`. No args, no flags.
- `integrationtestCmd` — `Use: "integrationtest"`. Parent command with `PersistentPostRun` that calls `integrationtest.GlobalConnectionHandler.DisconnectAll()`. Sub-commands:
  - `loadTestRandomCmd` — `Use: "loadtest-random"`. Flags: `--count` (int, default 1), `--delay-ms` (int, default 100).
  - `publishFileDataCmd` — `Use: "publish-file-data"`. Flag: `--count` (int, default 1).
- `func RunWithLogAndRecover(fn func(cmd *cobra.Command, args []string, log logger.Logger)) func(cmd *cobra.Command, args []string)` — exported wrapper that injects a logger, logs start/complete, and recovers from panics.

Package-level vars set by `main`: `var Version string` and `var ShopmonkeyPublicPGPKey string` (declared in `cmd/server.go`).

### 2.3 `internal/upgrade` (package `upgrade`)

```go
type UpgradeConfig struct {
    Logger       logger.Logger
    Context      context.Context
    BinaryURL    string
    SignatureURL string
    Filename     string
    PublicKey    string
}

func Upgrade(config UpgradeConfig) error            // download + PGP-verify + extract (used by download cmd)
func Apply(targetPath string, sourcePath string) error  // atomic self-replace (NOT used by download)
func RollbackError(err error) error                 // extracts rollback error from Apply failure
```

Unexported: `prepareAndCheckBinary`, `commitBinary`, `rollbackErr`, `hideFile` (two build-tagged variants).

### 2.4 `internal/integrationtest` (package `integrationtest`)

```go
var GlobalConnectionHandler connectionHandler

func NewConnection[T any](connection typedConnection[T]) T
type NatsConnection struct { nc *nats.Conn }            // Connect/Close/Get
type JetstreamConnection struct { NatsConnection; js jetstream.JetStream }

func PublishRandomMessages(js jetstream.JetStream, count int, delayMs int, log logger.Logger) int
func PublishTestData(path string, count int, js jetstream.JetStream, log logger.Logger) int
func NatsMessageFromEvent(event *internal.DBChangeEvent) natsMessage
func NewTestDataGenerator(filename string, publicTables []string) *TestDataGenerator
func (t *TestDataGenerator) NextEvent() *internal.DBChangeEvent
func (t *TestDataGenerator) Close() error
```

### 2.5 Supporting helpers reused by these commands (package `cmd` / `util`)

- `util.NewHTTPRetry(req *http.Request, opts ...HTTPRetryOption) *HTTPRetry`, `(*HTTPRetry).Do() (*http.Response, error)`, options `WithLogger`, `WithTimeout`.
- `cmd.handleAPIError(resp, context) error`, `cmd.getRequestID(resp) string`, `cmd.setHTTPHeader(req, apiKey)`, `cmd.getDataDir(cmd, logger) string`, `cmd.getCommandExample(command, args...) string`, `cmd.newLogger(cmd) logger.Logger`, `cmd.mustFlagString(cmd, name, required) string`.

---

## 3. Behavior & algorithms

### 3.1 `enroll [code]`

1. Build logger via `newLogger(cmd)`, then `logger.WithPrefix("[enroll]")`.
2. `code := args[0]`.
3. `apiURL := mustFlagString(cmd, "api-url", false)` (not required; empty default).
4. `dataDir := getDataDir(cmd, logger)` — resolves `--data-dir`, abs+clean, creates with mode `0700` if missing (after checking parent dir writable), else verifies writable. Fatals on failure.
5. If `apiURL == ""`: take `firstLetter := code[0:1]` (first byte/char of the code), call `api.GetAPIURL(firstLetter)`. On error: `logger.Fatal("error getting api url: %s", err)`. Otherwise `apiURL = *maybeApiURL`.
   - **Gotcha:** `code[0:1]` panics if `code` is empty, but `ExactArgs(1)` only guarantees one arg, not non-empty — an empty string arg would panic.
6. Create request: `http.NewRequest("GET", apiURL+"/v3/eds/internal/enroll/"+code, nil)`. **No** auth header / no `setHTTPHeader` is applied here (default Go headers only).
7. Execute via `util.NewHTTPRetry(req, util.WithLogger(logger)).Do()`. Fatal `"failed to enroll server: %s"` on transport error.
8. `defer resp.Body.Close()`. Status handling:
   - `200 OK` → continue.
   - `404 Not Found` → `logger.Fatal("invalid enrollment code or it has already been used")`.
   - any other non-200 → `logger.Fatal("%s", handleAPIError(resp, "enroll"))`.
9. Decode body into `api.EnrollResponse` (`json.NewDecoder(...).Decode`). Fatal on decode error.
10. If `!enrollResp.Success` → `logger.Fatal("failed to start enroll: %s", enrollResp.Message)`.
11. TOML-encode `enrollResp.Data` (type `EnrollTokenData`) into a `bytes.Buffer` via `github.com/BurntSushi/toml`. The resulting file content keys are `token` and `server_id` (from the `toml` struct tags).
12. `tokenFile := filepath.Join(dataDir, "config.toml")`.
13. `os.Create(tokenFile)` (truncates/creates), then immediately `os.WriteFile(tokenFile, buf.Bytes(), 0644)` — the actual content is written by `WriteFile` with mode `0644`. (The `os.Create` is effectively redundant; both target the same path.)
14. Log `"Enrollment successful!"` and `"run %s to start the server"` where `%s` is `getCommandExample("server")` (a backtick-wrapped invocation string).

`config.toml` is later read by `viper` at startup (`initConfig`), so it is the persisted credential store. Note the enroll command itself does **not** add a `[default]` section — it writes the two keys at top level.

### 3.2 `download [version] [filename]`

1. Logger with prefix `[download]`.
2. `host.Info()` (gopsutil) → fatal on error.
3. `version := args[0]`; `filename := args[1]`.
4. If `version` does not start with `"v"`, prepend `"v"` (so `1.2.3` → `v1.2.3`).
5. `platform := strings.ToUpper(Host.Platform[0:1]) + Host.Platform[1:]` — capitalizes first letter. gopsutil `Platform` is lowercase (e.g. `"windows"`→`"Windows"`, `"linux"`→`"Linux"`, `"darwin"`→`"Darwin"`).
6. `arch := Host.KernelArch` (e.g. `x86_64`, `arm64`, `aarch64`).
7. `ext := "tar.gz"`; if `Host.Platform == "windows"` then `ext = "zip"`.
8. `binaryURL := fmt.Sprintf("https://github.com/shopmonkeyus/eds/releases/download/%s/eds_%s_%s.%s", version, platform, arch, ext)` — e.g. `.../download/v1.2.3/eds_Linux_x86_64.tar.gz`.
9. `signatureURL := binaryURL + ".sig"`.
10. Call `upgrade.Upgrade(...)` with `Context: context.Background()`, the URLs, `Filename: filename`, `PublicKey: ShopmonkeyPublicPGPKey`. Fatal `"%s"` on error.
11. On success: `"version %s download successful, saved to %s"`.

### 3.3 `upgrade.Upgrade` — download + PGP-verify + extract

1. Record `started := time.Now()`; `defer` logs `"download took %s"`.
2. Create temp file `os.CreateTemp("", "eds")`; `defer os.Remove(tmp.Name())`.
3. Load armored public key: `crypto.NewKeyFromArmored(config.PublicKey)` (ProtonMail gopenpgp v3). Error → `"error reading public key: %w"`.
4. `pgp := crypto.PGP()`; build verifier: `pgp.Verify().VerificationKey(publicKey).New()`.
5. GET `BinaryURL` (with context) through `util.NewHTTPRetry(...).Do()`. Stream body to temp file via `io.Copy`; record `binaryLen`. Close body and temp file. Debug-log size.
6. GET `SignatureURL` similarly; `io.ReadAll` the body into `signature []byte`. Debug-log size.
7. Re-open temp file; create verifying reader: `verifier.VerifyingReader(of, bytes.NewReader(signature), crypto.Auto)`. `crypto.Auto` lets the library auto-detect armored vs binary signature.
8. `reader.ReadAllAndVerifySignature()` → `verifyResult`. Then `verifyResult.SignatureError()` — if non-nil → `"error in signature verification: %w"`. **This is the security gate; verification failure aborts before extraction.**
9. Create destination file `os.Create(config.Filename)`.
10. Extraction branch keyed on `filepath.Ext(config.BinaryURL)`:
    - **`.zip`** (Windows): `zip.OpenReader(tmp.Name())`. Iterate entries; for the first entry whose `filepath.Ext(f.Name) == ".exe"`, copy its contents into the destination and **`return nil` immediately** (skips the chmod step below).
    - **else** (tar.gz; note `filepath.Ext(".../eds_Linux_x86_64.tar.gz")` == `".gz"`): open temp, `gzip.NewReader`, `tar.NewReader`. Loop `tr.Next()`; for the entry whose `header.Name == "eds"` (exact match), copy contents to destination and `break`. **Edge:** if no `"eds"` entry exists, the loop runs until `tr.Next()` returns `io.EOF`, which is returned as `"error reading tar header: %w"` (so EOF is treated as an error here).
11. `os.Chmod(config.Filename, 0755)` (only reached on the tar.gz path; the zip path returns earlier).

### 3.4 `upgrade.Apply` / `commitBinary` / `RollbackError` (present but NOT invoked by `download`)

Atomic self-replacement helper (Apache-2.0 derived). `Apply(target, source)`:
- Copies `source` into `<dir>/.<filename>.new` with mode `0755`, closing the fp explicitly (Windows file-lock workaround).
- `commitBinary`: removes any `.<filename>.old`, renames `target`→`.old`, renames `.new`→`target`. On failure rolls back (`.old`→`target`); if rollback also fails returns `*rollbackErr`. On success removes `.old`; if removal fails (Windows) calls `hideFile` to set the FILE_ATTRIBUTE_HIDDEN (`2`) via `SetFileAttributesW` (`hide_windows.go`); on non-Windows `hideFile` is a no-op.
- `RollbackError(err)` returns the inner rollback error if `err` is a `*rollbackErr`, else nil.

### 3.5 `publickey`

Prints `ShopmonkeyPublicPGPKey` via `fmt.Println` (adds trailing newline). The key is embedded from `shopmonkey.asc` at build time (`//go:embed`). The literal content is the ASCII-armored Curve25519 (EdDSA/ECDH) OpenPGP public key for `Shopmonkey, Inc. <engineering@shopmonkey.io>` beginning `-----BEGIN PGP PUBLIC KEY BLOCK-----` … ending `=r0eF` / `-----END PGP PUBLIC KEY BLOCK-----`.

### 3.6 `version`

Prints `Version` via `fmt.Println`. `Version` originates in `main.go` as `var version = "dev"`; if it equals `"dev"` and env var `GIT_SHA` is set and non-empty, `version` is replaced by that value before being assigned to `cmd.Version`.

### 3.7 `integrationtest` and sub-commands (developer-only)

- `RunWithLogAndRecover(fn)` wraps a run func: builds `log := newLogger(cmd)`, logs `"starting integration test: <cmd.Name()>"`, installs `defer recover()` that logs `"error running integration test: %s"` on panic, calls `fn`, then logs `"completed integration test: <cmd.Name()>"`.
- Parent `integrationtestCmd.PersistentPostRun` always calls `GlobalConnectionHandler.DisconnectAll()` to close all NATS connections.

**`loadtest-random`:** reads `--count` (default 1) and `--delay-ms` (default 100). Creates a Jetstream connection via `integrationtest.NewConnection(&integrationtest.JetstreamConnection{})`. Calls `PublishRandomMessages(js, count, delayMs, log)`.

`PublishRandomMessages`:
- Constants: `cid = "28a6712e-83a0-4ede-97cb-c3f5201068dc"`, `lid = "5b7a05d6-c971-4f77-8792-9c12744a811d"`, `uid = "test-user-789"`.
- For `i` in `0..count-1`: `customerID = "customer"+strconv.Itoa(i)`; build a random customer map (`generateRandomCustomer`); marshal to JSON (fatal on error).
- Build `internal.DBChangeEvent`: `ID = util.Hash(time.Now()) + customerID`, `Operation="UPDATE"`, `Table="customer"`, `Key=[customerID]`, `ModelVersion="547388d6b0a76f85"`, `CompanyID=&cid`, `LocationID=&lid`, `UserID=&uid`, `Timestamp=time.Now().UnixMilli()`, `MVCCTimestamp=fmt.Sprintf("%d", time.Now().UnixNano())`, `After=json.RawMessage(customerJSON)`, `Diff=["firstName","lastName","email"]`, `Imported=false`.
- `buf = json.Marshal(event)`; `msgID = util.Hash(event)`; `subject = fmt.Sprintf("dbchange.customer.UPDATE.%s.1.PUBLIC.2", *event.CompanyID)`.
- `js.Publish(ctx, subject, buf, jetstream.WithMsgID(msgID))` (panic on error).
- Logs progress; if `delayMs > 0` sleeps `delayMs` ms; increments `delivered`.

`generateRandomCustomer(companyID, locationID, customerID)` builds a `map[string]any` with randomized fields using `math/rand` and `github.com/google/uuid`. Notable probabilistic rules (using `rand.Float32() > threshold`):
- `customerType` ∈ {Individual, Fleet}; `country` ∈ {US, CA, MX}; `preferredContactMethod` ∈ {Email, Phone, SMS}; `preferredLanguage` ∈ {en_US, es_US, fr_CA}.
- `dotNumber` only when type == Fleet (6-digit zero-padded).
- `discountPercent = rand.Float64()*25.0`.
- `createdDate = now - rand(0..365d) seconds`, formatted `time.RFC3339`; `updatedDate = now`.
- `lastTimeOrderWorked`: 70% chance (`rand.Float32() > 0.3`).
- `taxExempt/gstExempt/hstExempt/pstExempt`: 20% chance each (`> 0.8`).
- `marketingOptIn`: 30% chance (`> 0.7`).
- additional `locationIds`: 50% chance, then 1–3 random UUIDs appended.
- `metadata`: 50% chance; `customFields`: 40% chance (`> 0.6`).
- Static `labels`: one label with `color "#FFD700"`.
- Field formats: `postalCode = "%05d"`, `phone = "555-%03d-%04d"`, `email = "user%d@example.com"`, etc.

**`publish-file-data`:** reads `--count` (default 1). Hard-coded `path = "../eds_test_data/output.jsonl.tar.gz"`. Calls `PublishTestData(path, count, js, log)`.

`PublishTestData`:
- `getPublicTables(log)`: builds an API registry via `registry.NewAPIRegistry(context.Background(), log, "http://api.shopmonkey.cloud", "test", nil)` (note: HTTP, not HTTPS, and API key literal `"test"`), calls `GetLatestSchema()`, returns the list of table names. Panics on any error.
- `NewTestDataGenerator(path, publicTables)`: opens the file, wraps with `gzip.NewReader`, then `tar.NewReader`; calls `tr.Next()` once to skip to the first file, then `tr.Next()` again ("skip the header") — **two `Next()` calls before scanning**; wraps the current tar entry in `bufio.Scanner`. Stores fixed IDs `cid="28a6712e-83a0-4ede-97cb-c3f5201068dc"`, `lid="5b7a05d6-c971-4f77-8792-9c12744a811d"`, `uid="test-user-789"`.
- Loop until `delivered >= count` or `NextEvent()` returns nil:
  - `NextEvent`: scans next JSONL line into `internal.DBChangeEvent`; if `event.Table` not in `publicTables` → skip; else set `event.Timestamp = makeEventTimestampRecent(event.Timestamp)` and override `CompanyID/LocationID/UserID` with the fixed test IDs; return.
  - `makeEventTimestampRecent(ts)`: `daysInMillis=86400000`; shifts the timestamp forward so its day equals `tomorrow` (`time.Now().UnixMilli()/daysInMillis + 1`): `ts + (tomorrowDays - originalDays)*daysInMillis`.
  - `NatsMessageFromEvent(event)`: `json.Marshal(event)`; `subject = fmt.Sprintf("dbchange.%s.UPDATE.%s.1.PUBLIC.2", event.Table, *event.CompanyID)`; `msgID = util.Hash(event)`.
  - `js.Publish(ctx, subject, message, jetstream.WithMsgID(msgID))` (panic on error). Increment `delivered`.

`integrationtest.connection.go` connection management:
- `connectionHandler` holds a slice of `connectable`; `addConnection` appends; `DisconnectAll` calls `Close()` on each.
- `NewConnection[T]` registers the connection globally, calls `Connect()`, returns `Get()`.
- `NatsConnection.Connect`: if `nc == nil`, `nats.Connect("nats://localhost:4222")` (panic on error). `Close` closes if non-nil. `Get` returns `*nats.Conn`.
- `JetstreamConnection` embeds `NatsConnection`; `Connect` connects NATS then `jetstream.New(nc)` (panic on error); `Get` returns the `jetstream.JetStream`.

### 3.8 `util.HTTPRetry` (shared retry engine)

- `defaultTimeout = 30s`.
- `NewHTTPRetry(req, opts...)` sets `timeout=30s` unless `WithTimeout` overrides; `WithLogger` attaches a logger.
- `Do()`: on first call records `started = now`; increments `attempts`; performs `http.DefaultClient.Do(req)`.
- `shouldRetry(resp, err)`:
  - If `err != nil` and the message contains `"connection reset"` or `"connection refused"`, retry **only if** `started + timeout` is still in the future (i.e. within the 30s/configured window).
  - If `resp != nil` and status ∈ {`408` RequestTimeout, `502` BadGateway, `503` ServiceUnavailable, `504` GatewayTimeout, `429` TooManyRequests}: drain+close body and retry.
  - Otherwise no retry.
- Backoff: `jitter = 100ms + rand.Int63n(500*attempts) ms`; logs at Trace; `time.Sleep(jitter)`; recursively calls `Do()`. **No hard cap on attempt count** — retries are bounded only by the 30s timeout window for connection errors, and are effectively unbounded for the 5xx/429 status cases (they always return `true`). Be careful: a server that persistently returns 503 causes infinite retry with growing jitter.
- **Important:** the request body is not re-buffered between attempts; since enroll/download requests have `nil` or already-consumed bodies, that is acceptable here, but POSTs reusing the same `*http.Request` could resend an exhausted body.

### 3.9 Error/response helpers

- `setHTTPHeader(req, apiKey)` (NOT used by enroll/download GETs, but used by session start/end): sets `Content-Type: application/json`, `User-Agent: "Shopmonkey EDS Server/" + Version`; if `apiKey != ""` adds `Authorization: Bearer <apiKey>`.
- `getRequestID(resp)` reads header `X-Request-Id`.
- `errorResponse.Parse(buf, statusCode, context, requestId)`: if `requestId != ""`, `requestIdTag = "(requestId=<id>)"`. If `json.Unmarshal(buf, &{Message})` succeeds → `"<context>: <message> <requestIdTag>"`; else → `"<context>: <rawBody> (status code=<code>) <requestIdTag>"`.
- `handleAPIError(resp, context)` reads the full body then delegates to `errorResponse.Parse`.

### 3.10 `util.Hash`

`Hash(vals ...interface{}) string`: creates an xxhash (`github.com/cespare/xxhash/v2`), and for each value writes `[]byte(fmt.Sprintf("%+v", v))` (via zero-copy string→[]byte). Returns `fmt.Sprintf("%x", h.Sum(nil))` (lowercase hex). Used for NATS message dedup IDs. **Faithful porting requires reproducing Go's `%+v` formatting**, which is non-trivial for structs/maps.

---

## 4. External dependencies

| Go package | Role | Suggested .NET equivalent |
|---|---|---|
| `github.com/spf13/cobra` | CLI command/flag framework | `System.CommandLine`, or `Spectre.Console.Cli` |
| `github.com/spf13/viper` | Reads `config.toml` at startup | `Microsoft.Extensions.Configuration` + `Tomlyn`/`Tomlyn.Extensions.Configuration` |
| `github.com/BurntSushi/toml` | TOML-encode `EnrollTokenData` to `config.toml` | `Tomlyn` (NuGet) |
| `github.com/ProtonMail/gopenpgp/v3/crypto` | OpenPGP key parse + detached signature verify | `BouncyCastle.Cryptography` (Org.BouncyCastle, OpenPGP API). No first-class BCL OpenPGP. |
| `github.com/shirou/gopsutil/v4/host` | Host platform/arch/OS info for download URL + osinfo | `System.Runtime.InteropServices.RuntimeInformation` (OSPlatform, OSArchitecture); for richer host info, P/Invoke or `System.Management` |
| `archive/tar`, `archive/zip`, `compress/gzip` | Extract release archive | `System.Formats.Tar.TarReader`, `System.IO.Compression.ZipArchive`, `System.IO.Compression.GZipStream` |
| `net/http` (`http.DefaultClient`) | HTTP requests + retry | `System.Net.Http.HttpClient` (single shared instance) |
| `github.com/nats-io/nats.go` + `/jetstream` | NATS / JetStream publishing (integration tests) | `NATS.Client.Core` + `NATS.Client.JetStream` (NuGet) |
| `github.com/google/uuid` | Random UUIDs in test data | `System.Guid.NewGuid()` |
| `github.com/cespare/xxhash/v2` | Message dedup hashing | `System.IO.Hashing.XxHash64` (NuGet `System.IO.Hashing`) |
| `github.com/savsgio/gotils/strconv` | Zero-copy string↔[]byte | `System.Text.Encoding`/`Span<byte>` (just use UTF-8 bytes) |
| `github.com/denisbrodbeck/machineid` | Stable machine ID (`ProtectedID("eds")`) | Custom: hash a stable machine GUID (e.g. Windows `MachineGuid` registry / `/etc/machine-id`) with an app-scoped key |
| `github.com/shopmonkeyus/go-common/logger` | Structured leveled logger | `Microsoft.Extensions.Logging` (`ILogger`) |
| `github.com/shopmonkeyus/go-common/slice` | `slice.Contains` | `System.Linq` `Contains`/`HashSet<string>` |
| `//go:embed shopmonkey.asc` | Embed PGP key into binary | Embedded resource (`<EmbeddedResource>`) or a `const string` |

---

## 5. Edge cases & gotchas

- **PGP verification ordering:** The destination file (`config.Filename`) is created *before* extraction, but only *after* `SignatureError()` passes. If verification fails the function returns before creating the destination — the C# port must preserve "verify, then extract" ordering so a bad/unsigned binary is never written.
- **Zip path skips chmod:** On the `.zip` branch, `Upgrade` returns immediately after copying the `.exe` entry and never reaches `os.Chmod(0755)`. The tar.gz branch always chmods. (On Windows chmod is mostly cosmetic; on *nix only tar.gz applies, which is fine since *nix uses tar.gz.) Replicate this asymmetry exactly.
- **Tar entry name is exact `"eds"`; zip entry matched by extension `.exe`.** A renamed archive entry would not be found. In the tar case, reaching EOF without finding `"eds"` surfaces as `"error reading tar header"` (EOF is not specially handled).
- **`filepath.Ext` semantics:** `filepath.Ext("...tar.gz") == ".gz"`, so the tar branch is selected by the *not-`.zip`* condition, not by recognizing `.tar.gz`. Any C# `Path.GetExtension` mapping must branch the same way (zip ⇒ zip, everything else ⇒ tar.gz).
- **`code[0:1]` on empty arg panics.** `ExactArgs(1)` does not prevent an empty-string argument. Port should guard against empty code.
- **Enroll uses no auth/custom headers** (plain GET with Go defaults), unlike session start/end which use `setHTTPHeader`. Don't accidentally add `Authorization`/`User-Agent` to enroll.
- **`config.toml` written with mode `0644`** (world-readable), even though it contains the API token. The data dir itself is `0700`. The redundant `os.Create` before `os.WriteFile` is harmless but should not be mistaken for required behavior.
- **`EdsSession.Credential` has no `omitempty`** → when nil it serializes as JSON `null` (not omitted). The credential is written elsewhere via `writeCredsToFile` which base64-decodes the value and writes mode `0600`.
- **HTTP retry is essentially unbounded for 5xx/429** (always returns `true`), and for connection-reset/refused it only stops once the (default 30s) window elapses. The jitter grows with `attempts` (`rand(0, 500*attempts)`). A port using a fixed retry count would diverge; replicate the time-window + status-set logic.
- **Retry recursion + body reuse:** safe here because GETs have nil bodies; do not blindly reuse a consumed request body in C#.
- **Panics as control flow in integration tests:** `connection.go`, `filedata.go`, `random.go` use `panic` liberally; the `RunWithLogAndRecover` wrapper catches them and logs `"error running integration test: %s"`. The parent `PersistentPostRun` still runs `DisconnectAll()` afterward.
- **`util.Hash` uses `fmt.Sprintf("%+v", v)`** — Go-specific reflection formatting. For `DBChangeEvent` this includes pointer-dereferenced field names and values in struct order. Exact reproduction in C# is the single largest fidelity risk for the integration-test message IDs (dedup keys must match the Go output if interoperating with the same stream).
- **`makeEventTimestampRecent` uses integer day math** (`86400000` ms/day) and `+1` day to "make it tomorrow"; reproduce with integer division (not floating point) to match.
- **OS info shape (`SessionStart.OsInfo`)** is `any` populated by `util.GetSystemInfo()` → `SystemInfo{ Host *host.InfoStat `json:"host"`, NumCPU int64 `json:"num_cpu"`, GoVersion string `json:"go_version"` }`. `GoVersion` is `runtime.Version()[2:]` (strips the leading `"go"`). The `Host` object is the full gopsutil `InfoStat` (hostname, os, platform, platformVersion, kernelVersion, kernelArch, uptime, bootTime, procs, hostId, etc.). The C# port must emit an equivalent JSON object even though it has no exact gopsutil analog.
- **Windows file hiding:** `hide_windows.go` calls `SetFileAttributesW(path, 2)` (FILE_ATTRIBUTE_HIDDEN) via lazy-loaded `kernel32.dll`; returns the syscall error when `r1 == 0`. The non-Windows variant is a no-op. (Only relevant to `Apply`, not `download`.)
- **`download` resolves arch via `Host.KernelArch`** which can differ from process arch (e.g. `x86_64` vs `amd64` vs `aarch64`); the GitHub asset naming must match whatever gopsutil reports. A C# port using `RuntimeInformation.OSArchitecture` (`X64`/`Arm64`) must map to the same asset suffix strings the releases use.

---

## 6. C# port notes

- **Command framework:** Map each Cobra command to a `System.CommandLine` `Command` (or Spectre.Console `Command<TSettings>`). Keep the persistent `--data-dir` (`-d`, default `<cwd>/data`), `--verbose`, `--silent`, `--timestamp` as global options. The hidden `--api-url` should be a hidden option on `enroll` only.
- **API DTOs:** Use `record`/`class` with `System.Text.Json` `[JsonPropertyName]` attributes matching the Go json tags exactly. Critically: for `EdsSession.Credential` do **not** set ignore-when-null (it must serialize `null`); for fields with `omitempty` (`driver`, `companyIds`, etc.) use `JsonIgnoreCondition.WhenWritingNull`/`WhenWritingDefault` to match. For `EnrollTokenData`, you need both JSON (read from API) and TOML (write to file) shapes — when writing TOML use keys `token` and `server_id` (Tomlyn lets you control property names via a model or manual writing).
- **`GetAPIURL`:** a simple `Dictionary<string,string>` with the four exact entries; return null/throw for unknown keys ("invalid code"). Keep it case-sensitive.
- **HTTP retry:** Implement an `HttpClient`-based retry policy mirroring `HTTPRetry`: a 30s wall-clock window, retry on `HttpRequestException` whose inner socket error is connection reset/refused (`SocketError.ConnectionReset`/`ConnectionRefused`) only while within the window, and retry on status 408/429/502/503/504 unconditionally; backoff `100ms + Random(0, 500*attempt)ms`. Consider Polly, but note Polly's typical bounded retry differs from this "unbounded-on-5xx" behavior — configure to match (or replicate manually). Use a single shared `HttpClient`.
- **PGP verification:** Use BouncyCastle's OpenPGP API. Steps: parse the armored public key ring, build a detached-signature verifier from the `.sig` bytes (handle both armored and binary — gopenpgp's `crypto.Auto`), stream the downloaded binary through it, and confirm the signature validates before writing/extracting. Embed `shopmonkey.asc` as an embedded resource. Treat any verification error as fatal and ensure no output file is produced on failure.
- **Archive extraction:** Branch on the URL extension: `.zip` ⇒ `ZipArchive`, find first entry ending in `.exe`, copy out, and (matching Go) do **not** chmod; otherwise ⇒ `GZipStream` + `System.Formats.Tar.TarReader`, find entry named exactly `eds`, copy out, then set executable bit on *nix (no-op on Windows). Use a temp file (`Path.GetTempFileName`) and delete it in a `finally`.
- **Platform/arch string:** Reproduce `Capitalize(platform)` + `KernelArch`. Map `RuntimeInformation` to the exact strings the GitHub release uses (verify against an actual release asset list; e.g. `Linux`/`Darwin`/`Windows` and `x86_64`/`arm64`). This is brittle — centralize it.
- **Machine ID:** `denisbrodbeck/machineid.ProtectedID("eds")` is an HMAC-SHA256 of the OS machine GUID keyed by the app id, hex-encoded. To stay byte-compatible, read the same source (`HKLM\SOFTWARE\Microsoft\Cryptography\MachineGuid` on Windows; `/etc/machine-id` or `/var/lib/dbus/machine-id` on Linux; IOPlatformUUID on macOS), HMAC-SHA256 with key `"eds"`, hex-encode. If byte-compatibility with the Go server's ID is not required, document the deviation.
- **`util.Hash` (xxhash of `%+v`):** Use `System.IO.Hashing.XxHash64`. The hard part is `fmt.Sprintf("%+v", value)`. If these dedup IDs must match Go-produced IDs on the same NATS stream, you must emulate Go's `%+v` formatting precisely (field-name=value, struct ordering, pointer dereference). If the integration-test tooling is internal-only and not cross-running with Go, a documented, deterministic alternative is acceptable — call this out as a risk.
- **NATS/JetStream test commands:** Port with `NATS.Client` only if these dev tools are needed; otherwise they can be omitted from an initial faithful port (they are not part of the production data path). Preserve the subject format `dbchange.<table>.UPDATE.<companyId>.1.PUBLIC.2` and `WithMsgID` dedup semantics if ported.
- **Error formatting:** Reproduce `handleAPIError` output strings exactly (`"<context>: <message> (requestId=<id>)"` vs the raw-body fallback with `(status code=N)`), as operators may grep logs for these.
- **Process-exit semantics:** Go's `logger.Fatal` logs then exits the process (non-zero). In C#, throw a dedicated exception caught at the top level that writes the message and sets a non-zero exit code; mirror the specific exit codes used by flag validation (`os.Exit(3)` for missing/invalid flags, `os.Exit(1)` for general command failure).
- **TOML round-trip:** When writing `config.toml`, ensure mode/permissions parity is *not* over-tightened unless intended (Go uses `0644`); on Windows this is largely moot. Ensure the keys are top-level `token`/`server_id` so the existing viper-style reader finds them.