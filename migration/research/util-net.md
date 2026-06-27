I now have everything needed, including test files that pin down the exact `cstr.Mask` algorithm and hash outputs. Here is the behavioral specification.

---

# Behavioral Specification: `util-net` subsystem (`internal/util`)

## 1. Purpose

This subsystem is a grab-bag of low-level utility helpers used throughout the EDS consumer. The "util-net" slice covers: an HTTP request retry wrapper with jittered linear backoff (`http.go`); extraction of the API base URL from a JWT's issuer claim (`api.go`); masking of secrets in URLs, emails, JWTs, and CLI argument lists for safe logging (`mask.go`); content hashing/partitioning via xxHash64 and FNV-32a (`hash.go`); NATS message decoding across plain-JSON / gzip+JSON / msgpack encodings (`nats.go`); miscellaneous filesystem/network/path helpers and CockroachDB changefeed filename parsing (`util.go`); host/machine/system-info gathering (`sysinfo.go`); the running executable path (`process.go`); and Docker-container detection (`docker.go`). These are pure leaf utilities with no EDS-domain state; a faithful port must reproduce their exact output strings, constants, and edge behavior because other subsystems depend on byte-identical results (hashes used as keys, masked strings in logs, partition indices).

---

## 2. Public surface

All in Go package `util` (module `github.com/shopmonkeyus/eds`, path `internal/util`).

### http.go
```go
const defaultTimeout = time.Second * 30   // unexported

type HTTPRetry struct {           // exported type, all fields UNEXPORTED
    attempts int
    started  *time.Time
    timeout  time.Duration
    req      *http.Request
    logger   logger.Logger        // github.com/shopmonkeyus/go-common/logger
}

func (r *HTTPRetry) Do() (*http.Response, error)
func (r *HTTPRetry) shouldRetry(resp *http.Response, err error) bool  // unexported

type HTTPRetryOption func(*HTTPRetry)

func WithLogger(logger logger.Logger) HTTPRetryOption
func WithTimeout(dur time.Duration) HTTPRetryOption
func NewHTTPRetry(req *http.Request, opts ...HTTPRetryOption) *HTTPRetry
```

### api.go
```go
func GetAPIURLFromJWT(jwtString string) (string, error)
```

### mask.go
```go
func MaskURL(urlString string) (string, error)
func MaskEmail(val string) string
func MaskArguments(args []string) []string

// package-level compiled regexes (unexported):
var isURL   = regexp.MustCompile(`^(\w+)://`)
var isEmail = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
var isJWT   = regexp.MustCompile(`^[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+$`)
```

### hash.go
```go
func Hash(vals ...interface{}) string
func Modulo(value string, num int) int
```

### nats.go
```go
func DecodeNatsMsg(msg *nats.Msg, v interface{}) error
```

### util.go
```go
func JSONStringify(val any) string
func Exists(fn string) bool
func SliceContains(slice []string, val string) bool
func ToFileURI(dir string, file string) string
func IsLocalhost(url string) bool
func GetFreePort() (port int, err error)
func ListDir(dir string) ([]string, error)
func ParseCRDBExportFile(file string) (string, time.Time, bool)
func parsePreciseDate(dateStr string) (time.Time, error)   // unexported

var isWindowsDriveLetter = regexp.MustCompile(`^[a-zA-Z]:[/\\]`)
var crdbExportFileRegex  = regexp.MustCompile(`^(\d{33})-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz`)
```

### sysinfo.go
```go
type SystemInfo struct {
    Host      *host.InfoStat `json:"host"`        // github.com/shirou/gopsutil/v4/host
    NumCPU    int64          `json:"num_cpu"`
    GoVersion string         `json:"go_version"`
}

func GetSystemInfo() (*SystemInfo, error)
func GetMachineId() (string, error)
func GetLocalIP() (string, error)
```
`host.InfoStat` (gopsutil) serializes with these JSON tags: `hostname, uptime, bootTime, procs, os, platform, platformFamily, platformVersion, kernelVersion, kernelArch, virtualizationSystem, virtualizationRole, hostId`.

### process.go
```go
func GetExecutable() string
```

### docker.go
```go
func IsRunningInsideDocker() bool
```

---

## 3. Behavior & algorithms

### 3.1 HTTP retry (`http.go`)

**`NewHTTPRetry(req, opts...)`** — builds `HTTPRetry{req: req, timeout: 30s}` then applies options. `WithLogger` sets logger; `WithTimeout` overrides the 30s default.

**`Do()`** (recursive):
1. If `started == nil`, set `started = time.Now()` (captured once, on first call only — persists across retries).
2. `attempts++` (starts at 1 on first send).
3. `resp, err = http.DefaultClient.Do(r.req)` — uses the Go **default** HTTP client (no custom client/transport, so its own request timeout is whatever the default client uses = none by default).
4. If `shouldRetry(resp, err)` is true:
   - Compute jitter: `jitter = 100ms + (rand.Int63n(500 * attempts)) ms`.
     - Exact Go: `time.Duration(time.Millisecond*100 + time.Millisecond*time.Duration(rand.Int63n(int64(500*r.attempts))))`.
     - So jitter ∈ `[100ms, 100ms + 500*attempts ms)`. Upper bound grows linearly with attempt count (linear backoff with jitter). With `attempts=1`: [100, 600)ms; `attempts=2`: [100, 1100)ms; etc.
     - `rand.Int63n` uses the **math/rand global** generator (not crypto, not seeded here).
   - If logger set: `logger.Trace("%s request failed (path: %s) (status: %d), retrying request in %v", method, url, code, jitter)` where `code` is 0 when `resp == nil`.
   - `time.Sleep(jitter)` then `return r.Do()` (recurse).
5. Otherwise return `(resp, err)`.

**`shouldRetry(resp, err)`**:
- If `err != nil`: retry **only** if the error message **contains** the substring `"connection reset"` OR `"connection refused"`, AND `started.Add(timeout).After(time.Now())` is true (i.e., still inside the timeout window measured from `started`). For any other error type → returns false (no retry). The timeout window is the **only** bound on connection-error retries.
- If `resp != nil`: retry (return true) for these status codes only: `408 Request Timeout`, `502 Bad Gateway`, `503 Service Unavailable`, `504 Gateway Timeout`, `429 Too Many Requests`. Before returning true it **drains and closes** the body: `io.Copy(io.Discard, resp.Body); resp.Body.Close()`.
- Else return false.

**CRITICAL GOTCHA:** Status-code retries have **no timeout and no max-attempt cap**. A server stuck returning 503/429/etc. causes an **infinite retry loop** (only connection-reset/refused errors are time-bounded). The 30s timeout never gates HTTP-status retries.

### 3.2 API URL from JWT (`api.go`)

`GetAPIURLFromJWT(jwtString)`:
1. `p := jwt.NewParser(jwt.WithoutClaimsValidation())` (golang-jwt v5). No signature verification, no expiry/claims validation.
2. `tokens, _, err := p.ParseUnverified(jwtString, &jwt.RegisteredClaims{})` — splits on `.`, base64url-decodes header+payload, JSON-unmarshals payload into `RegisteredClaims`. Errors → `"failed to parse jwt: %w"`.
3. `iss, err := tokens.Claims.GetIssuer()` — reads the `iss` claim (in jwt v5 this never errors; returns empty string if absent). Error path wraps `"failed to get issuer from jwt: %w"`.
4. **Legacy rewrite:** if `iss == "https://shopmonkey.io"` → return `"https://api.shopmonkey.cloud"`.
5. Otherwise return `iss` verbatim. (Test: a token with `iss=http://localhost:3101` returns `http://localhost:3101`.)

### 3.3 Masking (`mask.go`)

The masking primitive is `cstr.Mask` from `github.com/shopmonkeyus/go-common/string`. Its exact contract, **derived from the unit tests**, is:

> `Mask(s)`: let `n = len(s)` (byte length), `shown = n / 2` (integer floor). Return the first `shown` bytes of `s` followed by `(n - shown)` `*` characters.
> - `n=0 → ""`; `n=1 → "*"`; `"FOO"(3) → "F**"`; `"user"(4) → "us**"`; `"password"(8) → "pass****"`; `"example"(7) → "exa****"`; `"thisisapassword"(15) → "thisisa********"`; `"us-west-2"(9) → "us-w*****"`.

**`MaskURL(urlString)`**:
1. `url.Parse`; on error → `"failed to parse URL: %w"`.
2. Build output: `scheme + "://"`.
3. If userinfo present: write `Mask(username)`; if a password is set, write `":" + Mask(password)`; then `"@"`.
4. Write `u.Host` **verbatim** (includes port; never masked).
5. Path: let `p = u.Path`. If `p != "/" && p != ""`: write `"/"`, then if `len(p) > 1 && p[0] == '/'` write `Mask(p[1:])` — note the **entire** path after the first slash (including embedded `/`) is masked as one string (e.g. `/TEST/PUBLIC` → `/TEST/******`).
6. Query: for each key, build `fmt.Sprintf("%s=%s", k, Mask(strings.Join(values, ",")))` (multi-valued params joined with `,` before masking). Collect into a slice, **`sort.Strings`** it (lexicographic sort of the whole `key=maskedvalue` strings → deterministic ordering), then if non-empty join with `"&"` prefixed by `"?"`.
7. Return assembled string.

Verified outputs (tests, verbatim):
- `http://user:password@localhost:8080/path?query=1` → `http://us**:pass****@localhost:8080/pa**?query=*`
- `snowflake://FOO:thisisapassword@TFLXCJY-LU41011/TEST/PUBLIC` → `snowflake://F**:thisisa********@TFLXCJY-LU41011/TEST/******`
- `s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` → `s3://bucket/fol***?access-key-id=AKIAIOSFOD**********&region=us-w*****&secret-access-key=wJalrXUtnFEMI/K7MDEN********************`
- A query key with no value (`secret-access`) becomes `secret-access=` (empty value → `Mask("")=""`).
- `http://example.com` → `http://example.com` (no user/path/query → unchanged).

**`MaskEmail(val)`**: `tok = Split(val, "@")`; `dot = Split(tok[1], ".")`; return `Mask(tok[0]) + "@" + Mask(dot[0]) + "." + strings.Join(dot[1:], ".")`. The TLD (everything after the first domain label dot) is left **unmasked**. Examples: `user@example.com → us**@exa****.com`; `another.user@example.com → anothe******@exa****.com`. **Panics** if no `@` (no `tok[1]`).

**`MaskArguments(args)`**: returns a new slice; for each arg, check in this order:
1. `isURL` matches (`^(\w+)://`) → `MaskURL(arg)`; if MaskURL errors, fall back to `Mask(arg)`.
2. else `isEmail` matches → `MaskEmail(arg)`.
3. else `isJWT` matches (`^seg.seg.seg$` of `[A-Za-z0-9-_]`) → `Mask(arg)` (whole token, first-half shown).
4. else → arg unchanged.

JWT example masked to first-half-shown + asterisks for the remainder.

### 3.4 Hashing (`hash.go`)

**`Hash(vals...)`**:
1. `h := xxhash.New()` — **XXH64** (cespare/xxhash/v2, seed 0).
2. For each `v`: `h.Write([]byte(fmt.Sprintf("%+v", v)))` (via `gstr.S2B`, a zero-copy unsafe string→[]byte; behaviorally identical to `[]byte(...)`). Values are concatenated into one running hash (streaming == hashing the concatenation).
3. Return `fmt.Sprintf("%x", h.Sum(nil))` — `Sum(nil)` is the 8-byte **big-endian** digest; `%x` produces a fixed **16-char lowercase hex** string (zero-padded per byte).

Go `%+v` formatting matters and must be reproduced for the actual types passed:
- string → the string itself; int `42` → `"42"`; bool `true`/`false`; `nil` → `"<nil>"`; structs → `{Field:val ...}` (with field names).

Verified (tests):
| Input | Output |
|---|---|
| `()` (empty) | `ef46db3751d8e999` |
| `("hello")` | `26c7827d889f6da3` |
| `("hello", 42, true)` | `d481b75d0fa4abff` |
| `("hello", 42, true, nil)` | `a668199a6b3fc355` |
| `(nil)` | `7c5b4e400f80bf7c` |

**`Modulo(value, num)`**:
1. `hasher := fnv.New32a()` — **FNV-1a 32-bit** (offset basis `2166136261`, prime `16777619`).
2. `hasher.Write([]byte(value))`.
3. `partition := int(hasher.Sum32()) % num` — `Sum32()` is `uint32`; `int(...)` on a 64-bit platform is a 64-bit widening (always non-negative, 0..4294967295), then `% num`.
4. `if partition < 0 { partition = -partition }` — **dead code on 64-bit** Go (only reachable on 32-bit `int` platforms where the uint32→int conversion can wrap negative). For a faithful 64-bit port, compute `(int)((long)(uint)hash % num)`; result is always in `[0, num)`.

Verified: `Modulo("",10)=1` (FNV offset 2166136261 % 10 = 1), `("1",1)=0`, `("1",2)=0`, `("1",3)=1`, `("1 2 3 4",10)=5`.

### 3.5 NATS message decode (`nats.go`)

`DecodeNatsMsg(msg, v)`:
1. `encoding = msg.Header.Get("content-encoding")`.
2. `gzipped = (encoding == "gzip/json")`; `msgpacked = (encoding == "msgpack")`. `data = msg.Data`.
3. If gzipped → `data = compress.Gunzip(data)` (standard gzip; test compresses with stdlib `compress/gzip`).
4. Else if msgpacked → `msgpack.Unmarshal(data, &o)` into a generic `any`, then `data = json.Marshal(o)` (msgpack → object → **re-encoded to JSON**).
5. If any error so far → return it.
6. `json.Unmarshal(data, v)` → final decode into the caller's target.
7. Default case (no/unknown encoding): `data` stays raw and is JSON-unmarshaled directly.

Note the msgpack path double-converts (msgpack → `any` → JSON → target), which routes all numbers/types through JSON semantics.

### 3.6 Misc helpers (`util.go`)

- **`JSONStringify(val)`**: `json.Marshal`, **ignores error**, returns `string(buf)`; on marshal failure returns `""`.
- **`Exists(fn)`**: `os.Stat`; returns false **only** on `os.IsNotExist`; returns **true** for any other error (e.g. permission denied) or success.
- **`SliceContains`**: linear scan, exact `==`.
- **`ToFileURI(dir, file)`**:
  1. If `dir` is not absolute **and** not a Windows drive letter (`^[a-zA-Z]:[/\\]`), make it absolute via `filepath.Abs`.
  2. `absDir = filepath.Clean(dir)`.
  3. If `os.PathSeparator == '\\'` (Windows): `"file://" + path.Join(filepath.ToSlash(absDir), file)`.
  4. Else: `"file://" + path.Join(absDir, file)`.
  - Tests: unix `/var/.../dir` + `*.ndjson.gz` → `file:///var/.../dir/*.ndjson.gz` (three slashes because abs path starts with `/`); trailing slash on dir collapses identically; `c:/foo/bar` → `file://c:/foo/bar/*.ndjson.gz` (two slashes — drive-letter path has no leading slash).
- **`IsLocalhost(url)`**: substring contains `"localhost"` OR `"127.0.0.1"` OR `"0.0.0.0"`.
- **`GetFreePort()`**: `net.ResolveTCPAddr("tcp","localhost:0")` → `net.ListenTCP` → return `l.Addr().(*net.TCPAddr).Port`, `defer l.Close()`. Asks OS for an ephemeral port (TOCTOU: port freed on close before reuse).
- **`ListDir(dir)`**: recursive `os.ReadDir`; recurses into subdirectories; **skips files named exactly `.DS_Store`**; returns full joined paths (`filepath.Join`). Returns files only (no dir entries). Errors propagate from any level.
- **`parsePreciseDate(dateStr)`** (33-digit string): `trimmed = dateStr[:14] + "." + dateStr[14:23]`; parse with Go layout `"20060102150405.999999999"`. Uses digits 0–13 as `YYYYMMDDHHMMSS`, 14–22 as 9-digit fractional seconds (nanoseconds); digits 23–32 ignored. Test: `202407242003015854988560000000000` → `2024-07-24T20:03:01.585498856Z`.
- **`ParseCRDBExportFile(file)`**: `filename = filepath.Base(file)`; match against `^(\d{33})-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz`. If no match → `("", time.Time{}, false)`. Group 1 = 33-digit timestamp → `parsePreciseDate`; group 2 = **table name**; group 3 = schema/uniquer suffix. Returns `(tableName, parsedTime, true)`. On date-parse error → `("", time.Time{}, false)`. Examples: `...-user-2.ndjson.gz` → table `user`; `...-labor_rate-2.ndjson.gz` → table `labor_rate`; `...-user-14a.ndjson.gz` → table `user`. (Comment references CockroachDB changefeed file format `/[date]/[timestamp]-[uniquer]-[topic]-[schema-id]`.)

### 3.7 System info (`sysinfo.go`)

- **`GetSystemInfo()`**: `Host = host.Info()` (gopsutil; errors propagate); `NumCPU = int64(runtime.NumCPU())`; `GoVersion = runtime.Version()[2:]` — strips the leading `"go"` (e.g. `"go1.24.0"` → `"1.24.0"`).
- **`GetMachineId()`**: `machineid.ProtectedID("eds")` — reads the OS machine GUID and returns `hex(HMAC-SHA256(key=machineID, msg="eds"))`. Machine-ID source is OS-specific: Linux `/etc/machine-id` (or `/var/lib/dbus/machine-id`), Windows registry `HKLM\SOFTWARE\Microsoft\Cryptography\MachineGuid`, macOS `IOPlatformUUID`.
- **`GetLocalIP()`**: iterate `net.InterfaceAddrs()`; return the **first** address that is an `*net.IPNet`, is **not loopback**, **is private** (RFC1918: 10/8, 172.16/12, 192.168/16 for IPv4), and **is IPv4** (`To4() != nil`), formatted as dotted-quad. If none → error `"no private IP found"`.

### 3.8 Process / Docker

- **`GetExecutable()`**: `os.Executable()`; on error fall back to `os.Args[0]`.
- **`IsRunningInsideDocker()`**: return true if `/.dockerenv` exists; else if `/proc/1/cgroup` exists, read it and return true if its trimmed contents **contain** `"docker"` OR `"lxc"` OR `"rt"`. Otherwise false. (Linux-only paths; on Windows always returns false because neither path exists.)

---

## 4. External dependencies

| Go package | Role | .NET / C# equivalent |
|---|---|---|
| `net/http`, `http.DefaultClient` | HTTP send in retry wrapper | `System.Net.Http.HttpClient` (use a single shared instance; see gotchas re: cloning requests) |
| `math/rand` (`rand.Int63n`) | Jitter for backoff | `System.Random.Shared.NextInt64(max)` (.NET 6+) |
| `time` | Durations, timeout window | `TimeSpan`, `DateTime/Stopwatch`, `Task.Delay` |
| `github.com/shopmonkeyus/go-common/logger` | Trace logging in retry | Project's `ILogger` abstraction (e.g. `Microsoft.Extensions.Logging.ILogger`, `LogTrace`) |
| `github.com/golang-jwt/jwt/v5` | Unverified JWT parse, issuer claim | Manual base64url-decode of middle segment + `System.Text.Json` to read `iss` (simplest/faithful), or `System.IdentityModel.Tokens.Jwt` `JwtSecurityTokenHandler.ReadJwtToken` |
| `net/url` | URL parse for masking | `System.Uri` (with care for non-http schemes) |
| `regexp` | URL/email/JWT/filename/drive-letter detection | `System.Text.RegularExpressions.Regex` (compiled, `RegexOptions.Compiled`) |
| `sort`, `strings` | Query sort, joins, contains | `Array.Sort`/`List.Sort` (ordinal), `string.Join`, `string.Contains` |
| `github.com/shopmonkeyus/go-common/string` (`cstr.Mask`) | First-half-shown masking primitive | Custom helper (see §3.3 contract) |
| `github.com/cespare/xxhash/v2` | XXH64 content hash | `System.IO.Hashing.XxHash64` (NuGet `System.IO.Hashing`) |
| `hash/fnv` (`New32a`) | FNV-1a 32-bit for partitioning | Implement manually (offset `2166136261`, prime `16777619`); no BCL FNV |
| `github.com/savsgio/gotils/strconv` (`S2B`) | Zero-copy string→[]byte | `System.Text.Encoding.UTF8.GetBytes` (UTF-8) |
| `encoding/json` | JSON (en/de)code | `System.Text.Json` |
| `github.com/nats-io/nats.go` | NATS `*Msg` (Header, Data) | `NATS.Client` (NuGet `NATS.Net` / `NATS.Client`) |
| `github.com/shopmonkeyus/go-common/compress` (`Gunzip`) | gzip decompress | `System.IO.Compression.GZipStream` (decompress) |
| `github.com/vmihailenco/msgpack/v5` | msgpack decode | `MessagePack` (NuGet `MessagePack`) or `MessagePack-CSharp` |
| `github.com/shirou/gopsutil/v4/host` | Host/OS info | `System.Runtime.InteropServices.RuntimeInformation`, `System.Environment`, WMI/`System.Management` for richer fields |
| `github.com/denisbrodbeck/machineid` | Stable machine ID | Read same OS sources manually (registry `MachineGuid` on Windows, `/etc/machine-id` on Linux, `IOPlatformUUID` on macOS) + HMAC-SHA256 (`System.Security.Cryptography.HMACSHA256`) |
| `runtime` (`NumCPU`, `Version`) | CPU count, Go version | `Environment.ProcessorCount`; runtime/framework version string |
| `net` (InterfaceAddrs, TCPListener, IPNet) | Local IP, free port | `System.Net.NetworkInformation.NetworkInterface`, `System.Net.Sockets.TcpListener` |
| `os`, `path`, `path/filepath` | FS stat/read, path ops | `System.IO.File/Directory`, `System.IO.Path` |

---

## 5. Edge cases & gotchas

- **Infinite HTTP retries on 5xx/429/408**: status-code retries are *not* time-bounded and have no max-attempt cap. Only `connection reset`/`connection refused` errors are bounded by the timeout window. A persistently failing endpoint loops forever. Decide whether to preserve this (faithful) or add a cap — preserve unless told otherwise.
- **Connection-error detection is substring matching** on `err.Error()` for the literal strings `"connection reset"` and `"connection refused"`. .NET surfaces these as `SocketException`/`HttpRequestException` with different messages — you must map `SocketError.ConnectionReset` / `SocketError.ConnectionRefused` (and inner exceptions) to these conditions, not match English text.
- **`HTTPRetry.Do` is recursive** and reuses the same `*http.Request`. In .NET an `HttpRequestMessage` cannot be re-sent; you must **clone** the request (method, URI, headers, and a re-readable body) per attempt, and convert the recursion to a loop to avoid unbounded stack growth.
- **`started` captured once**: the timeout window is measured from the first attempt, including time spent sleeping in jitter — not per-attempt.
- **`shouldRetry` drains/closes the response body** for retryable statuses; for non-retried responses the caller owns the body. Mirror disposal carefully in .NET (`HttpResponseMessage` disposal).
- **Mask is byte-length based** (`len(s)`, byte slicing). For non-ASCII input the split can land mid-rune; reproduce with byte semantics (operate on UTF-8 bytes) if exact parity on non-ASCII matters. All test data is ASCII.
- **`MaskEmail` panics** on input without `@` (indexes `tok[1]`) and produces a trailing `.` if the domain has no dot. It is safe only when reached via `MaskArguments` (gated by `isEmail`). Guard the public method in C#.
- **`MaskURL` query ordering** is deterministic via `sort.Strings` on full `key=value` strings (ordinal/byte sort). Use ordinal (not culture-aware) sorting in C#.
- **`Uri` scheme parsing differences**: Go's `url.Parse` happily parses `snowflake://`, `s3://`, `mysql://` with userinfo/host/path/query. .NET `System.Uri` may parse authority/userinfo differently for non-registered schemes; verify against the test vectors in §3.3 or hand-roll a parser.
- **`Hash` relies on Go `%+v` formatting** of arbitrary values; the byte stream (and thus the hash) depends on Go's formatting rules (`<nil>` for nil, `{Field:val}` for structs, decimal ints, `true/false`). The C# formatter must produce byte-identical strings for the value types actually hashed, or hashes used as keys will diverge.
- **xxHash output endianness**: `Sum(nil)` is big-endian; `%x` yields a fixed 16-char zero-padded lowercase hex. In C#, `XxHash64.HashToUInt64(bytes).ToString("x16")` matches (e.g. `0xef46db3751d8e999` → `"ef46db3751d8e999"`).
- **`Modulo` negative-branch is dead on 64-bit**; ensure your C# implementation never produces a negative index. Use `(int)((long)(uint)fnvHash % num)`. `num <= 0` would divide-by-zero/panic (Go panics on `% 0`); guard if needed.
- **`Exists` returns true on non-IsNotExist errors** (e.g. permission). Don't simplify to `File.Exists` (which returns false on access errors) without considering this — faithful behavior is "not provably absent".
- **`ToFileURI`** branches on `os.PathSeparator` (Windows vs not) and emits a different number of leading slashes for drive-letter vs rooted paths. Replicate per-OS; do not rely on `new Uri(path).AbsoluteUri` which normalizes differently (`file:///C:/...`).
- **`ListDir` skips `.DS_Store`** (macOS) and recurses; preserve the skip.
- **`GetLocalIP`** returns the *first* matching IPv4 private, non-loopback address; interface enumeration order is OS-dependent and not guaranteed stable. Note Go's `IsPrivate` semantics (RFC1918 for v4) when filtering in .NET.
- **`GetMachineId`** must read the *same* OS identity source and compute `hex(HMAC-SHA256(key=machineId, msg="eds"))` to produce identical IDs across the Go→C# migration (important if the ID is persisted/used as a stable consumer identity). The HMAC key is the machine id; the message is the literal `"eds"`.
- **`IsRunningInsideDocker`** is Linux-centric (`/.dockerenv`, `/proc/1/cgroup`) and returns false on Windows/macOS. The cgroup match includes the short token `"rt"` which is unusually broad (any cgroup line containing `rt`); preserve it exactly.
- **NATS msgpack path** round-trips through JSON, so number typing follows JSON rules (everything via `any`→JSON). In C# replicate the two-step (msgpack→object→JSON→target) if exact type coercion parity is required; a direct msgpack→target deserialize could differ.
- **`GetFreePort`** has an inherent TOCTOU race (port closed before reuse); identical in any language.
- **`runtime.Version()[2:]`** assumes a `go`-prefixed version; for non-standard version strings (`devel ...`) the slice is naive. Map to your .NET equivalent runtime/version string.

---

## 6. C# port notes

- **HTTP retry**: implement as `async Task<HttpResponseMessage> SendWithRetryAsync(...)` using a `while(true)` loop (not recursion). Track `attempts` and a `Stopwatch`/`startTimestamp` captured before the first send. For each iteration **clone** the `HttpRequestMessage` (helper that copies method, URI, headers, and buffers/clones the content). Map connection errors via catching `HttpRequestException`/`SocketException` and inspecting `SocketError` (`ConnectionReset`, `ConnectionRefused`) instead of string matching. Retryable status set: `RequestTimeout(408)`, `BadGateway(502)`, `ServiceUnavailable(503)`, `GatewayTimeout(504)`, `TooManyRequests(429)` — dispose the response before retrying. Jitter: `TimeSpan.FromMilliseconds(100 + Random.Shared.NextInt64(500 * attempts))`; `await Task.Delay(jitter)`. Default timeout `TimeSpan.FromSeconds(30)`; expose `WithLogger`/`WithTimeout` via an options object or builder. Preserve the no-cap-on-status-retries behavior unless product requirements say otherwise (flag it in code comments). Consider using a single shared `HttpClient` to mirror `http.DefaultClient`.
- **Mask**: implement the primitive exactly: `static string Mask(string s){ int shown = s.Length/2; return s.Substring(0,shown) + new string('*', s.Length - shown); }` (byte-accurate for ASCII; switch to UTF-8 byte handling only if non-ASCII parity is needed). Build `MaskUrl` manually rather than relying on `Uri` normalization: parse scheme, userinfo (split on first `:`), host (with port verbatim), path, and query; sort query entries with `StringComparer.Ordinal`; join multi-values with `,` before masking. Precompile the three regexes as `static readonly Regex` with the **exact** patterns. Keep the check order URL → email → JWT → passthrough in `MaskArguments`.
- **Hashing**: use `System.IO.Hashing.XxHash64`. Build the input by concatenating UTF-8 bytes of each value's Go-`%+v`-equivalent string; format result with `.ToString("x16")`. Centralize a `GoFormat(object?)` helper covering the value types actually passed (null→`"<nil>"`, bool→`"true"/"false"`, integers→invariant decimal, strings verbatim). For `Modulo`, implement FNV-1a 32-bit manually and compute `(int)((long)(uint)hash % num)`.
- **JWT**: prefer manual parsing for fidelity and zero dependencies: split on `.`, base64url-decode segment[1] (pad to multiple of 4, replace `-`/`_`), `JsonDocument.Parse`, read `iss`. Apply the legacy rewrite `https://shopmonkey.io` → `https://api.shopmonkey.cloud`. Return wrapped errors mirroring the two failure messages.
- **NATS decode**: read `content-encoding` header; `"gzip/json"` → `GZipStream` decompress then `JsonSerializer.Deserialize`; `"msgpack"` → MessagePack deserialize to a generic object, re-serialize to JSON, then deserialize to target (to faithfully match the Go round-trip), or carefully validate that a direct MessagePack→T path yields equivalent results; default → JSON deserialize raw.
- **System info / machine id**: wrap platform-specific code with `RuntimeInformation.IsOSPlatform`. For `GetMachineId`, read registry `MachineGuid` (Windows), `/etc/machine-id` (Linux), `IOPlatformUUID` (macOS), then `HMACSHA256` with **key = machine id bytes**, **message = "eds"**, hex-encode lowercase — this guarantees ID continuity with the Go consumer. `GoVersion` has no direct analog; substitute the .NET runtime version.
- **Filesystem helpers**: `Exists` → `File.Exists(p) || Directory.Exists(p)` is *not* equivalent on access errors; if exact parity matters, replicate "false only when provably not-found". `ToFileURI` → branch on `Path.DirectorySeparatorChar` and build the string manually to reproduce the leading-slash count. `ListDir` → recursive enumeration skipping `.DS_Store`. `ParseCRDBExportFile`/`parsePreciseDate` → port regexes verbatim and parse the timestamp by taking chars [0,14) + "." + [14,23) with `DateTime.ParseExact(..., "yyyyMMddHHmmss.fffffffff?", ...)` (note .NET caps fractional precision at 7 digits / 100ns ticks — 9-digit nanosecond parsing requires custom handling to avoid precision loss; parse the 9 fractional digits manually and add as ticks). This nanosecond-vs-tick mismatch is the main porting risk in this file.
- **General risks**: ordinal string comparisons everywhere (avoid culture-sensitive defaults); regex semantics are compatible but anchor/charclass behavior should be tested against the provided vectors; the nanosecond timestamp precision (Go `time.Time` = ns) vs .NET `DateTime`/`DateTimeOffset` (100ns ticks) is a lossy boundary — use `long` ticks math or a custom struct if sub-100ns ordering ever matters (the CRDB filenames carry sub-tick precision in trailing digits that Go already discards, so 7-digit tick precision is sufficient for the parsed portion).

All file paths referenced are under `D:/Users/kessler/source/Repos/edsGolang/internal/util/` (`http.go`, `api.go`, `mask.go`, `hash.go`, `nats.go`, `util.go`, `sysinfo.go`, `process.go`, `docker.go`) with behavior cross-validated against the sibling test files (`mask_test.go`, `hash_test.go`, `api_test.go`, `nats_test.go`, `util_test.go`).