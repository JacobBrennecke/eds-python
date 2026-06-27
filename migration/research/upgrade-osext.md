I have read all assigned files in full plus the relevant call sites and helpers. Here is the behavioral specification.

# Behavioral Specification: `upgrade` + `osext` subsystems

## 1. Purpose

These two `internal` packages implement EDS's in-place self-upgrade capability. `internal/upgrade` does two distinct jobs: (a) `Upgrade()` downloads a release archive from a URL, cryptographically verifies it with a PGP signature, extracts the platform binary, and writes it to a target file; and (b) `Apply()`/`commitBinary()` atomically swap a freshly-downloaded binary into the place of the currently-running executable, with rollback-on-failure semantics and Windows-specific handling for the fact that a running `.exe` cannot be deleted/overwritten (it is hidden instead of removed). `internal/osext` is a vendored copy of the classic Go "executable path" library that resolves the absolute path of the running process across OSes. Note: `osext` is effectively **dead code** in this repo — nothing imports it; the live code obtains the executable path via `util.GetExecutable()` (which wraps the stdlib `os.Executable()`). It is included here for completeness and because a faithful port may want the same resolution semantics.

The end-to-end flow (from `cmd/server.go` and `cmd/download.go`): on an upgrade notification the server spawns a child `eds download <version> <file>` process (which calls `Upgrade()`), validates the downloaded binary's reported version, then calls `Apply(currentExe, downloadedFile)` to swap it in, then triggers a restart via an HTTP `GET /restart` to the parent supervisor.

---

## 2. Public surface

### Package `upgrade` (`internal/upgrade`)

#### Type `UpgradeConfig` (upgrade.go)
```go
type UpgradeConfig struct {
    Logger       logger.Logger    // github.com/shopmonkeyus/go-common/logger
    Context      context.Context
    BinaryURL    string
    SignatureURL string
    Filename     string           // output path the extracted binary is written to
    PublicKey    string           // ASCII-armored PGP public key
}
```
No struct tags (not serialized).

#### Func `Upgrade` (upgrade.go)
```go
func Upgrade(config UpgradeConfig) error
```
Downloads, verifies, extracts, and writes the binary. See §3.

#### Func `Apply` (apply.go)
```go
func Apply(targetPath string, sourcePath string) error
```
Swaps `sourcePath`'s contents into `targetPath` (the running executable). See §3.

#### Func `RollbackError` (apply.go)
```go
func RollbackError(err error) error
```
Given an error returned by `Apply`, returns the rollback failure error if the original error is a `*rollbackErr`; otherwise returns `nil` (including when `err == nil`). Callers MUST call this on any non-nil `Apply` error to distinguish a recoverable failure (old binary still in place) from a catastrophic one (no binary at the original path).

#### Unexported (but behaviorally important)
- `func prepareAndCheckBinary(targetPath, sourcePath string) error` — copies source into `.<filename>.new`.
- `func commitBinary(targetPath string) error` — the rename/rollback dance.
- `type rollbackErr struct { error; rollbackErr error }` — embeds the original error (so it satisfies `error` via the embedded field) and carries the rollback error separately.
- `func hideFile(path string) error` — platform-specific (see hide_windows.go / hide_noop.go).

### Package `osext` (`internal/osext`)

#### Func `Executable` (osext.go)
```go
func Executable() (string, error)
```
Returns `filepath.Clean(executable())` — an absolute path usable to re-invoke the current program. The returned error is whatever `executable()` returned (note: `Clean` is applied to the path even on error).

#### Func `ExecutableFolder` (osext.go)
```go
func ExecutableFolder() (string, error)
```
Returns `filepath.Dir(Executable())`. On error from `Executable`, returns `("", err)`.

#### Unexported per-OS `executable()` implementations
- `osext_windows.go`: package-level `var kernel = syscall.MustLoadDLL("kernel32.dll")` and `var getModuleFileNameProc = kernel.MustFindProc("GetModuleFileNameW")`; `func executable() (string, error)` → `getModuleFileName()`.
- `osext_procfs.go`: build-tagged `linux || netbsd || openbsd || solaris || dragonfly || android`.
- `osext_sysctl.go`: build-tagged `darwin || freebsd`; also `var initCwd, initCwdErr = os.Getwd()` and `func getAbs(execPath string) (string, error)`.
- `osext_plan9.go`: plan9 (reads `/proc/<pid>/text`).

---

## 3. Behavior & algorithms

### 3.1 `Upgrade(config)` (upgrade.go)

Step-by-step:

1. Record `started := time.Now()`; a deferred closure logs `Debug("download took %s", time.Since(started))` on return.
2. Create a temp file: `os.CreateTemp("", "eds")` (pattern `"eds"`, default temp dir). On error: `"error creating temp file: %w"`. Defer `os.Remove(tmp.Name())`. Log `Trace("created temp file %s to download archive", tmp.Name())`.
3. Parse the armored public key: `crypto.NewKeyFromArmored(config.PublicKey)`. On error: `"error reading public key: %w"`.
4. Build a PGP verifier: `pgp := crypto.PGP(); verifier, err := pgp.Verify().VerificationKey(publicKey).New()`. On error: `"error creating PGP verifier: %w"`. Log `Trace("created PGP verifier using public key")`.
5. Download the binary:
   - `http.NewRequest("GET", config.BinaryURL, nil)` → on error `"error creating HTTP request: %w"`.
   - Execute with retry: `util.NewHTTPRetry(req.WithContext(config.Context), util.WithLogger(config.Logger)).Do()` → on error `"error downloading binary from %s: %w"`. Defer `resp.Body.Close()`.
   - `binaryLen, err := io.Copy(tmp, resp.Body)` → on error `"error copying binary data: %w"`.
   - Explicitly `resp.Body.Close()` and `tmp.Close()`. Log `Debug("downloaded binary of size %d bytes from %s", binaryLen, config.BinaryURL)`.
6. Download the signature (same retry pattern):
   - `http.NewRequest("GET", config.SignatureURL, nil)` → `"error creating HTTP request: %w"`.
   - `NewHTTPRetry(...).Do()` → `"error downloading signature from %s: %w"`. Defer close.
   - `signature, err := io.ReadAll(sresp.Body)` → `"error reading signature: %w"`. Explicitly close. Log `Debug("downloaded signature of size %d bytes from %s", len(signature), config.SignatureURL)`.
7. Verify the signature:
   - Reopen temp file `os.Open(tmp.Name())` → `"error opening file %s: %w"`. Defer close.
   - `reader, err := verifier.VerifyingReader(of, bytes.NewReader(signature), crypto.Auto)` → `"error verifying signature data: %w"`. (`crypto.Auto` = auto-detect armored vs binary signature encoding.)
   - `verifyResult, err := reader.ReadAllAndVerifySignature()` → `"error verifying signature: %w"`. **This reads the entire binary to compute the verification.**
   - `if sigErr := verifyResult.SignatureError(); sigErr != nil` → `"error in signature verification: %w"`. Log `Debug("verified signature of binary")`. Close `of`.
8. Create the output file: `os.Create(config.Filename)` → `"error creating file %s: %w"`. Defer close. (`os.Create` truncates/creates with mode 0666 before umask.)
9. Extract the binary — branch on `filepath.Ext(config.BinaryURL) == ".zip"`:
   - **ZIP branch** (Windows releases): log `Debug("extracting zip file: %s", tmp.Name())`. `zip.OpenReader(tmp.Name())` → `"error opening zip file: %w"`. Defer close. Iterate `uz.File`; log `Trace("zip file: %s", f.Name)` per entry. For the **first** entry whose `filepath.Ext(f.Name) == ".exe"`: open (`"error opening file in archive: %w"`), `io.Copy(of, af)` (`"error copying file from archive: %w"`), close the archive entry, and **`return nil` immediately** (skips the chmod step below). If no `.exe` entry exists, the loop finishes and execution falls through to the chmod step (the output file is left empty / zero bytes).
   - **TAR.GZ branch** (non-Windows releases): log `Debug("extracting tar.gz file: %s", tmp.Name())`. Reopen temp (`os.Open`, `"error opening file %s: %w"`). `gzip.NewReader(tmpf)` → `"error creating gzip reader: %w"`. Defer close. `tr := tar.NewReader(gz)`. Loop `tr.Next()`: **any** error (including `io.EOF`) returns `"error reading tar header: %w"`. Log `Trace("tar file: %s", header.Name)`. For the entry whose `header.Name == "eds"` (exact match, the canonical binary name): `io.Copy(of, tr)` (`"error copying file from archive: %w"`), then `break`. After the loop, `gz.Close()` and `tmpf.Close()`.

     **Gotcha:** if no entry is named exactly `"eds"`, the loop never breaks and eventually `tr.Next()` returns `io.EOF`, which is returned wrapped as `"error reading tar header: EOF"`. So a missing `eds` entry surfaces as a tar-header read error, not a "not found" error.
10. After tar branch (and after a zip with no `.exe`): log `Trace("setting permissions on file %s", config.Filename)` and `os.Chmod(config.Filename, 0755)` → `"error setting permissions on file: %w"`. Return `nil`.

**Net output formats:** Windows = `.zip` containing an `*.exe`; all others = `.tar.gz` containing a file literally named `eds`. The chosen archive format is driven solely by the **URL extension** (`.zip` vs anything else), not by the runtime OS.

### 3.2 `Apply(targetPath, sourcePath)` (apply.go)

```
Apply = prepareAndCheckBinary(targetPath, sourcePath); if err return err; commitBinary(targetPath)
```

#### `prepareAndCheckBinary(targetPath, sourcePath)`
1. `source, err := os.Open(sourcePath)` → `"failed to open source file: %w"`. (Note: `source` is never explicitly closed — relies on GC/process exit. A defer is NOT present.)
2. `updateDir := filepath.Dir(targetPath)`, `filename := filepath.Base(targetPath)`.
3. `newPath := filepath.Join(updateDir, fmt.Sprintf(".%s.new", filename))` — i.e. a hidden sibling named `.<basename>.new`.
4. `fp, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)` → returns raw err. Defer `fp.Close()`.
5. `io.Copy(fp, source)` → returns raw err.
6. **Explicit `fp.Close()`** before returning (comment: "if we don't call fp.Close(), windows won't let us move the new executable because the file will still be 'in use'"). The deferred close also runs but is a harmless double-close.
7. Return `nil`.

#### `commitBinary(targetPath)`
1. Compute `updateDir`, `filename`, `newPath` = `.<filename>.new`, `oldPath` = `.<filename>.old`.
2. `_ = os.Remove(oldPath)` — best-effort delete of any leftover `.old` (error ignored). Comment notes this is needed on Windows because (a) a prior successful update can't delete `.old` while the process runs, and (b) Windows rename fails if the destination already exists.
3. `err := os.Rename(targetPath, oldPath)` — move the running binary aside. On error: return that error directly (no rollback needed; original still in place).
4. `err = os.Rename(newPath, targetPath)` — move the new binary into place.
   - **On error (failure):** the filesystem is now in a bad state (no file at original path). Attempt rollback: `rerr := os.Rename(oldPath, targetPath)`.
     - If `rerr != nil`: return `&rollbackErr{err, rerr}` (catastrophic — caller's `RollbackError` will be non-nil).
     - Else: return `err` (the original rename error; old binary restored, recoverable).
5. **On success:** `errRemove := os.Remove(oldPath)`. If `errRemove != nil` (typical on Windows, where the old binary may still be locked/in-use): `_ = hideFile(oldPath)` (best effort, error ignored). Return `nil`.

**Ordering is critical and must be preserved exactly:** remove-old → rename target→old → rename new→target → (rollback on failure) → remove/hide old on success.

### 3.3 `RollbackError(err)` (apply.go)
- `err == nil` → return `nil`.
- Type-assert `err.(*rollbackErr)`; if ok → return `rerr.rollbackErr`.
- Else → return `nil`.

### 3.4 `hideFile` (hide_windows.go / hide_noop.go)
- **Non-Windows** (`//go:build !windows`): `func hideFile(_ string) error { return nil }`.
- **Windows** (file `hide_windows.go`, selected by the `_windows.go` filename suffix — no explicit build tag):
  ```go
  kernel32 := syscall.NewLazyDLL("kernel32.dll")
  setFileAttributes := kernel32.NewProc("SetFileAttributesW")
  r1, _, err := setFileAttributes.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))), 2)
  if r1 == 0 { return err } else { return nil }
  ```
  - The magic constant `2` is `FILE_ATTRIBUTE_HIDDEN`. The call sets the hidden attribute (replacing all other attributes, since it passes the attribute set as exactly `2`).
  - `SetFileAttributesW` returns nonzero on success; `r1 == 0` means failure → return the OS error.
  - Uses a fresh `NewLazyDLL` each call (not cached), unlike osext_windows.go which caches.

### 3.5 `osext.Executable()` per-OS resolution

- **Windows** (osext_windows.go): `getModuleFileName()`:
  - Allocate `b := make([]uint16, syscall.MAX_PATH)` (MAX_PATH = 260 UTF-16 code units), `size := uint32(len(b))`.
  - `getModuleFileNameProc.Call(0, &b[0], size)` — first arg `hModule = 0/NULL` means "this process's exe".
  - `n := r0` (number of chars copied). If `n == 0` → return `("", e1)` (the syscall error).
  - Return `string(utf16.Decode(b[0:n]))`. **No buffer-too-small retry** — paths longer than MAX_PATH are silently truncated to 260 chars (a known limitation).
- **Linux/Android** (osext_procfs.go): `os.Readlink("/proc/self/exe")`; on error return `(execpath, err)`; then `strings.TrimSuffix` AND `strings.TrimPrefix` of `" (deleted)"` (the `deletedTag` const) — handles the case where the running binary was deleted/replaced on disk.
- **NetBSD**: `os.Readlink("/proc/curproc/exe")`.
- **OpenBSD/DragonFly**: `os.Readlink("/proc/curproc/file")`.
- **Solaris**: `os.Readlink(fmt.Sprintf("/proc/%d/path/a.out", os.Getpid()))`.
- Fallback in procfs file: `errors.New("ExecPath not implemented for " + runtime.GOOS)`.
- **Darwin/FreeBSD** (osext_sysctl.go): builds a 4-element `mib` array and calls `sysctl` twice (once with null buffer to get length `n`, once to fill `buf`):
  - FreeBSD mib: `{1 /*CTL_KERN*/, 14 /*KERN_PROC*/, 12 /*KERN_PROC_PATHNAME*/, -1}`.
  - Darwin mib: `{1 /*CTL_KERN*/, 38 /*KERN_PROCARGS*/, int32(os.Getpid()), -1}`.
  - Errors: nonzero `errNum` from either syscall returns `("", errNum)`. `n == 0` after either call returns `("", nil)` ("shouldn't happen").
  - Truncate `buf` at the first NUL byte. If `execPath[0] != '/'` (not rooted), call `getAbs(execPath)` = `filepath.Join(initCwd, filepath.Clean(execPath))` using the package-init-time CWD (`initCwd, initCwdErr = os.Getwd()`); if `initCwdErr != nil` return `(execPath, initCwdErr)`.
  - On Darwin, additionally `filepath.EvalSymlinks(execPath)` because `KERN_PROCARGS` may return a symlink.
- **Plan9** (osext_plan9.go): open `/proc/<pid>/text`, `syscall.Fd2path(fd)`.

### 3.6 Supporting: `util.NewHTTPRetry(...).Do()` (used by `Upgrade`)

- `defaultTimeout = 30s` (`time.Second * 30`).
- `Do()` is **recursive**: increments `attempts`, executes `http.DefaultClient.Do(req)`, and if `shouldRetry` returns true, sleeps a jitter then recurses.
- `shouldRetry`:
  - On transport error whose message **contains** `"connection reset"` or `"connection refused"`: retry only while `started + timeout` is still in the future (i.e. within 30s window).
  - On response with status `408` (RequestTimeout), `502` (BadGateway), `503` (ServiceUnavailable), `504` (GatewayTimeout), or `429` (TooManyRequests): drain+close body and retry (unconditionally, no time bound).
  - Otherwise no retry.
- Jitter: `100ms + rand.Int63n(500*attempts) ms`. Logged at `Trace` when a logger is set.

### 3.7 Call-site behavior (`cmd/download.go`, `cmd/server.go`) — context for the port

- **download.go** builds: `binaryURL = "https://github.com/shopmonkeyus/eds/releases/download/%s/eds_%s_%s.%s"` with `version` (forced `v` prefix), `platform` (host platform with first letter upper-cased), `arch` (`Host.KernelArch`), `ext` (`"zip"` if `Host.Platform == "windows"` else `"tar.gz"`). `signatureURL = binaryURL + ".sig"`. PublicKey = `ShopmonkeyPublicPGPKey`. Calls `Upgrade(...)`; on error `logger.Fatal`. On success logs `version %s download successful, saved to %s`.
- **server.go** upgrade routine: refuses upgrade inside Docker (`util.IsRunningInsideDocker()`); pauses; downloads via a spawned child process `eds download <version> <fn> --verbose=<bool>` into `dataDir/eds-<versionWithoutV>`; runs `<fn> version` and string-compares trimmed output to the requested version; then `exec := util.GetExecutable()` and `upgrade.Apply(exec, fn)`:
  - If `RollbackError(err) != nil` → `logger.Fatal("failed to apply upgrade: %s", rerr)` (process aborts — unrecoverable).
  - Else if `err != nil` → log error, `unpause()`, return failure response `"failed to rename old binary: %s"`.
  - On success → `http.Get("http://127.0.0.1:<parentPort>/restart")` to restart.

---

## 4. External dependencies

| Go package | Role | Suggested .NET/C# equivalent |
|---|---|---|
| `github.com/ProtonMail/gopenpgp/v3/crypto` | Parse armored PGP public key, build a verifier, stream-verify the downloaded binary against a detached signature (`crypto.Auto` auto-detects armored vs binary). | **BouncyCastle.Cryptography** (`Org.BouncyCastle.Bcpg.OpenPgp`) — `PgpPublicKeyRingBundle`, `PgpObjectFactory`, `PgpSignature.Verify()`. No first-party BCL OpenPGP support. |
| `github.com/shopmonkeyus/eds/internal/util` (`NewHTTPRetry`, `WithLogger`, `GetExecutable`, `IsRunningInsideDocker`) | Retrying HTTP GET with jitter; current-exe path; docker detection. | `HttpClient` + **Polly** for retry/jitter; `Process.GetCurrentProcess().MainModule.FileName` or `Environment.ProcessPath` for exe path. |
| `github.com/shopmonkeyus/go-common/logger` | Structured logger (`Debug/Trace/Info/Error/Fatal`). | `Microsoft.Extensions.Logging.ILogger` (map Trace→`LogTrace`, Fatal→log + exit). |
| `archive/zip` | Open the `.zip`, find first `.exe` entry, copy out. | `System.IO.Compression.ZipFile` / `ZipArchive`. |
| `archive/tar` | Stream tar entries, find `eds`. | **SharpZipLib** (`ICSharpCode.SharpZipLib.Tar`) or `System.Formats.Tar.TarReader` (.NET 7+). |
| `compress/gzip` | Decompress `.tar.gz`. | `System.IO.Compression.GZipStream`. |
| `net/http` | HTTP requests. | `System.Net.Http.HttpClient`. |
| `os`, `io`, `path/filepath`, `bytes` | File create/open/rename/remove/chmod, temp files, copy, path manipulation. | `System.IO.File`/`FileStream`/`Path`/`Directory`, `File.Move`, `File.Delete`, `Path.Combine`, `Path.GetDirectoryName`, `Path.GetFileName`. |
| `syscall` + `unsafe` (Windows hide) | `kernel32!SetFileAttributesW(path, FILE_ATTRIBUTE_HIDDEN)`. | `File.SetAttributes(path, FileAttributes.Hidden)` (BCL — no P/Invoke needed), or P/Invoke `SetFileAttributesW`. |
| `syscall`+`unicode/utf16`+`unsafe` (osext windows) | `kernel32!GetModuleFileNameW(NULL,...)`. | `Environment.ProcessPath` (.NET 6+) or `Process.GetCurrentProcess().MainModule.FileName`. |
| `syscall` (osext sysctl/procfs/plan9) | POSIX exe-path resolution. | `Environment.ProcessPath` covers all OSes in .NET 6+. |
| `context` | Cancellation/deadline for HTTP. | `CancellationToken`. |
| `time` | Timing/jitter. | `Stopwatch`, `TimeSpan`, `Task.Delay`. |

---

## 5. Edge cases & gotchas

- **`Upgrade` archive selection is URL-driven, not OS-driven.** The `.zip` vs `.tar.gz` decision is `filepath.Ext(config.BinaryURL) == ".zip"`. Replicate exactly — do not branch on the host OS inside the extraction routine.
- **ZIP path returns early and skips chmod.** When the `.exe` is found and copied, `Upgrade` returns `nil` immediately — the `os.Chmod(0755)` at the end is NOT executed. (On Windows this is fine; chmod is largely a no-op there anyway.) Conversely, if **no** `.exe` exists in the zip, control falls through and the (empty) output file is chmod'd 0755 and `nil` is returned — a silent "success" producing a zero-byte binary. A faithful port should preserve this, though it is arguably a latent bug.
- **TAR "not found" surfaces as an EOF error.** Missing `eds` entry → `tr.Next()` eventually returns `io.EOF`, wrapped as `"error reading tar header: EOF"`. There is no explicit "binary not found in archive" error. Match this.
- **`prepareAndCheckBinary` never closes `source`.** The opened `source` file handle leaks until process exit. Not fatal because the process is about to swap binaries and restart. In C#, prefer `using` but be aware the Go original leaks — leaking vs disposing shouldn't change observable behavior here.
- **Double `fp.Close()`** in `prepareAndCheckBinary` (explicit + deferred). Harmless in Go; in C# a second `Dispose()` on a `FileStream` is also safe. The explicit close BEFORE the rename is **mandatory on Windows** — the file must not be open or the subsequent `os.Rename(newPath, targetPath)` in `commitBinary` will fail with sharing violation.
- **Windows can't delete a running `.exe`.** This is the entire reason for: (1) renaming the running exe to `.old` instead of deleting it, and (2) `hideFile` when `os.Remove(oldPath)` fails. On C#/Windows, `File.Delete` of the running exe throws; the catch-and-hide path must be preserved. `File.Move` (rename) of a running exe to a sibling path on the same volume *does* succeed on Windows — this is the key trick that makes the swap possible.
- **Rename must be intra-directory / same volume.** All renames use the same `updateDir`, so they stay on one filesystem and behave atomically. If a port placed `.new`/`.old`/temp on a different volume, `File.Move` semantics change (copy+delete) and the "running exe locked" trick breaks. Keep new/old siblings of the target.
- **Pre-delete of `.old` ignores errors** (`_ = os.Remove(oldPath)`). Required because Windows rename fails if destination exists, and a prior upgrade may have left a (hidden, possibly still-locked) `.old`. If the leftover `.old` is locked and can't be removed, the subsequent `Rename(targetPath, oldPath)` will fail and `Apply` returns a non-rollback error (recoverable).
- **`rollbackErr` semantics for `RollbackError`.** A `*rollbackErr` is ONLY produced when the new→target rename fails AND the old→target rollback also fails. That is the single "catastrophic, manual recovery required" signal. The original-rename-failed-but-rollback-succeeded case returns the *plain* original error (RollbackError → nil). The port must mirror this two-tier distinction precisely, since `cmd/server.go` calls `logger.Fatal` (aborts) only in the catastrophic case.
- **Magic constant `2` = `FILE_ATTRIBUTE_HIDDEN`** in hide_windows.go. It passes the attribute set as exactly `2`, which *replaces* the file's attribute bits (does not OR in). If you port to `File.SetAttributes`, using `File.SetAttributes(path, FileAttributes.Hidden)` matches (sets attributes to just Hidden); `attrs | Hidden` would differ subtly. Behavior is equivalent for the `.old` swap file.
- **`SetFileAttributesW` success test is inverted from typical errno checks:** returns nonzero on success, so `r1 == 0` means failure. Don't confuse with the "0 == success" convention.
- **`GetModuleFileNameW` has no large-path retry** — buffer is fixed `MAX_PATH` (260). Long paths truncate silently. `Environment.ProcessPath` in .NET does not have this limit, so a port is *more* correct, not less.
- **osext darwin `getAbs` uses init-time CWD.** `initCwd` is captured at package init; if the program `chdir`s before calling `Executable()`, a relative exe path is resolved against the original CWD, not the current one. Only relevant if porting the macOS path logic (likely unnecessary — see §6).
- **osext is dead code** in this repo (no importers). The live executable-path source is `util.GetExecutable()` → `os.Executable()` with fallback to `os.Args[0]`. For the Windows target, both ultimately resolve to `GetModuleFileNameW`-equivalent behavior.
- **No concurrency** in either package — all functions are synchronous, single-threaded, blocking. The only async surface is the recursive HTTP retry's `time.Sleep`. No goroutines, mutexes, or shared state (except osext's package-level lazy DLL procs and `initCwd`, set once at init).
- **No panics/recovers** in the upgrade logic. `osext_windows.go`/`osext_sysctl.go` use `syscall.MustLoadDLL`/`MustFindProc` which **panic** at package init if kernel32/proc is missing (impossible on real Windows). `hide_windows.go` uses the non-panicking `NewLazyDLL`/`NewProc` (resolution deferred to `.Call`).

---

## 6. C# port notes

- **Target is Windows**, so prioritize: the `Apply`/`commitBinary` rename-and-hide algorithm, `hideFile` via `File.SetAttributes(path, FileAttributes.Hidden)`, the ZIP extraction branch in `Upgrade`, and executable-path resolution via `Environment.ProcessPath`. The tar.gz/POSIX paths can be implemented for completeness but won't execute on the production host.

- **Structure suggestion:**
  - `Upgrader` class with `Task UpgradeAsync(UpgradeConfig config, CancellationToken ct)` mirroring `Upgrade`. Keep the exact step order; preserve the early-return-on-`.exe` (skip chmod) and the EOF-as-not-found behaviors if faithfulness is required, but consider adding an explicit "binary not found in archive" error and a guard against the zero-byte-success case (document any intentional deviation).
  - `BinarySwapper` static class with `Apply(string targetPath, string sourcePath)`, private `PrepareAndCheckBinary`, `CommitBinary`, plus a `RollbackException` type and a `RollbackError(Exception)` helper. Model `rollbackErr` as a custom exception carrying both `OriginalError` and `RollbackError` (or return a result object). Keep `RollbackError(...)` returning the rollback error only for the catastrophic case, matching the Go contract so the caller can decide between fatal-abort and recoverable-retry.

- **File naming/paths:** use `Path.Combine(Path.GetDirectoryName(targetPath), "." + Path.GetFileName(targetPath) + ".new")` and `".old"`. The leading-dot "hidden by convention" name is from Unix; on Windows the `.` prefix doesn't hide the file — that's why the explicit `SetFileAttributes(...Hidden)` exists. Keep both the dot-prefixed name *and* the SetAttributes call to match Go byte-for-byte in on-disk layout.

- **Critical Windows ordering & handle hygiene:** Ensure the `.new` `FileStream` is fully `Dispose()`d before calling `File.Move`. Use `File.Move(target, old)` then `File.Move(new, target)`; on the second failing, `File.Move(old, target)` to roll back; wrap each in try/catch to replicate the error tiers. `File.Move` in .NET does not overwrite by default (matches Go rename failing if dest exists) — keep the pre-delete of `.old` (`File.Delete` in a swallowed try/catch).

- **HTTP retry:** reproduce `util.NewHTTPRetry` semantics — 30s window for `connection reset`/`connection refused` (in .NET, inspect `HttpRequestException`/`SocketException` inner exception types/messages), unconditional retry on status 408/502/503/504/429, jitter = `100ms + Random.Next(0, 500*attempts)ms`. Polly's `WaitAndRetry` with a custom sleep-duration provider and result/exception predicates fits well. Drain+dispose the response body on retryable status codes.

- **PGP verification** is the highest-risk port item: there is no BCL OpenPGP. Use BouncyCastle. The Go code does a *detached*-signature streaming verify against a separate `.sig` file with `crypto.Auto` (armored-or-binary). In BouncyCastle: load the public key ring from the armored key string, read the (possibly armored) signature with `PgpObjectFactory`/`ArmoredInputStream` auto-detection, then `sig.InitVerify(pubKey)` + feed the binary bytes + `sig.Verify()`. Test against a real Shopmonkey release `.sig` to confirm armored/binary handling matches `crypto.Auto`.

- **Executable path:** replace the whole `osext` package with `Environment.ProcessPath` (.NET 6+; falls back appropriately) or `Process.GetCurrentProcess().MainModule!.FileName`. Apply `Path.GetFullPath`/normalization to match `filepath.Clean`. Provide `ExecutableFolder()` = `Path.GetDirectoryName(...)`. The live code uses `os.Executable()` with `os.Args[0]` fallback — mirror with `Environment.ProcessPath ?? Environment.GetCommandLineArgs()[0]`.

- **Risks to watch:** (1) signature-verification compatibility with gopenpgp (encoding/auto-detection); (2) ensuring no open handle on `.new` before the move (sharing violations); (3) faithfully reproducing the two error tiers so the supervisor's fatal-vs-recoverable decision is preserved; (4) the zip/tar "silent success" edge cases — decide explicitly whether to replicate or harden, and document it.