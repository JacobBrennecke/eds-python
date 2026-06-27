# Behavioral Specification — `notification` subsystem

File: `D:/Users/kessler/source/Repos/edsGolang/internal/notification/notification.go`
Package: `notification`
Supporting deps read for accuracy:
- `D:/Users/kessler/source/Repos/edsGolang/internal/util/nats.go` (`DecodeNatsMsg`)
- `D:/Users/kessler/source/Repos/edsGolang/internal/util/util.go` (`JSONStringify`)
- `D:/Users/kessler/source/Repos/edsGolang/internal/consumer/consumer.go` (`CredentialInfo`, `NewNatsConnection`)
- `D:/Users/kessler/source/Repos/edsGolang/internal/driver.go` (`DriverConfigurator`, `FieldError`)
- `D:/Users/kessler/source/Repos/edsGolang/cmd/server.go` (wiring/usage)
- `D:/Users/kessler/source/Repos/edsGolang/internal/notification/notification_test.go` (behavioral expectations)

---

## 1. Purpose

This subsystem is the **NATS-based remote control plane** for an EDS consumer server. A `NotificationConsumer` connects to NATS using session credentials, subscribes to a per-session subject, and listens for control commands published by the Shopmonkey backend (`restart`, `ping`, `shutdown`, `pause`, `unpause`, `upgrade`, `sendlogs`, `configure`, `import`, `driverconfig`, `validate`). Each command is decoded, dispatched to a caller-supplied callback in `NotificationHandler`, and a result is returned either as a synchronous NATS request/reply (JSON) or as an asynchronous published message (msgpack). The parent server (`cmd/server.go`) constructs the consumer with concrete handlers that actually restart the child process, reconfigure drivers, run backfill/import jobs, upload logs, etc. It is the out-of-band command channel that runs alongside the main data-streaming consumer.

---

## 2. Public surface

All exported identifiers in package `notification`.

### 2.1 `NotificationHandler` (struct of callbacks)

```go
type NotificationHandler struct {
    Restart      func()
    Shutdown     func(message string, deleted bool)
    Pause        func() error
    Unpause      func() error
    Upgrade      func(version string) UpgradeResponse
    SendLogs     func() *SendLogsResponse
    Configure    func(config *ConfigureRequest) *ConfigureResponse
    BackfillInit func(*InitBackfillRequest) *InitBackfillResponse
    Import       func(*ImportRequest) *ImportResponse
    DriverConfig func() *DriverConfigResponse
    Validate     func(driver string, values map[string]any) *ValidateResponse
}
```
Note: despite the doc comment calling it an "interface", it is a **struct of function fields**. Any nil field that the matching action invokes will panic (no nil-checks except `SendLogs`).

### 2.2 Request/response DTOs (with exact json/msgpack tags)

```go
type SendLogsResponse struct {
    Path      string `json:"path" msgpack:"path"`
    SessionID string `json:"sessionId" msgpack:"sessionId"`
}

type ImportRequest struct {
    Backfill bool   `json:"backfill" msgpack:"backfill"`
    JobID    string `json:"jobId" msgpack:"jobId"`
}

type InitBackfillRequest struct {
    Backfill bool `json:"backfill" msgpack:"backfill"`
}

type InitBackfillResponse struct {
    Success   bool    `json:"success" msgpack:"success"`
    Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
    SessionID string  `json:"sessionId" msgpack:"sessionId"`
    JobID     string  `json:"jobId" msgpack:"jobId"`
}

type ImportResponse struct {
    Success   bool    `json:"success" msgpack:"success"`
    Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
    SessionID string  `json:"sessionId" msgpack:"sessionId"`
    LogPath   *string `json:"-" msgpack:"-"`      // NEVER serialized
    JobID     string  `json:"jobId" msgpack:"jobId"`
}

type UpgradeResponse struct {
    Success   bool    `json:"success" msgpack:"success"`
    Message   string  `json:"message,omitempty" msgpack:"message,omitempty"` // value type, not pointer
    SessionID string  `json:"sessionId" msgpack:"sessionId"`
    LogPath   *string `json:"-" msgpack:"-"`      // NEVER serialized
    Version   string  `json:"version" msgpack:"version"`
}

type ConfigureRequest struct {
    URL      string `json:"url" msgpack:"url"`
    Backfill bool   `json:"backfill" msgpack:"backfill"`
}

type ConfigureResponse struct {
    Success   bool    `json:"success" msgpack:"success"`
    Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
    MaskedURL *string `json:"maskedURL,omitempty" msgpack:"maskedURL,omitempty"`
    SessionID string  `json:"sessionId" msgpack:"sessionId"`
    Backfill  bool    `json:"backfill" msgpack:"backfill"`
    LogPath   *string `json:"-" msgpack:"-"`      // NEVER serialized
}

type DriverConfigResponse struct {
    Drivers   map[string]internal.DriverConfigurator `json:"drivers" msgpack:"drivers"`
    SessionID string                                 `json:"sessionId" msgpack:"sessionId"`
}

type ValidateResponse struct {
    Success     bool                  `json:"success" msgpack:"success"`
    Message     string                `json:"messsage,omitempty" msgpack:"message,omitempty"` // NOTE the JSON key is the misspelling "messsage"
    FieldErrors []internal.FieldError `json:"field_errors,omitempty" msgpack:"field_errors,omitempty"`
    SessionID   string                `json:"sessionId" msgpack:"sessionId"`
    URL         string                `json:"url,omitempty" msgpack:"url,omitempty"`
}

type Notification struct {
    Action string         `json:"action" msgpack:"action"`
    Data   map[string]any `json:"data,omitempty" msgpack:"data,omitempty"`
}
func (n *Notification) String() string   // returns util.JSONStringify(n)
```

Unexported (but part of the wire contract — important for the port):

```go
type genericResponse struct {
    Success   bool    `json:"success" msgpack:"success"`
    Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
    SessionID string  `json:"sessionId" msgpack:"sessionId"`
    Action    string  `json:"action" msgpack:"action"`
}
```

### 2.3 Referenced external (from `internal` package)

```go
// internal/driver.go
type DriverConfigurator struct {
    Metadata DriverMetadata `json:"metadata"`   // no msgpack tag
    Fields   []DriverField  `json:"fields"`     // no msgpack tag
}
type FieldError struct {
    Field   string `json:"field" msgpack:"field"`
    Message string `json:"error" msgpack:"error"`  // NOTE json key is "error", not "message"
}
func (f FieldError) Error() string  // returns f.Message
```

### 2.4 `NotificationConsumer` and methods

```go
type NotificationConsumer struct {  // all fields unexported
    nc        *nats.Conn
    sub       *nats.Subscription
    logger    logger.Logger
    natsurl   string
    handler   NotificationHandler
    wg        sync.WaitGroup
    sessionID string
}

func New(logger logger.Logger, natsurl string, handler NotificationHandler) *NotificationConsumer
func (c *NotificationConsumer) Start(credsFile string) error
func (c *NotificationConsumer) Stop()
func (c *NotificationConsumer) Restart(credsFile string) error
func (c *NotificationConsumer) PublishSendLogsResponse(response *SendLogsResponse) error
func (c *NotificationConsumer) CallSendLogs()
```

Unexported methods that carry behavior: `publishResponse`, `publishStatus`, `publish`, `publishSimpleStatus`, `configure`, `upgrade`, `importaction`, `driverconfig`, `validate`, `callback`, plus free function `getBool`.

### 2.5 Constants / magic strings (no named consts; all inline literals)

- Logger prefix: `"[notification]"`
- Subscribe subject template: `"eds.notify.%s.>"` (`%s` = SessionID)
- Publish subject template: `"eds.client.%s.%s-%s"` → `eds.client.<sessionId>.<action>-<actionMod>` where actionMod ∈ {`"response"`, `"status"`}
- Header `nats.MsgIdHdr` (`"Nats-Msg-Id"`) = a fresh `uuid.NewString()` per published message
- Header `"content-encoding"` = `"msgpack"` on published messages
- sendlogs action name literal: `"sendlogs"` (lowercase, no dash) used in `PublishSendLogsResponse`
- ping reply payload: literal bytes `"pong"`
- `getBool` true-string literal: `"true"`

---

## 3. Behavior & algorithms

### 3.1 `New`
Returns a `*NotificationConsumer` with `logger = logger.WithPrefix("[notification]")`, `natsurl`, and `handler` set. `nc`, `sub`, `sessionID` left zero/nil. No connection happens here.

### 3.2 `Start(credsFile string) error`
1. `c.nc, info, err = consumer.NewNatsConnection(c.logger, c.natsurl, credsFile)`. On error returns wrapped error: `"failed to create nats connection: %w"`.
   - `NewNatsConnection`: if `credsFile == ""`, builds a dev `CredentialInfo{CompanyIDs:["*"], ServerID:"dev", SessionID: uuid.NewString()}` and connects with no creds; otherwise parses the creds file to derive `CredentialInfo` (CompanyIDs, ServerID, SessionID) and connects with NATS user-credentials. Connection name is `"eds-"+ServerID`.
2. Debug-log `"connected to nats: %s"` with `info.SessionID`.
3. Build `subject = fmt.Sprintf("eds.notify.%s.>", info.SessionID)`.
4. `c.sub, err = c.nc.Subscribe(subject, c.callback)`. On error returns wrapped error: `"failed to subscribe to eds.notify: %w"`. (This is an **async push subscription**; messages drive `callback`.)
5. Debug-log `"subscribed to: %s"`.
6. `c.sessionID = info.SessionID`; return nil.

### 3.3 `Stop()`
1. If `c.sub != nil`: `c.sub.Unsubscribe()`; on error logs `"failed to unsubscribe from nats: %s"`. Then `c.sub = nil`.
2. If `c.nc != nil`: `c.nc.Close()`; then `c.nc = nil`.
3. `c.wg.Wait()` — blocks until all in-flight callbacks AND any background import goroutine finish.
4. Debug-log `"stopped"`. Idempotent (safe to call when already stopped).

### 3.4 `Restart(credsFile string) error`
`c.Stop()` then `return c.Start(credsFile)`. (Distinct from the `handler.Restart` action callback.)

### 3.5 Publishing helpers
- `publishResponse(sessionId, action, v)` → `publish(sessionId, action, "response", v)`
- `publishStatus(sessionId, action, v)` → `publish(sessionId, action, "status", v)`
- `publish(sessionId, action, actionMod, v)`:
  1. `data, err := msgpack.Marshal(v)`; on error returns `"error marshaling response: %w"`.
  2. `msg := nats.NewMsg(fmt.Sprintf("eds.client.%s.%s-%s", sessionId, action, actionMod))`.
  3. `msg.Data = data`.
  4. `msg.Header.Add("Nats-Msg-Id", uuid.NewString())`.
  5. `msg.Header.Add("content-encoding", "msgpack")`.
  6. Trace-log `"sending response: %s"` with subject.
  7. `c.nc.PublishMsg(msg)`; on error returns `"error sending response: %w"`. Else nil.
- `publishSimpleStatus(action, errMsg string)`: publishes a **status** message with `c.sessionID` and a `genericResponse{Success: errMsg=="", Message: &errMsg, SessionID: c.sessionID, Action: action}`. On publish error logs `"failed to send %s status: %s"`.
  - **Gotcha:** `Message` is always `&errMsg` (non-nil pointer). Because `omitempty` on a non-nil pointer does NOT omit, the success case serializes `"message":""` (empty string present), NOT omitted.

### 3.6 `PublishSendLogsResponse(response *SendLogsResponse) error`
`return c.publishResponse(response.SessionID, "sendlogs", response)` — publishes msgpack to `eds.client.<response.SessionID>.sendlogs-response`. Uses the response's own SessionID, not `c.sessionID`.

### 3.7 `CallSendLogs()`
1. `response := c.handler.SendLogs()`.
2. If `response == nil`: Warn `"sendlogs handler returned nothing"`; return (no publish).
3. Else `PublishSendLogsResponse(response)`; on error logs `"failed to send sendlogs response: %s"`.

Called both from the `"sendlogs"` action and from the server's hourly `logSenderTicker` (every `time.Hour`).

### 3.8 `callback(m *nats.Msg)` — the dispatcher
1. `c.wg.Add(1)`; `defer c.wg.Done()`.
2. Decode message into `var notification Notification` via `util.DecodeNatsMsg`. On error: Error-log `"failed to decode notification message: %s"` and return.
   - **`DecodeNatsMsg` decoding rules** (must be replicated): read header `content-encoding`. If `"gzip/json"` → gunzip then json-unmarshal. If `"msgpack"` → msgpack-unmarshal into `any`, re-marshal to JSON, then json-unmarshal into target. Otherwise json-unmarshal raw bytes directly. Net effect: `notification.Data` is always a JSON-shaped `map[string]any` (objects→`map[string]any`, numbers→`float64`, bools→`bool`).
3. Trace-log `"received message: %s"` with `notification.String()` (JSON).
4. Define closure `respondGenerically(err error)`:
   - If `err != nil`: Error-log `"failed to %s: %s"` (action, err); set `errmsg = &err.Error()`.
   - Publish a **response** with `c.sessionID` and `genericResponse{Success: errmsg==nil, Message: errmsg, SessionID: c.sessionID, Action: notification.Action}`.
   - On publish error logs `"failed to send pause response: %s"` (note: hard-coded "pause" wording for all actions).
   - Here, on success `errmsg` is nil so `Message` IS omitted (`omitempty` + nil pointer).
5. `switch notification.Action`:

   - **`"restart"`**: `publishSimpleStatus("restart", "")` → `c.handler.Restart()` (synchronous) → `respondGenerically(nil)`.
   - **`"ping"`**: if `Data["subject"].(string)` present → Trace-log and `c.nc.Publish(subject, []byte("pong"))`; on publish error logs `"error sending ping response: %s"`. If missing/not-string → Warn `"invalid ping notification. missing subject for: %s"`. (No response/status published.)
   - **`"shutdown"`**: `deleted := getBool(Data["deleted"])`. If `Data["message"].(string)` present → `c.handler.Shutdown(message, deleted)`. Else Warn `"invalid shutdown notification. missing message for: %s"`. (No reply.)
   - **`"pause"`**: `respondGenerically(c.handler.Pause())`.
   - **`"unpause"`**: `respondGenerically(c.handler.Unpause())`.
   - **`"upgrade"`**: extract `version` from `Data["version"].(string)`. If missing/not-string → build msg `"invalid upgrade notification. missing version for: %s"`, Warn it, `publishSimpleStatus("upgrade", msg)`, and **return** (no upgrade). Else `publishSimpleStatus("upgrade", "")` then `c.upgrade(version)`.
   - **`"sendlogs"`**: `c.CallSendLogs()`.
   - **`"configure"`**: build `ConfigureRequest`; `req.URL = Data["url"].(string)` if present (else empty); `req.Backfill = getBool(Data["backfill"])`; `c.configure(req, m)`.
   - **`"import"`**: build `ImportRequest`; `req.Backfill = getBool(Data["backfill"])`; `c.importaction(&req, m)`.
   - **`"driverconfig"`**: `c.driverconfig(m)`.
   - **`"validate"`**: require `Data["driver"].(string)` (else Error-log `"invalid validate notification. missing driver for: %s"` and return) and `Data["config"].(map[string]any)` (else Error-log `"invalid validate notification. missing config for: %s"` and return); then `c.validate(driver, config, m)`.
   - **default**: Warn `"unknown action: %s"`.

### 3.9 `configure(config ConfigureRequest, m *nats.Msg)`
1. `response := c.handler.Configure(&config)`.
2. `m.Respond([]byte(util.JSONStringify(response)))` — **synchronous NATS reply, JSON-encoded**. On error logs `"failed to send driverconfig response: %s"` (note: mislabeled "driverconfig").
3. Else if `response.LogPath != nil`: `PublishSendLogsResponse(&SendLogsResponse{Path:*response.LogPath, SessionID: response.SessionID})`; on error logs `"failed to publish send logs response during configure: %s"`.

### 3.10 `upgrade(version string)`
1. `response := c.handler.Upgrade(version)` (returns `UpgradeResponse` by value).
2. `publishResponse(response.SessionID, "upgrade", response)` — **async msgpack publish** (NOT `m.Respond`). On error logs `"failed to send upgrade response: %s"`.
3. Else if `response.LogPath != nil`: publish sendlogs response; on error logs `"failed to publish send logs response during upgrade: %s"`.

### 3.11 `importaction(req *ImportRequest, m *nats.Msg)`
1. `initResponse := c.handler.BackfillInit(&InitBackfillRequest{Backfill: req.Backfill})`.
2. `m.Respond([]byte(util.JSONStringify(initResponse)))` — **JSON reply** of the *init* response. On error logs `"failed to send import response: %s"` and **returns** (no import).
3. If `!initResponse.Success`: **return** (the failed init was already delivered via the reply).
4. `req.JobID = initResponse.JobID`.
5. `publishSimpleStatus("import", "")`.
6. `c.wg.Add(1)`; spawn a **background goroutine** (so other commands like restart can proceed while a long import runs):
   - `defer c.wg.Done()`.
   - `response := c.handler.Import(req)`.
   - `publishResponse(response.SessionID, "import", response)` (async msgpack). On error logs `"failed to send import response: %s"`.
   - Else if `response.LogPath != nil`: publish sendlogs response; on error logs `"failed to publish send logs response during import: %s"`.

### 3.12 `driverconfig(m *nats.Msg)`
`response := c.handler.DriverConfig()`; `m.Respond([]byte(util.JSONStringify(response)))` — JSON reply. On error logs `"failed to send driverconfig response: %s"`.

### 3.13 `validate(driver string, vals map[string]any, m *nats.Msg)`
`response := c.handler.Validate(driver, vals)`; `m.Respond([]byte(util.JSONStringify(response)))` — JSON reply. On error logs `"failed to send validate response: %s"`.

### 3.14 `getBool(val any) bool`
- If `val` is `bool` → return it.
- Else if `val` is `string` → return `val == "true"`.
- Else → `false`. (So `nil`, numbers, etc. → false.)

### 3.15 Serialization-channel summary (critical)

| Action | Reply channel | Encoding | Subject |
|---|---|---|---|
| restart | published status + published response | msgpack | `eds.client.<sid>.restart-status`, `…restart-response` |
| ping | published "pong" | raw bytes | subject from `Data["subject"]` |
| shutdown | none | — | — |
| pause / unpause | published response | msgpack | `eds.client.<sid>.<action>-response` |
| upgrade | published status, then published response (+ optional sendlogs) | msgpack | `…upgrade-status`, `…upgrade-response` |
| sendlogs | published response | msgpack | `eds.client.<sid>.sendlogs-response` |
| configure | **`m.Respond` (request/reply)** + optional published sendlogs | **JSON** reply / msgpack sendlogs | reply inbox |
| import | **`m.Respond`** (init) + published status + async published response (+ optional sendlogs) | JSON reply / msgpack | reply inbox, `…import-status`, `…import-response` |
| driverconfig | **`m.Respond`** | **JSON** | reply inbox |
| validate | **`m.Respond`** | **JSON** | reply inbox |

`sessionId` used for published subjects is `c.sessionID` for status/generic responses, but `response.SessionID` for upgrade/import/sendlogs responses (taken from the handler's returned struct).

---

## 4. External dependencies

| Go package | Role here | .NET / C# equivalent |
|---|---|---|
| `github.com/nats-io/nats.go` | NATS core client: `Conn`, `Subscription`, `Subscribe`, `PublishMsg`, `Publish`, `NewMsg`, `Msg.Respond`, `Msg.Header`, `MsgIdHdr` | `NATS.Net` (NuGet `NATS.Client.Core`) — `NatsConnection`, `SubscribeAsync`, `PublishAsync`, request/reply via `RequestAsync` / reply-to subject. Headers via `NatsHeaders`. |
| `github.com/vmihailenco/msgpack/v5` | MessagePack encode of published responses | `MessagePack` (NuGet `MessagePack` by neuecc) or `MessagePack-CSharp`. Configure to match map-key naming (use the json/msgpack tag names). |
| `github.com/google/uuid` | `uuid.NewString()` for `Nats-Msg-Id` header (and dev session id) | `System.Guid.NewGuid().ToString()` (lowercase, hyphenated — matches Go's default format). |
| `github.com/shopmonkeyus/go-common/logger` | Leveled logger (`WithPrefix`, `Debug`, `Trace`, `Warn`, `Error`) | `Microsoft.Extensions.Logging.ILogger` with a scope/prefix, or Serilog. Preserve the level mapping (Trace/Debug/Warn/Error). |
| `github.com/shopmonkeyus/eds/internal` | `DriverConfigurator`, `FieldError`, driver registry types in DTOs | Port the corresponding C# DTOs from the `internal`/driver subsystem. |
| `github.com/shopmonkeyus/eds/internal/consumer` | `NewNatsConnection`, `CredentialInfo` (connection + session id derivation) | Port from the `consumer` subsystem; provide a `(NatsConnection, CredentialInfo)` factory. |
| `github.com/shopmonkeyus/eds/internal/util` | `DecodeNatsMsg` (content-encoding aware decode), `JSONStringify` | Helper: content-encoding switch (gzip/msgpack/json) + `System.Text.Json.JsonSerializer.Serialize`. |
| `encoding/json` (transitively, via util) | JSON (de)serialization of replies and `Notification` | `System.Text.Json` (use camelCase property names matching the json tags; do NOT use default PascalCase). |
| `github.com/shopmonkeyus/go-common/compress` (via util) | `Gunzip` for `gzip/json` encoded inbound msgs | `System.IO.Compression.GZipStream`. |
| `sync` (`WaitGroup`) | track in-flight callbacks + import goroutine for graceful Stop | `CountdownEvent`, or track `Task`s in a list and `Task.WhenAll`, or a `SemaphoreSlim`-based counter. |
| `fmt` | error wrapping (`%w`) and subject formatting | `string.Format` / interpolation; exceptions with `InnerException` for `%w`. |

---

## 5. Edge cases & gotchas

- **`NotificationHandler` fields are not nil-checked** (except `SendLogs` in `CallSendLogs`). If the backend sends an action whose handler is nil, Go panics. In C#, a nil delegate → `NullReferenceException`. The parent always wires all handlers, but a faithful port should mirror that there is no per-action guard. Note `callback` itself has **no panic recovery** — a panic propagates into the NATS client goroutine. (The server wraps its own ticker goroutine with `util.RecoverPanic`, but not the subscription callback.)
- **Two different reply mechanisms and encodings.** `configure`, `import` (init), `driverconfig`, `validate` use NATS request/reply (`m.Respond`) with **JSON**. `restart`, `pause`, `unpause`, `upgrade`, `import` (final), `sendlogs`, and all status messages are **published** to `eds.client.…` subjects with **msgpack** + headers. A port must not unify these.
- **`omitempty` semantics on pointers vs values.** For pointer `*string Message` with `omitempty`: omitted only when the pointer is nil. In `publishSimpleStatus`/`genericResponse` the success path sets `Message:&""` (non-nil) → serialized as `"message":""`. In `respondGenerically` success path `Message` is nil → omitted. `UpgradeResponse.Message` is a value `string` with `omitempty` → omitted only when empty string. Replicate each precisely.
- **Misspelled JSON key in `ValidateResponse`**: `json:"messsage,omitempty"` (three `s`). The msgpack tag is correctly `"message,omitempty"`. Since validate replies go out as **JSON**, the field name on the wire is literally `messsage`. A faithful port must emit `messsage` for JSON-encoded validate replies (otherwise the backend won't read it).
- **`FieldError` JSON key is `error`, not `message`** (`json:"error"`). msgpack tag is `field`/`error`.
- **`LogPath` fields are `json:"-" msgpack:"-"`** — never serialized. They are an out-of-band signal: when non-nil, the consumer additionally fires a separate `sendlogs` response with that path. The C# DTO should mark these `[JsonIgnore]` / `[IgnoreMember]`.
- **`getBool` is lenient**: accepts bool, or string equal exactly to `"true"` (case-sensitive); everything else (including `"True"`, `"1"`, numbers, nil) is false.
- **JSON number typing**: after `DecodeNatsMsg`, numeric `Data` values are `float64` and would fail a direct `.(string)`/`.(bool)` assertion; only `getBool` salvages booleans-as-strings. The data fields accessed (`subject`, `message`, `version`, `url`, `driver`) are expected as strings; `config` as object; `backfill`/`deleted` via getBool.
- **`import` runs the actual import on a detached goroutine** and increments `wg` so `Stop()` blocks until it finishes. The init reply is sent synchronously first; if init fails the goroutine is never spawned. In C#, do not block the subscription handler on the long import — spawn a tracked `Task` and await it in shutdown.
- **`Stop()` ordering**: unsubscribe → close conn → `wg.Wait()`. Because the conn is closed before waiting, any in-flight handler trying to publish after close will get a publish error (logged, not fatal). Replicate: closing the connection mid-flight must not crash handlers.
- **`Stop()` is idempotent** (nil-guards on `sub`/`nc`); `Restart` relies on this.
- **`restart` action calls `c.handler.Restart()` synchronously inside the callback**, then responds. In the server this restart can itself stop/restart the consumer; ensure no deadlock between the callback's `wg` and `Stop()`'s `wg.Wait()` (in Go they're different consumer lifetimes / goroutines). Be careful porting so a restart handler doesn't synchronously await the same wait handle the callback is counted in.
- **Copy-paste log strings**: `respondGenerically` always logs `"failed to send pause response"`; `configure` logs `"failed to send driverconfig response"`. These are cosmetic but should be preserved if log parity matters.
- **Subscription is a wildcard** `eds.notify.<sessionID>.>`; the trailing token (e.g. `.validate`, `.restart`) is part of the subject but the dispatcher ignores it — routing is driven entirely by the decoded `Notification.Action` field, not the subject token. (The test publishes to `eds.notify.<sid>.validate` but the action field is what matters.)
- **`ping` replies with raw bytes `"pong"`** (no encoding header) to an arbitrary subject supplied in the message — used for liveness probes.
- **Header constant**: `nats.MsgIdHdr` == `"Nats-Msg-Id"`; a fresh UUID per message enables NATS/JetStream dedup on the receiving side. Preserve generating a new id per publish.

---

## 6. C# port notes

- **Class layout.** Create `NotificationConsumer` holding `INatsConnection? _nc`, `INatsSub<…>? _sub` (or an `IAsyncDisposable` subscription handle), `ILogger _logger` (with a `"[notification]"` scope/prefix), `string _natsUrl`, `NotificationHandler _handler`, `string _sessionId`, and an in-flight tracker (e.g. `CountdownEvent`/list of `Task`). Mirror `New/Start/Stop/Restart` as constructor + `StartAsync`/`StopAsync`/`RestartAsync`. NATS.Net is async-first, so the synchronous Go signatures become `Task`-returning.
- **`NotificationHandler`.** Model as a class/record of delegates: `Action Restart`, `Action<string,bool> Shutdown`, `Func<Task>`/`Func<Exception?>` for Pause/Unpause (Go returns `error`; map to either return-`Exception?` or throw), `Func<string,UpgradeResponse> Upgrade`, `Func<SendLogsResponse?> SendLogs`, etc. Keep them nullable to match Go's lack of guards, but the parent will set all. Consider making them `async` Funcs given C# I/O is async.
- **Serialization fidelity is the highest risk.** Configure `System.Text.Json` so property names equal the json tags exactly (camelCase: `sessionId`, `jobId`, `maskedURL`, `field_errors`, and the misspelled `messsage`). Use `[JsonPropertyName]` per field; do NOT rely on a global naming policy because of irregular names (`maskedURL`, `field_errors`, `messsage`, `FieldError`→`error`). Use `[JsonIgnore(Condition = WhenWritingNull)]` for the `omitempty` pointer fields and `[JsonIgnore]` for `LogPath`. For value-type `omitempty` (e.g. `UpgradeResponse.Message` string, `Version`, `URL`) emulate Go's "omit when zero value" — STJ's `WhenWritingDefault` will omit empty strings; verify it matches Go for each field that has `omitempty`.
- **Two serializers.** Replies (`m.Respond`) use JSON; published `eds.client.*` messages use MessagePack with two headers. Build two helpers: `RespondJson(msg, obj)` and `PublishMsgpack(sessionId, action, mod, obj)` that set `Nats-Msg-Id`=`Guid.NewGuid().ToString()` and `content-encoding`=`msgpack`. For MessagePack, register a resolver so map keys equal the msgpack tag names (most use the same camelCase as JSON; differences: `FieldError` msgpack `field`/`error`, `ValidateResponse` msgpack `message`).
- **Inbound decode.** Port `DecodeNatsMsg`: read `content-encoding` header; `gzip/json`→`GZipStream` then JSON; `msgpack`→deserialize to a loosely-typed object then re-serialize to JSON then JSON-deserialize into `Notification` (or deserialize msgpack directly into `Notification`); otherwise JSON directly. After decode, `Notification.Data` is best modeled as `Dictionary<string, JsonElement>` (or `object`); replicate the type-assertion checks (`TryGetString`, `TryGetObject`) and the `getBool` leniency (bool, or string == "true", else false).
- **Concurrency / lifetime.** NATS.Net push subscriptions deliver via an `await foreach` loop; run that loop on a background `Task`. Track each handler invocation and the long-running import as `Task`s; `StopAsync` should unsubscribe, dispose/close the connection, then `await Task.WhenAll(inflight)` — matching Go's `wg.Wait()` after close. Guard against the import handler awaiting the same shutdown it's counted in (avoid deadlock).
- **Long-running import.** Reproduce the pattern: respond to the init request synchronously (JSON), publish an `import-status`, then fire-and-track a background task that runs `Import` and publishes the final `import-response` (msgpack) plus optional sendlogs. Do not block the subscription pump on it.
- **Error handling.** Go logs and continues on publish/respond failures (never throws out of the callback). In C#, wrap each handler body in try/catch that logs and swallows, so one bad message can't kill the subscription loop. Preserve the exact log message strings if log parity is required (including the mislabeled "pause"/"driverconfig" ones).
- **UUID/format parity.** `Guid.NewGuid().ToString()` yields lowercase hyphenated 8-4-4-4-12, matching `uuid.NewString()`.
- **Subjects.** Hard-code the same templates: subscribe `$"eds.notify.{sessionId}.>"`; publish `$"eds.client.{sessionId}.{action}-{mod}"`. Keep the wildcard `>` and ignore the trailing subject token (dispatch on `action`).