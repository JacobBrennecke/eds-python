I have read all assigned files and their key dependencies. Here is the behavioral specification.

# Behavioral Specification: `drivers-stream` (S3, Kafka, EventHub, File) + cmd driver shims

## 1. Purpose

This subsystem implements the four "streaming / object-store" output drivers of the EDS consumer: **S3** (object storage; AWS / Google Cloud Storage / LocalStack), **Kafka** (topic streaming), **EventHub** (Azure Event Hubs streaming), and **File** (local filesystem). Each driver is a sink that receives Shopmonkey database-change events (`internal.DBChangeEvent`) from the NATS/JetStream consumer and writes each event out as a JSON document/message, keyed/partitioned by table + company + location + primary key. All four also implement the **Importer** interface (bulk migration of CockroachDB changefeed export files), but none support deletes or schema migration. The `cmd/driver_*.go` files are tiny build-tag-gated blank-import shims that decide which driver packages compile into the binary (each driver self-registers in its `init()`).

---

## 2. Public surface

### 2.1 Shared interfaces these drivers implement (from `internal/driver.go`, `internal/importer.go`)

```go
type DriverLifecycle interface { Start(config DriverConfig) error }

type Driver interface {
    Stop() error
    MaxBatchSize() int
    Process(logger logger.Logger, event DBChangeEvent) (bool, error)
    Flush(logger logger.Logger) error
    Test(ctx context.Context, logger logger.Logger, url string) error
    Configuration() []DriverField
    Validate(map[string]any) (string, []FieldError)
}

type DriverHelp interface { Name() string; Description() string; ExampleURL() string; Help() string }
type DriverAlias interface { Aliases() []string }            // NOT implemented by any of these 4
type DriverMigration interface { ... }                       // NOT implemented by any of these 4

type Importer interface { Import(config ImporterConfig) error }
type ImporterHelp interface { SupportsDelete() bool }

// importer.Handler (internal/importer/importer.go)
type Handler interface {
    CreateDatasource(schema internal.SchemaMap) error
    ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error
    ImportCompleted() error
}
```

Each driver asserts at compile time it satisfies: `internal.Driver`, `internal.DriverLifecycle`, `internal.DriverHelp`, `internal.Importer`, `internal.ImporterHelp`, `importer.Handler`.

### 2.2 Supporting structs (referenced by these drivers)

```go
// internal/dbchange.go — the payload that gets serialized to JSON
type DBChangeEvent struct {
    Operation     string          `json:"operation"`
    ID            string          `json:"id"`
    Table         string          `json:"table"`
    Key           []string        `json:"key"`
    ModelVersion  string          `json:"modelVersion"`
    CompanyID     *string         `json:"companyId,omitempty"`
    LocationID    *string         `json:"locationId,omitempty"`
    UserID        *string         `json:"userId,omitempty"`
    Before        json.RawMessage `json:"before,omitempty"`
    After         json.RawMessage `json:"after,omitempty"`
    Diff          []string        `json:"diff,omitempty"`
    Timestamp     int64           `json:"timestamp"`       // epoch MILLISECONDS
    MVCCTimestamp string          `json:"mvccTimestamp"`
    Imported      bool            `json:"imported,omitempty"` // added during import only
    NatsMsg       jetstream.Msg   `json:"-"`
    object        map[string]any  // unexported, lazily decoded from After else Before
    SchemaValidatedPath *string   `json:"-"`               // set by schema validator during import
}
func (c *DBChangeEvent) GetPrimaryKey() string  // last element of Key[]; else object["id"]; else ""
func (c *DBChangeEvent) GetObject() (map[string]any, error) // decodes After, else Before, else nil

// internal/util/batcher.go (used by EventHub)
type Record struct {
    Table     string                  `json:"table"`
    Id        string                  `json:"id"`
    Operation string                  `json:"operation"`
    Diff      []string                `json:"diff"`
    Object    map[string]any          `json:"object"`
    Event     *internal.DBChangeEvent `json:"-"`
}
type Batcher struct { records []*Record; pks map[string]uint }
func NewBatcher() *Batcher
func (b *Batcher) Add(event *internal.DBChangeEvent)  // builds a Record (Id=GetPrimaryKey, Object=GetObject)
func (b *Batcher) Records() []*Record
func (b *Batcher) Clear()    // records=nil; pks=new map
func (b *Batcher) Len() int

type DriverField struct {
    Name string `json:"name"`; Type DriverType `json:"type"`
    Format DriverFormat `json:"format,omitempty"`
    Default *string `json:"default,omitempty"`
    Description string `json:"description"`; Required bool `json:"required"`
}
type FieldError struct { Field string `json:"field" msgpack:"field"`; Message string `json:"error" msgpack:"error"` }
```

### 2.3 S3 — `internal/drivers/s3/s3.go`

Exported:
- `type RecalculateV4Signature struct { next http.RoundTripper; signer *v4.Signer; cfg aws.Config }`
  - `func (lt *RecalculateV4Signature) RoundTrip(req *http.Request) (*http.Response, error)` — GCS interop signer.
- `func NewS3Client(ctx context.Context, logger logger.Logger, urlString string) (*awss3.Client, string, string, int, int, error)` — returns `(client, bucket, prefix, maxBatchSize, uploadTasks, err)`.

Unexported but central: `s3Driver`, `job`, `s3Provider` enum (`awsProvider=0`, `googleProvider=1`, `localstackProvider=2`), helpers `addFinalSlash`, `getBucketInfo`, `getEndpointResolver`, `parseBucketURL`, `getCloudProvider`.

Driver methods: `Start`, `Stop`, `MaxBatchSize`, `Process`, `Flush`, `Name`("AWS S3"), `Description`, `ExampleURL`, `Help`, `Configuration`, `Validate`, `Test`, plus importer methods `CreateDatasource`, `ImportEvent`, `ImportCompleted`, `Import`, `SupportsDelete`. Registers scheme `"s3"`.

### 2.4 Kafka — `internal/drivers/kafka/kafka.go`

Constants: `edsPartitionKeyHeader = "eds-partitionkey"`, `maxImportBatchSize = 1_000`.
Unexported: `messageBalancer` (`func (b *messageBalancer) Balance(msg gokafka.Message, partitions ...int) int`), `kafkaDriver`, helper `strWithDef`.
Driver methods as above; `Name()` = `"Kafka"`. Registers scheme `"kafka"`.

### 2.5 EventHub — `internal/drivers/eventhub/eventhub.go`

Constant: `maxImportBatchSize = 100`. Package var: `contentType = "application/json"`.
Exported funcs:
- `func ParseConnectionString(urlString string) (string, error)`
- `func NewPartitionKey(table string, companyId *string, locationId *string, id string) string`
Unexported: `eventHubDriver`, `newProducerClient`, `strWithDef`, methods `getKeys`, `addEventToBatch`.
`Name()` = `"Microsoft Azure EventHub"`. Registers scheme `"eventhub"`.

### 2.6 File — `internal/drivers/file/file.go`

Unexported `fileDriver`; exported method `func (p *fileDriver) GetPathFromURL(urlString string) (string, error)` (exported name on unexported type — effectively internal). Methods `getFileName`, `writeEvent`. `Name()` = `"File"`. Registers scheme `"file"`.

### 2.7 cmd shims (`cmd/driver_*.go`)

No exported symbols. Each is a build-constrained file in `package cmd` containing only a blank import of the driver package, e.g. `import _ "github.com/shopmonkeyus/eds/internal/drivers/s3"`.

---

## 3. Behavior & algorithms

### 3.0 Common: JSON payload (`util.JSONStringify`)
`JSONStringify(val) = string(json.Marshal(val))` with error ignored. **Every** driver writes the *entire* `DBChangeEvent` (or `Record.Event`, a `*DBChangeEvent`) as the message/file body. Field order = struct declaration order; `omitempty` fields dropped when empty; `before`/`after` are `json.RawMessage` emitted verbatim. Go's `json.Marshal` HTML-escapes `<`,`>`,`&` (and `U+2028/U+2029`) and sorts map keys — both matter for byte equivalence (see §6).

### 3.1 S3

**Scheme/aliases:** `"s3"`, no aliases. Registered as driver AND importer.

**Provider detection (`getCloudProvider`):** host matched by `util.IsLocalhost` (contains `"localhost"` / `"127.0.0.1"` / `"0.0.0.0"`) → `localstackProvider`; host contains `"googleapis.com"` → `googleProvider`; otherwise `awsProvider`.

**Bucket/prefix parsing (`getBucketInfo`):**
- `awsProvider`: endpoint `""`, bucket = `u.Host`, prefix = `addFinalSlash(strings.TrimPrefix(u.Path,"/"))`.
- non-AWS: split `TrimPrefix(u.Path,"/")` on `/`; bucket = first part; prefix = `addFinalSlash(join(rest,"/"))`. localstack endpoint = `"http://"+u.Host`; google/other endpoint = `"https://"+u.Host`.
- `addFinalSlash`: returns `""` if empty, else appends `/` if not already suffixed.

**Endpoint resolver (`getEndpointResolver`):** google → fixed `URL:"https://storage.googleapis.com"`, `SigningRegion:"auto"`, `Source:EndpointSourceCustom`, `HostnameImmutable:true`. Else if url non-empty → `PartitionID:"aws"`, that URL, `SigningRegion:region`, `SigningMethod:"v4"`. Else returns `&aws.EndpointNotFoundError{}` (SDK falls back to default).

**Region resolution (`NewS3Client`):** `region = env AWS_REGION`; if `?region=` present use it; else if region=="" → `env AWS_DEFAULT_REGION`; else `"us-west-2"`. Then a second block: `if u.Query().Has("region")` overrides again with `?region=`. Net: query `region` wins, then `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `"us-west-2"`.

**Credentials:** `?access-key-id=` else env `AWS_ACCESS_KEY_ID`; `?secret-access-key=` else `AWS_SECRET_ACCESS_KEY`; `?session-token=` else `AWS_SESSION_TOKEN`. Always uses `credentials.NewStaticCredentialsProvider(...)`.

**Retries:** `?max-retries=` parsed via `strconv.Atoi`; parse failure → `logger.Warn("skipping max-retires value: %s. %d", ...)` and default `5`; absent → `5`. Applied to `retry.NewStandard{MaxAttempts}`.

**Config build:** google → `LoadDefaultConfig(region="auto", customResolver, staticCreds)`; on success sets `cfg.HTTPClient` to a client whose `Transport` is `&RecalculateV4Signature{http.DefaultTransport, v4.NewSigner(), cfg}` and `cfg.RequestChecksumCalculation = RequestChecksumCalculationWhenRequired`. Non-google → `LoadDefaultConfig(region, customResolver, staticCreds)`. Client created with `o.UsePathStyle = true` and the custom retryer.

**Batch/upload tuning:** `maxBatchSize` default `1_000` (`?maxBatchSize=`; `Atoi` error → hard error; value `<=0` → reset `1_000`). `uploadTasks` default `4` (`?uploadTasks=`; `Atoi` error → hard error; `<=0` → `4`).

**`RecalculateV4Signature.RoundTrip`:** saves `Accept-Encoding`, deletes it (so it isn't signed), parses `X-Amz-Date` using layout `"20060102T150405Z"`, retrieves creds, re-signs with `signer.SignHTTP(ctx, creds, req, v4.GetPayloadHash(ctx), "s3", region, timeDate)`, restores `Accept-Encoding`, forwards to `next`.

**`connect(ctx, logger, url, testonly)`:** creates child `context.WithCancel`; calls `NewS3Client`; stores bucket/prefix/client. If `testonly` → return early. Logs `setting maxBatchSize=%d uploadTasks=%d`. Creates `ch = make(chan job, maxBatchSize)`, `errors = make(chan error, maxBatchSize)`, spawns `uploadTasks` goroutines running `run()` (each `waitGroup.Add(1)`).

**`run()` worker loop:** `select` on `ctx.Done()` (return) or `job := <-ch`: `buf = JSONStringify(event)`; `PutObject(context.Background(), {Bucket, Key, ContentType:"application/json", Body:bytes.NewReader(buf), ContentLength:len(buf)})`. On error: push `fmt.Errorf("error storing s3 object to %s:%s: %w", bucket, key, err)` to `errors`. On success: `job.logger.Trace("uploaded to %s:%s", ...)`. Always `jobWaitGroup.Done()`. (Note: uses `context.Background()`, not the cancelable ctx, for the actual PutObject.)

**`process` / object key naming:** if `event.SchemaValidatedPath != nil` → `key = path.Join(prefix, *SchemaValidatedPath)`; else `key = path.Join(prefix, event.Table, fmt.Sprintf("%d-%s.json", time.UnixMilli(event.Timestamp).Unix(), event.GetPrimaryKey()))`. So default key = `<prefix><table>/<unixSECONDS>-<primaryKey>.json`. If `dryRun` → trace `would store %s:%s`; else `jobWaitGroup.Add(1)` then `ch <- job{logger, event, key}`. Returns `(false, nil)`.

**`MaxBatchSize()` returns hard-coded `1_000`** (ignores the configured maxBatchSize, which only sizes the channel/worker pool).

**`Flush`:** `jobWaitGroup.Wait()`, then non-blocking drain of `errors` channel into a slice, `errors.Join(errs...)` (nil if none). Logs `flush called` / `flush finished`.

**`Stop`:** `cancel()`; debug `stopping s3 driver`; `jobWaitGroup.Wait()`; `close(ch)` if non-nil; `waitGroup.Wait()`; debug `stopped s3 driver`.

**Import:** `Import` returns nil if `SchemaOnly`; sets logger prefix `[s3]`, stores config, `connect(...,false)`, then `importer.Run`. `CreateDatasource` no-op. `ImportEvent` → `process(importConfig.Context, logger, event, importConfig.DryRun)`. `ImportCompleted` → `Flush`. `SupportsDelete` → `false`.

**`Test`:** `connect(ctx, logger, url, testonly=true)` (no workers, no upload). 

**`Validate`:** builds a `url.URL{Scheme:"s3"}`. Required `Bucket`; optional `Prefix`,`Region`,`Access Key ID`,`Secret Access Key`,`Endpoint`. If `Endpoint`!="" → `Host=Endpoint`, `Path=Bucket`; else `Host=Bucket`. Prefix appended to Path via `path.Join`. Query keys set: `region`, `access-key-id`, `secret-access-key` (NOTE: no session-token, no max-retries, no maxBatchSize/uploadTasks in Validate output). Returns `url.String()`.

**`Configuration()` fields (exact order):** `RequiredStringField("Bucket",...)`, `OptionalStringField("Prefix",...)`, `OptionalStringField("Region",...)`, `OptionalPasswordField("Access Key ID","The AWS AWS Key ID",...)`, `OptionalPasswordField("Secret Access Key",...)`, `OptionalStringField("Endpoint",...)`.

**`ExampleURL`:** `"s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"`.

### 3.2 Kafka

**Scheme/aliases:** `"kafka"`, no aliases. Driver + importer.

**`connect(url)`:** parse; require non-empty path (else `"kafka url requires a path which is the topic"`); `host=u.Host`, `topic=u.Path[1:]`. Builds `gokafka.Writer{ Addr:gokafka.TCP(host), Topic:topic, Balancer:&messageBalancer{}, AllowAutoTopicCreation:true, MaxAttempts:25, RequiredAcks:gokafka.RequireAll }`.

**Partitioner (`messageBalancer.Balance`):** if `len(partitions)==1` → that partition. Else scan headers for `eds-partitionkey` → `util.Modulo(util.Hash(string(value)), len(partitions))`. Else fall back to `util.Modulo(util.Hash(string(msg.Key)), len(partitions))`.
- `util.Hash(v...)`: xxhash over `fmt.Sprintf("%+v", v)` bytes, formatted `"%x"` (lowercase hex).
- `util.Modulo(value, num)`: FNV-32a hash of `value`, `int(sum32) % num`, negated if `<0`.

**`process(event, dryRun)`:**
- `key = fmt.Sprintf("dbchange.%s.%s.%s.%s.%s", Table, Operation, companyID|"NONE", locationID|"NONE", ID)`
- `partitionkey = fmt.Sprintf("%s.%s.%s.%s", Table, companyID|"NONE", locationID|"NONE", primaryKey)` where pk = `GetPrimaryKey()`.
- `strWithDef(val,def)`: returns `def` if `val==nil || *val==""`.
- dryRun → trace; else append `gokafka.Message{Key:[]byte(key), Value:[]byte(JSONStringify(event)), Headers:[{Key:"eds-partitionkey", Value:[]byte(partitionkey)}]}` to `p.pending`. If `len(pending) >= 1000` → call `Flush`.

**`Process`:** `waitGroup.Add(1)`/`defer Done`, call `process(event,false)`, returns `(false, nil)` (so the consumer's MaxBatchSize=-1 governs flush cadence, but the internal 1000 cap also force-flushes).

**`Flush`:** `waitGroup.Add(1)`/`defer Done`. If `len(pending)>0`: `ts=now`; loop while `time.Since(ts) < 10s`: `writer.WriteMessages(ctx, pending...)`. On error containing `"Leader Not Available"` → debug `waiting for kafka to become available`, `time.Sleep(1s)`, continue. On any other error → `return fmt.Errorf("error publishing message. %w", err)` (pending NOT cleared → NAK). On success → debug `flushed %d messages`, break. **After the loop `pending = nil` unconditionally.**

**`MaxBatchSize()` = `-1`.**

**`Stop`:** `once.Do`: `waitGroup.Wait()`; if writer != nil `writer.Close()`, set nil. Debug logs around each step.

**Import:** nil if `SchemaOnly`; prefix `[kafka]`, store ctx/config, `connect`, `importer.Run`. `ImportEvent` → `process(event, importConfig.DryRun)`. `ImportCompleted` → `Flush` then `writer.Close()`. `SupportsDelete` → `false`. `Test` → `connect` then `writer.Close()`.

**`Configuration()`:** `RequiredStringField("Hostname",...)`, `OptionalNumberField("Port",..., IntPointer(9092))`, `RequiredStringField("Topic",...)`. **`Validate`** → `fmt.Sprintf("kafka://%s:%d/%s", hostname, port(def 9092), topic)`.

**`ExampleURL`:** `"kafka://kafka:9092/topic"`. **Help** documents partition-key format `[TABLE].[COMPANY_ID].[LOCATION_ID].[PRIMARY_KEY]` and message-key format `dbchange.[TABLE].[OPERATION].[COMPANY_ID].[LOCATION_ID].[MESSAGE_ID]`.

### 3.3 EventHub

**Scheme/aliases:** `"eventhub"`, no aliases. Driver + importer.

**Connection string:** `ParseConnectionString` parses the url, **sets scheme to `"sb"`**, returns `"Endpoint=" + u.String()`. `newProducerClient` calls `azeventhubs.NewProducerClientFromConnectionString(connStr, "", nil)` (empty event-hub name → `EntityPath` from the connection string).

**`Process`:** `waitGroup.Add(1)`/defer Done; `batcher.Add(&event)`; returns `(false, nil)`. No size-based flush in streaming mode (relies on consumer Flush + `Stop`).

**`Flush` algorithm:** `records=batcher.Records()`, `count=batcher.Len()`. If `count>0`: `batcher.Clear()`. Iterate records in order:
- `companyId`/`locationId` read from `record.Object["companyId"]`/`["locationId"]` if they are strings (else "").
- `getKeys` → `key = "dbchange.<table>.<op>.<company|NONE>.<location|NONE>.<id>"`, `partitionKey = NewPartitionKey(table,&company,&location,id) = "<table>.<company|NONE>.<location|NONE>.<id>"`.
- **Batch grouping**: maintains `pendingPartitionKey`. If equal to current `partitionKey`, reuse last batch (`batches[len-1]`); otherwise create a new `EventDataBatch` with `EventDataBatchOptions{PartitionKey:&partitionKey}`, set `pendingPartitionKey`, append. (Only consecutive equal keys are coalesced — a key reappearing later starts a new batch.)
- `addEventToBatch`: `batch.AddEventData(&EventData{ Body:[]byte(JSONStringify(record.Event)), MessageID:&record.Event.ID, ContentType:&"application/json", Properties:{"objectId": key} }, nil)`.
Then send each batch: if `dryRun` → trace `would send batch (%03d/%03d) with count: %d, bytes: %d` (`1+offset`, `count`, `batch.NumEvents()`, `batch.NumBytes()`); else trace `sending batch ...` and `producer.SendEventDataBatch(ctx, batch, nil)`. `offset += batch.NumEvents()`.

**`MaxBatchSize()` = `-1`.**

**`Stop`:** `once.Do`: `Flush`, `waitGroup.Wait()`, `producer.Close(context.Background())`.

**Import:** nil if `SchemaOnly`; prefix `[eventhub]`, `connect`, set `config.Context`/`dryRun`/`importConfig`, `batcher=NewBatcher()`, `importer.Run`. `ImportEvent` → `batcher.Add`; if `Len>=100` → `Flush`. `ImportCompleted` → if batcher != nil `Flush` (always true after Import) else `producer.Close`. `SupportsDelete` → `false`. `Test` → `connect` then `producer.Close`.

**`Configuration()`:** single `RequiredStringField("Connection String", "The connection string primary key from the Event Hub console.", nil)`.
**`Validate`:** value must `HasPrefix("Endpoint=")` (else field error "expected to start with the prefix Endpoint="); must contain `"://"` (else "expected a url scheme after Endpoint= prefix"); returns `"eventhub://" + val[i+3:]` (everything after `://`). E.g. `Endpoint=sb://ns.servicebus.windows.net/;...;EntityPath=hub` → `eventhub://ns.servicebus.windows.net/;...;EntityPath=hub`.

**`ExampleURL`:** `"eventhub://my-eventhub.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=YXNkZmFzZGZhc2RmYXNkZmFzZGZhcwo=;EntityPath=my-eventhub"`.

### 3.4 File

**Scheme/aliases:** `"file"`, no aliases. Driver + importer.

**`GetPathFromURL(url)`:** parse; require non-empty `u.Path` (else `"path is required in url which should be the directory to store files"`). If `u.Path[0:1]=="/"` → `p.dir=u.Path`; else `p.dir, err = filepath.Abs(p.dir)` (NOTE: operates on the previously-set `p.dir`, NOT on `u.Path` — see §5). If `!util.Exists(p.dir)` → `os.MkdirAll(p.dir, 0755)`. Returns `p.dir`.

**Filename (`getFileName`):** `fmt.Sprintf("%s/%d-%s.json", table, ts.Unix(), id)` → `<table>/<unixSECONDS>-<id>.json`.

**`writeEvent(logger,event,dryRun)`:** `key=getFileName(event.Table, time.UnixMilli(event.Timestamp), event.GetPrimaryKey())`; `buf=JSONStringify(event)`; `fp=filepath.Join(dir,key)`. If not dryRun: ensure `filepath.Dir(fp)` exists (`MkdirAll 0755`), `os.WriteFile(fp, buf, 0644)`, trace `stored %s`. Else trace `would have stored %s`.

**`Process`** → `writeEvent(false)`, returns `(false,nil)`. **`Flush`** no-op (nil). **`MaxBatchSize`** = `-1`. **`Stop`** no-op. **`Start`** → prefix `[file]`, `GetPathFromURL(pc.URL)`.

**Import:** nil if `SchemaOnly`; prefix `[file]`, `GetPathFromURL`, store config, `importer.Run`. `ImportEvent` → `writeEvent(importConfig.DryRun)`. `ImportCompleted`/`CreateDatasource` no-op. `SupportsDelete` → `false`. `Test` → `GetPathFromURL(url)` only.

**`Configuration()`:** single `RequiredStringField("Directory", "The directory on the server to store files", nil)`.
**`Validate`:** reject `dir=="/"` ("cannot be the root directory"); `filepath.Abs`; if dir doesn't exist → check PARENT writable via `util.IsDirWritable`; else check dir writable. Returns `"file://" + filepath.ToSlash(absdir)`. `IsDirWritable` (windows build): stat, must be dir, owner-write permission bit set (`Perm()&(1<<7)`); (non-windows) additionally checks `os.Geteuid()==stat.Uid`.

**`ExampleURL`:** `"file://folder"`. **Help:** `"Provide a directory in the URL path to store events into this folder.\n"`.

### 3.5 Import flow (`importer.Run`, shared by all four)

`Run` gets latest schema; if `!NoDelete` calls `handler.CreateDatasource(schema)`; if `SchemaOnly` returns nil; lists files in `DataDir` (`util.ListDir`, recursive, skips `.DS_Store`); for each file parses CRDB changefeed filename via regex `^(\d{33})-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz` (`ParseCRDBExportFile` → table + precise timestamp); skips files whose table isn't in `config.Tables`; NDJSON-decodes each line into a synthetic `DBChangeEvent` (`Operation="INSERT"`, `Imported=true`, `ID=Hash(basename)`, `Timestamp=tv.UnixMilli()`, `MVCCTimestamp=fmt("%v",tv.UnixNano())`, `Key=[GetPrimaryKey()]`, ModelVersion from schema); optional schema validation may set `SchemaValidatedPath`; calls `handler.ImportEvent`; finally `handler.ImportCompleted`.

### 3.6 cmd shims & build tags

Each `cmd/driver_<x>.go` has a build constraint `//go:build use_<x> || !use_custom_driver` and a single blank import of the driver package, e.g.:
```go
//go:build use_s3 || !use_custom_driver
package cmd
import _ "github.com/shopmonkeyus/eds/internal/drivers/s3"
```
The blank import triggers the driver package's `init()` (which calls `internal.RegisterDriver`/`RegisterImporter`). **Selection logic:**
- Default build (no tags): `use_custom_driver` is false, so `!use_custom_driver` is true → **every** driver compiles in.
- `go build -tags use_custom_driver`: disables the catch-all; only files whose specific `use_<x>` tag is also passed compile. e.g. `-tags "use_custom_driver use_kafka use_file"` → only Kafka + File.
- Tag mapping per file: `driver_s3.go`→`use_s3`; `driver_kafka.go`→`use_kafka`; `driver_eventhub.go`→`use_eventhub`; `driver_file.go`→`use_file`; `driver_mysql.go`→`use_mysql`; `driver_postgres.go`→`use_postgres || use_postgresql` (two aliases); `driver_snowflake.go`→`use_snowflak` (**typo: missing trailing "e"** — must build with `use_snowflak`, not `use_snowflake`, to selectively include Snowflake); `driver_sqlserver.go`→`use_sqlserver`.

---

## 4. External dependencies

| Go package | Role | .NET / C# equivalent |
|---|---|---|
| `github.com/aws/aws-sdk-go-v2/{aws,config,credentials,service/s3,aws/retry,aws/signer/v4}` | S3 client, static creds, custom endpoint resolver, standard retryer, SigV4 re-signing for GCS | `AWSSDK.S3` (NuGet). `AmazonS3Config{ServiceURL, ForcePathStyle=true}`, `BasicAWSCredentials`/`SessionAWSCredentials`, `RetryPolicy`/`StandardRetryMode`. SigV4 re-sign workaround → custom `DelegatingHandler` (`PipelineCustomizer`/`IHttpRequestFactory`). |
| `github.com/segmentio/kafka-go` | Kafka producer (`Writer`, custom `Balancer`, headers, acks) | `Confluent.Kafka` (NuGet). `ProducerBuilder<byte[],byte[]>`, `Acks.All`, `IPartitioner` for custom balancing, `Headers`. |
| `github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs` | Event Hubs `ProducerClient`, `EventDataBatch`, partition keys | `Azure.Messaging.EventHubs` (NuGet). `EventHubProducerClient`, `EventDataBatch`, `CreateBatchOptions{PartitionKey}`, `EventData`. |
| `github.com/cespare/xxhash/v2` | xxHash for partition-key hashing (`util.Hash`) | `System.IO.Hashing.XxHash64` (NuGet `System.IO.Hashing`). Must format as lowercase hex of the 64-bit sum. |
| `hash/fnv` (stdlib) | FNV-32a for `util.Modulo` partition selection | Implement FNV-1a 32-bit manually (offset basis `2166136261`, prime `16777619`); no BCL type. |
| `github.com/savsgio/gotils/strconv` | zero-copy `string`→`[]byte` (`S2B`) | `Encoding.ASCII/UTF8.GetBytes` or `MemoryMarshal` over UTF-8 span. |
| `github.com/fatih/color` | ANSI color in `GenerateHelpSection` | `Spectre.Console` or raw ANSI; or strip for non-TTY. |
| `github.com/charmbracelet/x/ansi` (`ansi.Strip`) | strip ANSI in `GetDriverConfigurations` | regex strip of `\x1b\[[0-9;]*m`. |
| `github.com/nats-io/nats.go/jetstream` | `jetstream.Msg` field on event (not serialized) | N/A for these drivers (NATS layer is elsewhere). |
| `github.com/shopmonkeyus/go-common/logger` | structured leveled logger (`WithPrefix`, Debug/Info/Trace/Warn) | `Microsoft.Extensions.Logging.ILogger` + scopes, or Serilog. |
| stdlib `encoding/json` | event serialization (`json.Marshal`) | `System.Text.Json` (mind escaping/ordering — §6). |
| stdlib `net/url`, `path`, `path/filepath`, `os`, `context`, `sync`, `time` | URL parsing, path joining (forward-slash `path.Join` for S3 keys vs OS-specific `filepath.Join` for File), goroutines/WaitGroups, MkdirAll/WriteFile | `System.Uri`, `string.Join`/`Path.Combine`, `Directory`/`File`, `CancellationToken`, `Task`/`SemaphoreSlim`/`Channel<T>`, `DateTimeOffset`. |

---

## 5. Edge cases & gotchas

**S3:**
- `MaxBatchSize()` returns the literal `1_000` and ignores the `?maxBatchSize=` value (which only sizes the internal channel and worker count). Port must keep both behaviors.
- `run()` uses `context.Background()` for `PutObject`, so cancellation does NOT abort in-flight uploads.
- **Potential Stop deadlock:** `Stop()` calls `cancel()` then `jobWaitGroup.Wait()` before `close(ch)`. If workers select `ctx.Done()` while jobs remain buffered in `ch`, those jobs' `jobWaitGroup` is never `Done()` → `Wait()` blocks forever. In practice `Flush()` (which `jobWaitGroup.Wait()`s) is called before `Stop`, masking this. A C# port should drain/flush before cancelling.
- Object key uses `time.UnixMilli(Timestamp).Unix()` → **seconds** (Timestamp is ms). Two events for the same table+pk within the same second overwrite each other.
- `Validate` produces `s3://<endpoint>/<bucket>` when an Endpoint is given, but `getCloudProvider` only recognizes localhost/google; any other custom endpoint is treated as `awsProvider`, where `getBucketInfo` sets `bucket = u.Host` (the endpoint), NOT the path bucket — i.e. the Endpoint-config path is effectively broken for generic S3-compatible hosts. Reproduce or fix deliberately, but be aware.
- Errors are aggregated only at `Flush` via the buffered `errors` channel (size = configured maxBatchSize). A full channel could block a worker.
- GCS requires the `RecalculateV4Signature` round-tripper because the SDK's auto `Accept-Encoding` header breaks the precomputed signature; date parsed with layout `20060102T150405Z`.

**Kafka:**
- **Silent data loss on Flush timeout:** if every `WriteMessages` attempt returns an error containing `"Leader Not Available"` for ≥10s, the `for` loop exits, `pending=nil`, and `Flush` returns `nil` — events are dropped without error/NAK. Any non-leader error returns immediately and preserves `pending` (NAK). Replicate this exact branching.
- Partition selection is custom and must match byte-for-byte: `Hash` = lowercase-hex xxHash64 of `fmt.Sprintf("%+v", value)`; `Modulo` = abs(FNV-1a-32(hexstring)) % partitions. The hashed input is the *hex string*, not raw bytes.
- `strWithDef` substitutes `"NONE"` for nil-or-empty company/location IDs in both key and partition key.
- 1000-message internal cap force-flushes inside `process` even though `MaxBatchSize()` is `-1`.

**EventHub:**
- Batch coalescing only compares the *immediately previous* partition key, so an interleaved key stream produces many small batches; group ordering preserves input order. Reproduce the consecutive-only grouping.
- `connect` mutates the URL scheme to `"sb"`; `MessageID` = event ID; custom property `objectId` carries the dbchange key.
- In streaming mode there is no automatic flush; unbounded batcher growth between flushes. `Stop` flushes once via `sync.Once`.
- `Process`/`Flush` add to `waitGroup` but `Flush` is itself called from `Stop` inside `once.Do` — re-entrancy is fine because `waitGroup` counters balance.

**File:**
- **Relative-path bug:** when `u.Path` doesn't start with `/`, `GetPathFromURL` computes `filepath.Abs(p.dir)` using the (initially empty) struct field, not the URL path — relative URL paths are effectively unusable.
- **Windows drive-letter loss:** `Validate` emits `file://C:/foo`; `url.Parse` yields `Host="C:"`, `Path="/foo"`, and `GetPathFromURL` then sets `dir="/foo"`, dropping the drive. Cross-platform port must handle Windows file URIs explicitly (don't naively split host/path).
- File perms `0644`, dirs `0755` (no-op semantics on Windows; the .NET port can ignore Unix mode bits but should still create dirs).
- Same second-resolution overwrite issue as S3 (`ts.Unix()`).
- `IsDirWritable` differs by OS (the windows build skips the euid/uid ownership check). Replicate platform-appropriate writability checks.

**General:**
- JSON: Go `json.Marshal` HTML-escapes `<`,`>`,`&`,`U+2028`,`U+2029` and emits map keys sorted; `omitempty` drops zero values; `before`/`after` raw JSON passed through unchanged. Byte-for-byte parity with C# requires care (§6).
- `strWithDef` / `GetPrimaryKey` null handling: pointer fields (`CompanyID`,`LocationID`,`UserID`) may be nil; `GetPrimaryKey` returns last `Key[]` element, else `object["id"]`, else `""`.
- None of these four implement `Aliases()` or `DriverMigration`, so `SupportsMigration=false` and only the registered scheme matches.
- `Process` always returns `flush=false`; the driver never asks the consumer to flush — flush cadence is external (or the internal Kafka 1000 cap).

---

## 6. C# port notes

- **Driver abstraction:** define interfaces mirroring `Driver`/`DriverLifecycle`/`DriverHelp`/`Importer`/`ImporterHelp`/`Handler`. Use a static registry (`ConcurrentDictionary<string, IDriver>`) populated at startup; replace Go's `init()`-based self-registration with explicit registration or assembly-scan/DI. The build-tag mechanism maps cleanly to **csproj conditional compilation symbols** (`<DefineConstants>`) + `#if USE_KAFKA`, or to a plugin/DI registration list. Default = all drivers; "custom" build = only selected. Preserve the Snowflake tag typo behavior only if you must match the Go build matrix; otherwise document it.
- **JSON:** use `System.Text.Json`. To approximate Go's escaping, the closest is the default encoder (which escapes `<`,`>`,`&` and non-ASCII). Exact parity is not guaranteed (Go does NOT escape all non-ASCII, but C# default does); if downstream consumers are byte-sensitive, write a custom `JavaScriptEncoder` that escapes only `<`,`>`,`&`,`U+2028`,`U+2029`. **Sort object/dictionary keys** to match Go's map ordering, and preserve `before`/`after` as raw `JsonElement`/`JsonDocument` pass-through (do not re-serialize). Honor `omitempty` via `JsonIgnoreCondition.WhenWritingDefault`/`WhenWritingNull` and match property order to the Go struct order.
- **Hashing:** implement `Hash` with `System.IO.Hashing.XxHash64` and format the 8-byte result as lowercase hex (matching `fmt.Sprintf("%x", h.Sum(nil))`, big-endian byte order as Go's `Sum` produces). Note the hashed input is `fmt.Sprintf("%+v", value)` of the string — for a plain string that's just the string. Implement FNV-1a-32 by hand for `Modulo` (abs after `%`).
- **S3:** `AWSSDK.S3` with `ForcePathStyle=true`, `ServiceURL` for localstack/GCS, `BasicAWSCredentials`/`SessionAWSCredentials`. Replace the worker-pool (`chan job` + N goroutines + two `WaitGroup`s) with a bounded `Channel<Job>` and `uploadTasks` consumer `Task`s, plus a `ConcurrentBag<Exception>` drained in `Flush`. Keep `MaxBatchSize()==1000` hard-coded. For GCS, replicate the re-sign `DelegatingHandler` (strip+restore `Accept-Encoding`, re-sign). Use forward-slash key joining (object keys are S3 paths, not OS paths).
- **Kafka:** `Confluent.Kafka` `IProducer<byte[],byte[]>` with `Acks.All`, `AllowAutoCreateTopics=true`, `MessageSendMaxRetries≈25`, and a custom `IPartitioner` reproducing `messageBalancer`. Carefully replicate the 10-second `Leader Not Available` retry loop and its silent-drop-on-timeout behavior (or fix it deliberately and document the divergence). Use a `List<Message>` buffer with the 1000 force-flush.
- **EventHub:** `Azure.Messaging.EventHubs.Producer.EventHubProducerClient` from the connection string (rewrite scheme to `sb://`, prefix `Endpoint=`). Use `CreateBatchAsync(new CreateBatchOptions{PartitionKey=...})`; replicate consecutive-key batch coalescing exactly; set `EventData.MessageId`, `ContentType="application/json"`, and property `objectId`. Guard flush with a single-shot flag (`Interlocked`/`SemaphoreSlim`) like `sync.Once`.
- **File:** use `Directory.CreateDirectory` + `File.WriteAllBytes`. **Handle Windows file URIs explicitly** (use `new Uri(...).LocalPath` rather than splitting host/path) to avoid the drive-letter-loss bug — decide whether to faithfully reproduce the Go bug or correct it (recommend correcting, with a note). Filenames use `<table>/<unixSeconds>-<pk>.json`; create the table subdirectory before writing.
- **Concurrency:** Go `sync.WaitGroup` → `CountdownEvent`/`Task.WhenAll`/`SemaphoreSlim`; `sync.Once` → `Lazy<T>`/`Interlocked.CompareExchange`; `context.Context`+cancel → `CancellationTokenSource`. Mirror the ordering of cancel/wait/close in `Stop` but fix the potential S3 deadlock (drain the channel before completing).
- **Timestamps:** `time.UnixMilli(ts).Unix()` → `DateTimeOffset.FromUnixTimeMilliseconds(ts).ToUnixTimeSeconds()`. Be aware of the sub-second overwrite collision.
- **Risks to watch:** byte-exact JSON parity, partition-hash parity (xxhash hex + FNV mod), Kafka silent flush-timeout loss, S3 Stop deadlock, Windows file-URI handling, and the Endpoint-config S3 bucket-parsing mismatch.