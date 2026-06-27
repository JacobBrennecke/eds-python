# Behavioral Specification: `metrics-registry` Subsystem

## 1. Purpose

This subsystem provides two loosely related concerns that share the "instrumentation/metadata" role in the EDS consumer:

- **Metrics** (`internal/metrics.go`, package `internal`): Defines the process-wide Prometheus instruments that the consumer pipeline updates (pending events, total events, flush duration, flush count, processing latency). It also exposes a snapshot API (`GetSystemStats`) that combines current metric values with OS-level memory and CPU-load statistics. These metrics are scraped by Prometheus and/or reported up to the Shopmonkey control plane via the consumer's stats payload.
- **Schema Registry table sorting** (`internal/registry/registry.go`, package `registry`): A small helper that re-keys the API-returned schema map (keyed by *object* name) into a map keyed by *table* name, and builds the reverse lookup (table name → API object name) used when fetching versioned schemas from the API.

---

## 2. Public surface

### 2.1 `internal/metrics.go` (package `internal`)

**Package-level exported variables (the Prometheus instruments):**

```go
var PendingEvents prometheus.Gauge       // current count of in-flight/pending events
var TotalEvents prometheus.Counter       // monotonically increasing total events processed
var FlushDuration prometheus.Histogram   // seconds spent per driver flush
var FlushCount prometheus.Histogram      // number of events per flush
var ProcessingDuration prometheus.Histogram // seconds from receive to flush
```

**Exported struct `SystemStats`** (note the anonymous nested struct for `Metrics`):

```go
type SystemStats struct {
	Metrics struct {
		FlushCount         float64 `json:"flushCount"`
		FlushDuration      float64 `json:"flushDuration"`
		ProcessingDuration float64 `json:"processingDuration"`
		PendingEvents      float64 `json:"pendingEvents"`
		TotalEvents        float64 `json:"totalEvents"`
	} `json:"metrics"`
	Memory *mem.VirtualMemoryStat `json:"memory"`
	Load   *load.AvgStat          `json:"load"`
}
```

- `Memory` is a pointer to gopsutil's `mem.VirtualMemoryStat` (serialized with its own lowercase/camelCase JSON tags: `total`, `available`, `used`, `usedPercent`, `free`, `active`, `inactive`, `wired`, `buffers`, `cached`, `swapTotal`, `swapFree`, etc. — the full gopsutil struct).
- `Load` is a pointer to gopsutil's `load.AvgStat`, which serializes as:
  ```go
  type AvgStat struct {
      Load1  float64 `json:"load1"`
      Load5  float64 `json:"load5"`
      Load15 float64 `json:"load15"`
  }
  ```

**Exported functions:**

```go
func MetricsReset()                      // unregister + recreate all metrics; testing only
func GetSystemStats() (*SystemStats, error)
```

**Unexported functions** (relevant to behavior):

```go
func createCounters()                                          // builds & registers the 5 metrics
func init()                                                    // calls createCounters() at package load
func collect(col prometheus.Collector, do func(*dto.Metric))  // channel-based metric collection
func getMetricValue(col prometheus.Collector) float64         // sum / sample-count extraction
```

### 2.2 `internal/registry/registry.go` (package `registry`)

```go
type tableToObjectNameMap map[string]string  // unexported; table name -> API object name

func sortTable(tables internal.SchemaMap) (internal.SchemaMap, tableToObjectNameMap)  // unexported
```

Both are unexported, but `sortTable` is a critical pipeline step (called from `newAPIRegistryModified` in `internal/registry/api.go`). The relevant cross-package types are:

```go
// from internal/schema.go
type SchemaMap map[string]*Schema   // map of table/object name -> *Schema

type Schema struct {
	Properties   map[string]SchemaProperty `json:"properties"`
	Required     []string                  `json:"required"`
	PrimaryKeys  []string                  `json:"primaryKeys"`
	Table        string                    `json:"table"`
	ModelVersion string                    `json:"modelVersion"`
	columns      []string // unexported cache
}
```

---

## 3. Behavior & algorithms

### 3.1 Metric creation — `createCounters()`

Creates all five instruments via `promauto.New*`, which **auto-registers** each with `prometheus.DefaultRegisterer`. Exact definitions (names, help text, buckets) — these MUST be reproduced verbatim:

| Var | Type | `Name` | `Help` | Buckets |
|---|---|---|---|---|
| `PendingEvents` | Gauge | `eds_pending_events` | `The number of pending events` | — |
| `TotalEvents` | Counter | `eds_total_events` | `The total number of events processed` | — |
| `FlushDuration` | Histogram | `eds_flush_duration_seconds` | `The duration of driver flushes` | `{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}` |
| `FlushCount` | Histogram | `eds_flush_count` | `The count of events flushed` | `{1, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}` |
| `ProcessingDuration` | Histogram | `eds_processing_duration_seconds` | `The latency in duration of processing events from receving them to flushing them` | `{1, 2, 3, 5, 10, 60, 300, 600, 1800, 3600}` |

Notes:
- The `ProcessingDuration` help text contains a typo: **"receving"** (not "receiving"). Preserve it exactly for fidelity.
- Bucket slices are explicit `[]float64` (Prometheus does **not** auto-append `+Inf` to the slice you provide; the `+Inf` bucket is added internally by the histogram implementation).
- `init()` runs `createCounters()` once at package load. There is no guard; calling `createCounters()` again without first unregistering would **panic** (duplicate registration). This is why `MetricsReset` unregisters first.

### 3.2 `MetricsReset()`

Testing-only. Sequence:
1. `prometheus.DefaultRegisterer.Unregister(...)` for all five instruments (in declaration order: PendingEvents, TotalEvents, FlushDuration, FlushCount, ProcessingDuration). `Unregister` returns a bool that is ignored.
2. Calls `createCounters()` to recreate and re-register fresh instruments (resets all values to zero / empty histograms).

### 3.3 `collect(col, do)`

Generic helper that drives a `prometheus.Collector`'s `Collect` method:
1. Creates an unbuffered channel `c := make(chan prometheus.Metric)`.
2. Launches a goroutine: `col.Collect(c)` then `close(c)`.
3. `for x := range c` — for each emitted `prometheus.Metric`, allocates a `dto.Metric{}`, calls `x.Write(&m)` (error ignored via `_ =`), and invokes the `do(&m)` callback.

The `range` over multiple metrics handles label-vector cases (multiple series), though none of the five metrics here use labels — each emits exactly one `dto.Metric`.

### 3.4 `getMetricValue(col) float64`

Sums values across collected metrics:
```go
collect(col, func(m *dto.Metric) {
    if h := m.GetHistogram(); h != nil {
        total += float64(h.GetSampleCount())   // histogram -> COUNT of observations
    } else {
        total += m.GetCounter().GetValue()     // otherwise -> counter value
    }
})
```

Behavioral details that MUST be replicated exactly:
- **For Histograms** (`FlushDuration`, `FlushCount`, `ProcessingDuration`): the returned value is the **number of samples observed** (`SampleCount`), NOT the sum of observed values and NOT any bucket. So `s.Metrics.FlushDuration` actually carries the *count of flushes*, `s.Metrics.FlushCount` carries the *count of flush operations*, and `s.Metrics.ProcessingDuration` carries the *count of processing observations*. The field names are misleading but the behavior is "sample count."
- **For the Counter** (`TotalEvents`): returns the counter's current value.
- **For the Gauge** (`PendingEvents`): the `else` branch calls `m.GetCounter()`. For a Gauge metric the `Counter` field of the dto is `nil`; protobuf getters are nil-safe, so `(*Counter)(nil).GetValue()` returns **`0`**. Therefore **`getMetricValue(PendingEvents)` always returns `0`**, regardless of the gauge's actual value. This is confirmed by the consumer tests (`PendingEvents` asserted `== 0` both before and after processing an event). A faithful port must reproduce this: `SystemStats.Metrics.PendingEvents` is effectively always `0`.

### 3.5 `GetSystemStats() (*SystemStats, error)`

Builds a snapshot in this exact order:
1. `s.Metrics.FlushCount = getMetricValue(FlushCount)` (sample count of FlushCount histogram)
2. `s.Metrics.FlushDuration = getMetricValue(FlushDuration)` (sample count of FlushDuration histogram)
3. `s.Metrics.PendingEvents = getMetricValue(PendingEvents)` (always 0, per 3.4)
4. `s.Metrics.TotalEvents = getMetricValue(TotalEvents)` (counter value)
5. `s.Metrics.ProcessingDuration = getMetricValue(ProcessingDuration)` (sample count)
6. `s.Memory, err = mem.VirtualMemory()` — if `err != nil`, return `nil, err` immediately.
7. `s.Load, err = load.Avg()` — return `&s, err` (the load error, possibly nil, is returned together with the populated struct; note that on the load step it does NOT null out `s` on error — it returns the partially populated struct pointer alongside any error).

### 3.6 How the metrics are mutated elsewhere (context from `internal/consumer/consumer.go`)

For port fidelity, the call sites determine units/semantics:
- On receiving/enqueuing an event: `internal.PendingEvents.Inc()` and `internal.TotalEvents.Inc()` (consumer.go ~493–494).
- On each acked message during flush: `internal.PendingEvents.Dec()` and `count++` (a `float64` counter local).
- On various nack/error paths: `internal.PendingEvents.Dec()`.
- On flush completion:
  - `internal.ProcessingDuration.Observe(processingDuration.Seconds())` — observed in **seconds** (only if `pendingStarted != nil`).
  - `internal.FlushDuration.Observe(time.Since(started).Seconds())` — **seconds**.
  - `internal.FlushCount.Observe(count)` — count of events flushed (a `float64`).

So histograms are fed seconds (duration ones) and raw counts (FlushCount), consistent with their bucket scales.

### 3.7 `sortTable(tables)` — registry table re-keying

```go
func sortTable(tables internal.SchemaMap) (internal.SchemaMap, tableToObjectNameMap) {
	kv := make(internal.SchemaMap)
	otm := make(tableToObjectNameMap)
	for object, d := range tables {
		otm[d.Table] = object   // table name -> object (API) name
		kv[d.Table] = d         // re-key schema map by table name
	}
	return kv, otm
}
```

Algorithm:
- Input `tables` is the API response decoded into a `SchemaMap`, **keyed by the API "object" name** (the JSON object keys returned from `GET /v3/schema`).
- Output 1 (`kv`): the same `*Schema` values, **re-keyed by `d.Table`** (the `table` field inside each schema).
- Output 2 (`otm`): reverse map from `d.Table` → original `object` key.
- Iteration uses Go map ranging, so order is **non-deterministic**. This matters only if two distinct objects share the same `d.Table` value — in that case the **last one iterated wins** for both `kv[d.Table]` and `otm[d.Table]` (non-deterministic which). In normal data, object name and table name are effectively 1:1.

**Where `otm` (the `objects` field) is used** (registry/api.go `GetSchema`): when falling back to the API for a versioned schema, it maps table → object:
```go
object := r.objects[table]
if object == "" { object = table }
req, _ := http.NewRequest("GET", r.apiURL+"/v3/schema/"+object+"/"+version, nil)
```
So the reverse map exists to turn an internal table name back into the API object name for the schema-fetch URL, defaulting to the table name itself when no mapping exists.

**Where `sortTable` is called** (registry/api.go `newAPIRegistryModified`, line 225): immediately after decoding the raw schema JSON into `registry.schema`, it does `registry.schema, registry.objects = sortTable(registry.schema)`. After this, the registry caches/tracks each schema keyed by `schema.Table` and `schema.ModelVersion`.

---

## 4. External dependencies

| Go package | Role | Suggested .NET / C# equivalent |
|---|---|---|
| `github.com/prometheus/client_golang/prometheus` | Core metric types (`Gauge`, `Counter`, `Histogram`, `Collector`), `DefaultRegisterer`, `*Opts` config structs | **prometheus-net** (NuGet `prometheus-net`): `Gauge`, `Counter`, `Histogram`, `CollectorRegistry`, `Metrics.DefaultRegistry`. |
| `github.com/prometheus/client_golang/prometheus/promauto` | Factory funcs that construct **and auto-register** metrics with the default registry | prometheus-net's `Metrics.CreateGauge/CreateCounter/CreateHistogram` (auto-register with default registry). |
| `github.com/prometheus/client_model/go` (alias `dto`) | Protobuf wire model (`dto.Metric`) used to read out metric values via `Collector.Collect` + `Metric.Write` | Not needed in prometheus-net — read values directly (`gauge.Value`, `counter.Value`, `histogram.Count`). No protobuf round-trip required. |
| `github.com/shirou/gopsutil/v4/mem` | `mem.VirtualMemory()` → `*VirtualMemoryStat` (system memory snapshot) | **Hardware.Info** NuGet, or `GC.GetGCMemoryInfo()`/`System.Runtime` for managed memory; on Windows use `Microsoft.VisualBasic.Devices.ComputerInfo` or WMI/`PerformanceCounter`; cross-platform read `/proc/meminfo` on Linux. No single BCL equivalent — wrap behind an interface. |
| `github.com/shirou/gopsutil/v4/load` | `load.Avg()` → `*AvgStat` (1/5/15-min load averages) | No BCL equivalent. On Linux read `/proc/loadavg`; Windows has no native load average (gopsutil emulates it). Use **Hardware.Info** or a custom shim; expose `Load1/Load5/Load15`. |
| `github.com/shopmonkeyus/eds/internal` (registry.go) | Provides `SchemaMap` / `Schema` types | Internal project types — port alongside. |

---

## 5. Edge cases & gotchas

1. **PendingEvents always reports 0 in stats.** As detailed in 3.4, `getMetricValue` reads `GetCounter().GetValue()` for the non-histogram path; for a Gauge the dto Counter field is nil → 0. The live gauge value is correct for Prometheus scraping (`Inc`/`Dec` work), but the `SystemStats.Metrics.PendingEvents` snapshot is always 0. Tests confirm and depend on this. A faithful port must return 0 here (or reproduce the same code path), not the real gauge value.

2. **Histogram fields carry sample COUNT, not durations/sums.** `FlushDuration`, `FlushCount`, `ProcessingDuration` in `SystemStats.Metrics` are all `SampleCount`s. Do not "fix" this to report the histogram sum or average — that would diverge from Go behavior.

3. **Concurrency in `collect`.** Uses an unbuffered channel plus a goroutine that calls `Collect` then `close`. If the `do` callback panics, the goroutine would block on send forever (goroutine leak). In practice the callbacks here don't panic. The pattern is required by `client_golang`'s push-style `Collect(chan)` API; prometheus-net exposes values directly, so the C# port can skip the channel/goroutine entirely.

4. **`init()` + duplicate registration panic.** `init()` registers metrics at package load. Re-registering the same metric name with `DefaultRegisterer` panics in `promauto`. `MetricsReset` must unregister first. In .NET, prometheus-net's default registry will **return the existing metric** for an identical definition rather than throw, but a config mismatch throws — replicate the "unregister then recreate" via `Metrics.DefaultRegistry` collector removal or by using a fresh `CollectorRegistry` in tests.

5. **`MetricsReset` is global/process-wide mutable state.** It swaps the package-level vars. In C#, mutable static fields are an anti-pattern for testability; consider a `MetricsRegistry` instance with a reset method, but be careful that consumer code references the same singleton instance.

6. **`GetSystemStats` error handling asymmetry.** A memory error returns `nil, err`; a load error returns `&s, err` (non-nil struct with possibly-incomplete `Load`). Reproduce this exactly.

7. **gopsutil OS-specific behavior.** `load.Avg()` is meaningful on Linux/macOS; on Windows it's emulated (and may return synthesized values or errors). Memory fields vary by OS. A cross-platform .NET port needs an OS abstraction; the JSON shape of `Memory`/`Load` must match if the stats payload is consumed elsewhere.

8. **`sortTable` map-collision non-determinism.** If two objects map to the same `Table`, the surviving entry is whichever the map range visits last — non-deterministic. C# `Dictionary` enumeration order is also unspecified; the "last write wins" overwrite behavior matches naturally if you iterate and assign. Don't add dedup logic that changes which one wins.

9. **`sortTable` input vs output keying.** The input map is keyed by API object name; the output by table name. Downstream code (`GetSchema` URL build, cache/tracker keys) depends on this re-keying. Getting the key direction wrong breaks schema fetching.

10. **JSON tag fidelity.** `SystemStats.Metrics` uses camelCase tags (`flushCount`, `flushDuration`, `processingDuration`, `pendingEvents`, `totalEvents`) under a `metrics` object, plus `memory` and `load`. The embedded gopsutil structs bring their own tags. If the C# stats payload is sent to the same backend, match these exactly (use `JsonPropertyName`).

---

## 6. C# port notes

- **Use prometheus-net.** Map directly:
  - `PendingEvents` → `Gauge` (`Metrics.CreateGauge("eds_pending_events", "The number of pending events")`).
  - `TotalEvents` → `Counter` (`eds_total_events`, "The total number of events processed").
  - `FlushDuration` → `Histogram` with buckets `new[]{.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10}` (`Histogram.ExponentialBuckets` is NOT what you want — supply the explicit array).
  - `FlushCount` → `Histogram` buckets `new[]{1d,10,25,50,100,250,500,1000,2500,5000,10000}`.
  - `ProcessingDuration` → `Histogram` buckets `new[]{1d,2,3,5,10,60,300,600,1800,3600}`. Keep the help text typo "receving" if byte-for-byte fidelity with scraped help is required (otherwise this is cosmetic).
- **Reading values for the snapshot:** prometheus-net exposes `gauge.Value`, `counter.Value`, and `histogram.Count` (sample count) directly — no `Collect`/`dto`/goroutine machinery needed. To preserve exact behavior:
  - `Metrics.FlushCount = flushCountHistogram.Count;`
  - `Metrics.FlushDuration = flushDurationHistogram.Count;`
  - `Metrics.ProcessingDuration = processingDurationHistogram.Count;`
  - `Metrics.TotalEvents = totalEventsCounter.Value;`
  - `Metrics.PendingEvents = 0d;` ← **hard-code 0** to match the Go gauge/counter-getter quirk (document why). Do NOT use `pendingEventsGauge.Value`.
- **Structure:** Prefer a non-static `EdsMetrics` class holding the instruments, registered against a `CollectorRegistry` (default for prod, isolated per-test). Provide a `Reset()` that removes/recreates collectors for the testing path mirroring `MetricsReset`. If the existing port relies on global access (consumer calls `PendingEvents.Inc()`), expose a singleton.
- **SystemStats:** model as a record/class with a nested `Metrics` type and `Memory`/`Load` properties. Use `[JsonPropertyName(...)]` for camelCase tags. Behind `ISystemInfoProvider`, abstract OS memory/load gathering; implement Linux (`/proc/meminfo`, `/proc/loadavg`) and Windows variants. Replicate the error semantics: throw/return on memory failure before populating load; return the struct even if load lookup fails.
- **Histogram seconds:** ensure flush/processing durations are observed in **seconds** (e.g. `stopwatch.Elapsed.TotalSeconds`) to align with the bucket boundaries, matching the Go `.Seconds()` calls.
- **`sortTable`:** port as a private static helper returning a tuple `(SchemaMap kv, Dictionary<string,string> objectMap)`. Iterate the input dictionary (keyed by object name), set `kv[d.Table] = d` and `objectMap[d.Table] = object`. Keep "last write wins" on collisions. Use ordinal string handling for keys (table/object names are ASCII identifiers).
- **Risks to watch:** (1) accidentally reporting the real pending-events gauge value (would diverge from Go and break parity tests); (2) reporting histogram sums/averages instead of counts; (3) wrong key direction in `sortTable`; (4) duplicate-registration exceptions in prometheus-net when tests re-init metrics — use isolated registries per test rather than mutating the default registry.