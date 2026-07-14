# Camus observability guide

Camus exposes operational state as library primitives, not as an exporter or
event framework. Applications can synchronously pull an in-memory `RootStats`
snapshot, inspect `RootHealth`, asynchronously wait for low-frequency health
transitions with `HealthWatch`, and classify returned errors with `ErrorKind`.
None of these APIs performs filesystem I/O or changes durability semantics.

Record readiness remains part of the data API: await `Stream::read`. A health
watch does not report which stream is readable, and Camus has no record-event
callback or reliable event subscription.

## Observation boundary

Observability state belongs to one open session. Commit, maintenance, and
logical-activity counters start at zero when `Log::open` succeeds, while
recovery fields describe work performed by that open. The state is not
persisted in the Camus file format or reconstructed as historical telemetry;
a non-owning health watch may retain only its last published value after the
final root handle is dropped.

`Log::stats` copies a fixed-size root snapshot from memory. Its storage,
commit, maintenance, and recovery fields describe one completed storage
publication. Pressure and logical-operation counters are atomically sampled
while the snapshot is assembled, so they may be slightly newer or older than
one another under concurrency. The API promises a useful operational snapshot,
not a global transaction across every counter.

`Stream::stats` performs one in-memory lookup for a caller-selected stream.
Camus never emits a `StreamId` as an automatic metric label. An application
that deliberately exports per-stream metrics owns their cardinality and may
enumerate `Log::known_streams` itself.

All cumulative counters and duration sums saturate instead of wrapping.
Exporters should tolerate saturation and process restarts.

## Root statistics

`RootStats` separates current state from work performed:

| Group | Meaning |
| --- | --- |
| `storage` | Capacity, pending data, live/sealed/reclaimable segments, and exact charged bytes |
| `pressure` | Command queue, active storage job, and queue/readiness/capacity waits |
| `operations` | Caller-observed append, read, release, and explicit-reclaim outcomes |
| `commits` | Successful append/release durability groups and their grouped units, records, and encoded bytes |
| `maintenance` | Automatic/explicit passes, rollover reasons, reclaimed storage, and manifest compaction |
| `recovery` | Scanned durable structures, permitted repairs or completed lifecycle work, and open duration |

Current gauges such as `queue_depth`, `active_storage_jobs`, pending records,
and capacity bytes should be exported directly. Session counters should
normally be converted to deltas or rates by the application.

Logical operation counters describe what API Futures did:

- `started` increments before input validation or admission;
- `succeeded` and `failed` describe an outcome returned to the caller;
- `cancelled` means the caller dropped the Future before receiving an outcome;
  an already admitted mutation may still finish durably; and
- `records` and `payload_bytes` describe successful call inputs or outputs.

For release, `operations.release.records` includes submitted duplicates and
already-released IDs. `commits.release_records` counts unique records that
actually changed durable pending state. For append and release, commit-group
counters can also advance after the caller has cancelled its Future. This
separation is intentional: operation counters describe the async API boundary,
while commit counters describe completed storage barriers observed by the live
reactor.

`pressure.admitted_commands` counts reactor command admissions, not logical
calls. A capacity-blocked append can be admitted more than once while one
logical append Future remains active. `pressure.queue_wait` counts only
reservations that found the bounded command queue full; readiness and capacity
waits are tracked separately.

## Timing cost

Wait durations and the one recovery duration are always measured. End-to-end
logical-call and storage-job timing is disabled by default to avoid additional
clock reads on every fast path. Enable it when needed:

```rust,no_run
# use camus::{Capacity, Config};
# let root = std::path::Path::new("camus-data");
let config = Config::new(root, Capacity::Unbounded)
    .with_detailed_observability();
```

`RootStats::detailed_timings` states whether those optional measurements are
enabled. When disabled, logical-operation `elapsed` fields and
`pressure.storage_job_elapsed` remain zero even as their counters advance.

`DurationStats` contains an observation count, saturating total, and maximum.
It intentionally does not maintain histograms or quantiles in the storage
library. An application that needs distributions should time its own calls or
feed observations into its chosen telemetry stack.

## Health and failed-closed state

`Log::health` returns the latest `RootHealth`. Normal lifecycle transitions are
`Running -> ShuttingDown -> Closed`. A failed root transitions to `Poisoned`
and eventually `Closed`; a failure during shutdown may also move
`ShuttingDown -> Poisoned -> Closed`.

The first failed-closed cause is retained in `RootHealth::failure` through
closure. It contains:

- low-cardinality `OperationKind` and `ErrorKind` values;
- a conservative `DurabilityOutcome` (`Unknown` when a runtime failure may
  have interrupted admitted mutation work); and
- a human-readable message that may contain paths or other high-cardinality
  detail and must not be used as a metric label.

`ErrorKind`, `OperationKind`, and `RootState` expose stable snake-case labels
through `as_str` and `Display`. These observability enums and snapshot structs
are non-exhaustive so future signals can be added; use a wildcard when matching
enums. Returned non-poisoning errors are not stored in root health; classify
them at the call site with `Error::kind`.

Use `Log::watch_health` when a task needs prompt lifecycle notification:

```rust,no_run
# use camus::{Log, RootState};
async fn observe(log: &Log) {
    let mut watch = log.watch_health();
    while let Some(health) = watch.changed().await {
        eprintln!("camus state={} generation={}", health.state, health.generation);
        if health.state == RootState::Closed {
            break;
        }
    }
}
```

A `HealthWatch` is non-owning: it does not keep the storage root or lock alive.
It coalesces intermediate states, never backpressures the reactor, and returns
`None` after every owning `Log` and `Stream` has gone away. Always inspect the
latest value rather than treating it as an exact event history.

## Adapter guidance

A metrics or monitoring adapter should:

1. choose its own stable root label and poll `Log::stats` at a modest interval;
2. export gauges directly and cumulative fields as restart-aware counters;
3. watch health for prompt poisoned/closed notification;
4. count returned errors by `ErrorKind` at operation call sites; and
5. put failure messages, paths, record IDs, and stream IDs in logs only when
   explicitly useful.

The most actionable baseline signals are root state and first failure,
capacity use versus admissible bytes, pending records and payload bytes,
capacity waiters, queue depth and queue waits, operation failures and
cancellations, commit-group utilization, reclaimable bytes, recovery repairs,
and filesystem free space monitored outside Camus.

Camus deliberately does not depend on tracing, metrics, OpenTelemetry, or a
logging facade. That keeps the persistent-buffer boundary application-neutral
and lets the embedding process choose its telemetry runtime, naming, labels,
sampling, and export policy.
