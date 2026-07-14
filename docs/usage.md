# Camus usage guide

Camus is an embedded asynchronous persistent buffer for opaque records. It
provides a root-wide set of logical streams and one storage lifecycle:

```text
append -> waiting bounded read -> durable external effect -> release -> reclaim
```

An append that reports success remains recoverable until release is durable.
If the process stops after an external effect but before release, the same
record is returned again. Camus deliberately does not create consumers,
record-delivery callbacks, claims, retries, or exactly-once effects.

The public boundary is defined in [README.md](../README.md). On-disk ordering
and format details are in [architecture.md](architecture.md) and
[file-format.md](file-format.md).

## API map

| Goal | API |
| --- | --- |
| Open or recover a root | `Log::open(config).await` |
| Construct a logical handle | `log.stream(StreamId)` |
| Append one durability epoch | `stream.append` / `stream.append_batch` |
| Wait for pending work and read it | `stream.read(ReadLimits).await` |
| Durably remove an exact subset | `stream.release(ids).await` |
| Await one maintenance pass | `log.reclaim().await` |
| Observe current in-memory state | `stats`, `health`, `known_streams`, `id` |
| Await a root lifecycle transition | `log.watch_health().changed().await` |
| Drain and close the root | `log.shutdown().await` |

Potential filesystem work is async. Handle construction and reactor-maintained
observations are synchronous and never hide disk I/O.

## Open a root

Capacity is deliberately explicit. Other execution bounds have defaults.

```rust,no_run
use camus::{Capacity, Config, FullPolicy, Log};

async fn open(root: &std::path::Path) -> camus::Result<Log> {
    Log::open(Config::new(
        root,
        Capacity::Bounded {
            total_bytes: 4 * 1024 * 1024 * 1024,
            when_full: FullPolicy::Block,
        },
    ))
    .await
}
```

One open root owns an exclusive advisory lock. Another process or `Log` receives
`Error::RootInUse`. Opening validates authoritative headers, manifest state,
segment structure, and recovery ordering before returning.

By default Camus lazily creates and retains one private process-wide Tokio
runtime. Applications with an executor abstraction may provide an
`Arc<dyn Runtime>` through `Config::with_runtime`. A runtime must be able to
spawn the long-lived reactor, finite blocking filesystem jobs, and timer
Futures.

## Append opaque records

`StreamId` is a caller-selected `u64`. Every value is legal and there is no
reserved default stream.

```rust,no_run
use bytes::Bytes;
use camus::{Log, Record, StreamId};

async fn stage(log: &Log) -> camus::Result<()> {
    let uploads = log.stream(StreamId::new(7));
    uploads
        .append_batch(vec![
            Record::new(Bytes::from_static(b"image bytes"))
                .with_metadata(Bytes::from_static(b"idempotency-key=request-42")),
            Record::new(Bytes::from_static(b"video bytes")),
        ])
        .await?;
    Ok(())
}
```

Each call is one independent recovery epoch in exactly one logical stream.
Input order is preserved. Camus assigns contiguous stream-local sequences and
returns opaque fixed-size `RecordId` values; it never exposes segment IDs,
offsets, or other physical placement.

`Record` owns immutable `Bytes`. Moving it into the reactor does not deep-copy
metadata or payload. Metadata and payload remain uninterpreted by Camus.

## Wait and read

`Stream::read` is the readiness API. It waits when the stream has no pending
records and returns a non-empty owned snapshot after verifying every selected
metadata and payload checksum.

```rust,no_run
use camus::{ReadLimits, Stream};

async fn next_batch(stream: &Stream) -> camus::Result<()> {
    let snapshot = stream
        .read(ReadLimits::new(128, 8 * 1024 * 1024))
        .await?;
    for record in &snapshot {
        println!("{}: {} bytes", record.id, record.payload.len());
    }
    Ok(())
}
```

The record-count limit and sum of payload bytes are hard bounds. Metadata is
returned but does not count toward `max_bytes`. Camus chooses the longest
fitting prefix of currently pending stream order. If the earliest pending
record alone is too large, `ReadLimitTooSmall` reports its ID and required
payload size.

A read observes shared pending state; it does not claim, reserve, or remove a
record. Multiple handles can receive the same snapshot. One handle's durable
release removes those records from future reads for every handle. Coordinate
workers above Camus or make concurrent duplicate effects safe.

## Release only after the effect is durable

```rust,no_run
use camus::Stream;

async fn deliver(stream: &Stream) -> camus::Result<()> {
    let snapshot = stream.read(camus::ReadLimits::new(32, 1024 * 1024)).await?;
    let mut completed = Vec::new();
    for record in snapshot {
        make_effect_durable(&record).await?;
        completed.push(record.id);
    }
    stream.release(completed).await
}
```

A release is an atomic ensure-not-pending operation for its currently pending
subset. Exact arbitrary subsets are supported. Duplicate IDs and IDs already
released or reclaimed are successful no-ops. IDs from another root or stream,
and well-scoped IDs beyond the durable stream high-water, are typed errors.

The configured `max_release_records` is a hard request bound. Camus never
silently splits a larger request because doing so would weaken its atomic
meaning.

## Capacity behavior

Capacity covers the complete root, not individual streams. It charges exact
encoded format bytes and keeps dynamic maintenance headroom for checkpoint
rewrite, the largest next manifest frame, and an active segment footer.

- `Capacity::Unbounded` applies no Camus byte budget.
- `FullPolicy::RejectNew` returns `RejectedCapacity` before mutating storage.
- `FullPolicy::Block` waits asynchronously outside the command queue so reads,
  releases, and maintenance can make progress.
- A request that cannot fit even after all reclaimable bytes disappear returns
  `ExceedsCapacity` under either policy.

Camus does not inspect filesystem free space. Device-full, quota, and ordinary
I/O failures after admission have uncertain durable outcomes and poison the
open root.

## Rollover and reclamation

Physical segments are root-wide and may interleave records from many logical
streams. `segment_bytes` is a hard final-size limit. `max_segment_age` is an
optional soft reactor deadline; no separate application callback is needed.

Reclamation is automatic low-priority work and is promoted under capacity
pressure. A sealed segment can be deleted only after all of its records are
durably released. One pending record may therefore pin released bytes for
other streams in the same segment. `Log::reclaim` is an optional barrier for a
maintenance pass, so it may return an empty report when automatic work already
won the race.

## Observe a running root

`Log::stats` returns a fixed-size in-memory `RootStats` snapshot grouped into
storage, pressure, logical operations, commit groups, maintenance, and
recovery. `Stream::stats` returns pending state for one selected logical
stream. Neither call performs disk I/O.

Base counters and actual wait durations are always collected. Enable optional
end-to-end logical-call and storage-job timing at open when its extra clock
reads are useful:

```rust,no_run
# use camus::{Capacity, Config};
# let root = std::path::Path::new("camus-data");
let config = Config::new(root, Capacity::Unbounded)
    .with_detailed_observability();
```

`Log::health` is the current root lifecycle. `Log::watch_health` creates a
non-owning, coalescing async watch for prompt transitions such as `Poisoned` or
`Closed`. It is intentionally separate from stream readiness: await
`Stream::read` to know when a stream has pending data.

Classify returned errors with `Error::kind`. The low-cardinality `ErrorKind`
may be a metric label; paths, messages, record IDs, and stream IDs should not
be automatic labels. See [the observability guide](observability.md) for exact
counter, timing, cancellation, and health semantics.

## Cancellation, errors, and shutdown

Dropping an operation Future before command admission is side-effect free.
After admission, cancellation abandons only the result; the reactor completes
the bounded storage operation so an in-progress durability boundary is never
cancelled halfway.

Invalid input, configured-bound failures, scope errors, capacity rejection,
and closure do not poison the root. I/O, authoritative corruption, lazy body
checksum failure, or runtime progress failure do. The triggering operation
gets its specific error; queued and future storage operations receive
`Poisoned`. Close every handle and reopen the root to recover authoritative
state. Never infer absence from an errored append or release.

`shutdown().await` stops new admission, drains already admitted commands,
closes waiting reads and blocked appends, releases the lock, and makes surviving
handles return `Closed`. Cancelling a shutdown Future abandons only that wait;
the reactor continues the already-started shutdown. Dropping the final `Log` or
`Stream` also starts background shutdown; because that path cannot be awaited,
an immediate reopen may briefly receive `RootInUse`.

See [the runnable examples](../examples/README.md) for replay, waiting reads,
multi-stream use, capacity-aware maintenance, and observability.
