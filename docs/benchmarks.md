# Benchmark methodology

Camus benchmarks measure the persistent-buffer contract rather than a generic
in-memory key/value API. Every measured append reports success only after its
durability boundary, and every measured release/delete is durable before it
returns. The suite is a standalone crate under `benchmarks/` so RocksDB's C++
toolchain and native build do not become dependencies of the Camus library.
The runner is repository-only and is excluded from the published crate.

There is no universal checked-in performance threshold. Storage latency,
filesystem behavior, device firmware, CPU power policy, and background I/O
can change results by multiples. A trusted baseline and its candidate must be
collected on the same controlled host, target device, build profile, and
benchmark configuration.

The Camus adapter uses default observability: base counters remain enabled,
while optional end-to-end logical-call and storage-job timing is disabled.
Enable detailed timing in an application only when measuring that production
configuration deliberately.

## Directional Linux reference

The following five-sample Linux/ext4 result uses the baseline profile, a 4 KiB
payload, and storage revision `3d78162`. The engines ran as simple append file,
redb, RocksDB, then Camus. It is a directional comparison, not a universal
target or CI gate. Except for warm restart, each engine cell is
`records/s · p99 ms`; warm-restart cells are `p50 / p99 ms`.

| Workload | Camus | Simple append file | RocksDB | redb | Conclusion |
| --- | ---: | ---: | ---: | ---: | --- |
| Sequential durable append | 941 · 1.670 | 1,122 · 1.208 | 1,083 · 1.373 | 922 · 1.501 | The minimal file leads; Camus trails RocksDB by about 13% without group-commit opportunity. |
| Concurrent append, 1 stream | 13,014 · 1.835 | 1,199 · 25.166 | 7,454 · 2.742 | 972 · 27.607 | Camus leads RocksDB by about 75%; its group commit is effective. |
| Concurrent append, 16 streams | 12,265 · 2.372 | 1,188 · 17.433 | 7,375 · 2.906 | 956 · 33.047 | Camus leads RocksDB by about 66%; logical stream fan-out adds little physical I/O cost. |
| Append batches of 64 | 42,621 · 2.146 | 46,687 · 1.860 | 37,791 · 2.181 | 26,283 · 3.328 | The minimal file leads; Camus is about 13% ahead of RocksDB with similar p99. |
| Cached verified read | 292,893 · 29.000 | 184,168 · 50.430 | 268,880 · 38.732 | 212,628 · 44.827 | Camus leads throughput and has about 25% lower p99 than RocksDB. |
| Release batches of 256 | 255,167 · 1.363 | 266,945 · 2.146 | 246,433 · 1.571 | 227,529 · 2.074 | The minimal file has the highest throughput; Camus has the lowest p99. |
| Read/release drain | 167,844 · 1.944 | 148,223 · 2.273 | 151,590 · 2.226 | 163,507 · 2.474 | Camus leads end-to-end drain, with redb close on throughput. |
| Warm restart, first batch | 18.776 / 20.136 | 17.138 / 17.203 | 262.013 / 266.469 | 1.563 / 2.179 | redb is fastest; Camus is about 14× faster than RocksDB and close to the minimal file. |

Two additional five-sample Camus/RocksDB passes reversed the engine order to
check order sensitivity. Across all three current runs, the winner stayed the
same although the margin varied: Camus trailed sequential durable append by
9–13%, led one-stream concurrent append by 14–75%, led 16-stream concurrent
append by 66–80%, led batch append by 13–21%, led verified read by 9–56%, led
release by 3–8%, and led drain by 11–29%. Camus's warm-restart first-batch p50
was 13–14× faster than RocksDB's. The variability, especially for cached read,
is why these numbers are directional rather than release thresholds.

After the release and drain workloads, the median measured storage footprint
was 176 bytes for Camus, about 12.2 MiB for redb, and about 32.7 MiB for both
the simple append file and RocksDB. This is the immediate post-workload file
length, not a post-compaction RocksDB claim: the harness does not force a
RocksDB compaction after its synchronous deletes.

## Compared engines and semantic mapping

| Engine | Version line | Durable append | Durable release |
| --- | --- | --- | --- |
| Camus | repository checkout | one record epoch or `append_batch`; success follows the segment `sync_data` | exact `release`; success follows manifest `sync_data` |
| Simple append file | built-in benchmark format 1 | one checksummed, committed append frame followed by `sync_data` | one checksummed release frame followed by `sync_data` |
| RocksDB | `rocksdb` 0.24 | WAL enabled, compression disabled, `WriteBatch` with `WriteOptions::set_sync(true)` | synchronous WAL-backed `WriteBatch` deletes |
| redb | `redb` 4.1 | one write transaction with `Durability::Immediate` | one immediate-durability delete transaction |

The simple append-file adapter is a benchmark-only lower-level reference, not
a production storage engine. It keeps one append-only data file and one
append-only release file, rebuilds an in-memory ordered index on open, and
verifies the same per-record payload checksum used by the KV adapters. Atomic
append batches and release batches use checksummed commit frames. A process
mutex serializes calls through each `sync_data`; it deliberately has no group
commit, compaction, capacity management, tail repair, or cross-process locking.
This isolates the cost of a minimal durable append design while retaining the
workload's batch, read, release, and warm-restart semantics.

The RocksDB mapping follows its documented sync-write contract: the WAL stays
enabled and a sync write has `write` plus `fdatasync` crash semantics. RocksDB
may group concurrent writes internally. redb documents that an immediate
commit is persistent when `commit` returns. The exact APIs are documented by
[RocksDB's write options](https://docs.rs/rocksdb/0.24.0/rocksdb/struct.WriteOptions.html),
[RocksDB's WAL overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview),
and [redb durability](https://docs.rs/redb/4.1.0/redb/enum.Durability.html).

RocksDB is strictly opt-in because its native dependency is large. Only a
command that explicitly enables `rocksdb-engine` resolves, downloads, and
compiles it. A plain benchmark build enables redb but not RocksDB.

RocksDB measurements are also disabled on macOS. Rust's `File::sync_data` uses
`F_FULLFSYNC` on Apple platforms, while the bundled `librocksdb-sys` build
falls back to ordinary `fsync`; those operations have materially different
power-loss guarantees and latency. The runner rejects an explicit macOS
RocksDB selection. Run the four-engine comparison on Linux, where Camus and
RocksDB both use the platform's data-sync durability boundary.

The comparison adapters use a 16-byte big-endian `(stream, sequence)` key and
an opaque value containing metadata, payload, and an XXH3 checksum. Reads
verify that per-record checksum before returning. Exact release maps to a
durable release frame for the simple file and deleting the selected keys for
the KV engines. Synchronous adapter calls run through Tokio's blocking pool so
the caller observes the same async completion boundary as the Camus API.

This mapping intentionally favors the comparison engines in two ways: their
record keys are caller-generated, and the adapters do not reproduce Camus's
root identity, format validation, segment lifecycle, capacity admission, or
safe reclamation. The comparison answers how much the shared durable-buffer
lifecycle costs; it does not claim the engines provide identical products.

## Workloads

Every sample uses a fresh database directory. Setup, deterministic record
generation, validation, and cleanup are outside the measured interval.

| Workload | Measured interval | Primary interpretation |
| --- | --- | --- |
| `append_sequential` | one caller appends one record per durability unit | no-contention durable commit latency |
| `append_concurrent_1_stream` | concurrent callers append individual records to one logical stream | group-commit throughput and tail latency |
| `append_concurrent_N_streams` | the same callers are distributed over `N` logical streams | cost of logical stream fan-out independent of physical layout |
| `append_batch` | one caller appends atomic record batches | amortized durable throughput |
| `read_verified_snapshot` | one bounded read loads and verifies the complete prepared pending set | cached sequential read and checksum throughput |
| `release_batch` | exact prepared IDs are released/deleted in configured batches | isolated durable release/delete latency |
| `read_release_drain` | bounded verified read followed by exact durable release/delete until empty | end-to-end drain throughput and batch latency |
| `warm_restart_first_batch` | clean reopen followed by the first verified bounded read | time to useful work after a warm restart; use latency, not the displayed dataset-normalized throughput |

Setup writes for read, release, drain, and restart workloads are not measured.
Release and drain validate that the pending count reaches zero. Append, read,
and restart workloads validate the complete pending count before the database
directory is removed.

The restart case is deliberately named *warm*: it does not drop the operating
system page cache. Cold-start measurement requires host-specific privileged
cache control or a dataset larger than memory and belongs in a separate test
protocol.

## Application-scenario harness

The `scenarios` command exercises Camus alone through four finite,
application-shaped storage handoffs. It complements the isolated
engine-comparison workloads: record construction, append, read, complete
content validation, exact durable release, and final drain are all inside the
end-to-end scenario interval. It does not simulate downstream network or
service latency, because that would measure the application rather than the
persistent buffer.

Application names describe the byte flow only. Their metadata and payloads
remain opaque to Camus, and the harness stays in the unpublished benchmark
crate.

| Scenario | Reference topology | Record and batching | Boundary exercised |
| --- | --- | --- | --- |
| `outbox_live_handoff` | 8 producers, 1 stream, 1 drain worker | 16,384 × (24 B metadata + 1 KiB payload); single-record append; reads of 128 | Contended single-destination durable handoff and group commit |
| `telemetry_batch_spool` | 4 producers, 4 source streams, 4 drain workers | 131,072 × (24 B + 256 B); appends of 64; reads of 512 | Batched small-record ingest and independent stream readiness |
| `upload_staging_recovery` | 4 producers, 4 staging streams | 512 × (24 B + 256 KiB); appends of 4; reads of 16 | Complete pending backlog, clean reopen, verified drain, and reclamation |
| `multi_stream_write_behind` | 32 producers, 32 streams, 32 drain workers | 32,768 × (24 B + 4 KiB); single-record append; reads of 64 | Logical stream fan-out over one physical root |

Run a fast correctness pass from the repository root:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  scenarios --profile smoke
```

For the fixed three-sample reference sizes, select a directory on the device
being measured:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  scenarios --profile reference \
  --data-directory /path/on/device/camus-scenario-data \
  --output target/benchmark-results/scenarios-reference.json
```

Each consumer verifies every metadata and payload byte, rejects duplicates,
checks strictly increasing Camus `RecordId` sequence numbers per stream, and
matches append and release commit counters to the configured record count.
Every sample must finish with zero pending records. The recovery scenario must
also recover the complete configured backlog before draining it. Reported
pending-record and physical-byte high-water marks are observations made after
completed storage transitions, not continuous exact maxima.

### Directional application-scenario reference

The following report-schema-1 result is a three-sample controlled Linux run
collected on 2026-07-16. It is directional, not a CI threshold. Latency cells
are p99; `handoff` measures append start through successful durable release,
while the staging scenario reports clean reopen latency in that column.

| Scenario | records/s | logical MiB/s | append | release | handoff / reopen | observed pending high-water | Validation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Single-destination outbox | 5,631 | 5.63 | 2.161 ms | 2.251 ms | 5.460 ms | 24 | Full integrity and order; 0 pending |
| Batched telemetry spool | 92,385 | 24.67 | 4.047 ms | 4.116 ms | 12.902 ms | 768 | Full integrity and order; 0 pending |
| Restartable upload staging | 1,462 | 365.41 | 12.550 ms | 7.651 ms | 1.319 ms reopen | 512 | Recovered 512/512; full integrity and order; 0 pending |
| 32-stream write-behind | 4,566 | 17.94 | 11.305 ms | 12.517 ms | 36.110 ms | 96 | Full integrity and order; 0 pending |

The outbox grouped almost exactly eight concurrent single-record appends per
durability group, so contention is amortized without turning the logical
stream into a physical shard. Telemetry batching delivered the highest record
rate. Large-object staging sustained the highest byte rate and reopened the
complete 128 MiB logical backlog with a 1.319 ms p99 before draining it. The
32-stream case remained correct under root-wide contention, but its 36.110 ms
handoff p99 is the clearest candidate for future scheduling and dynamic
sharding work. Median post-drain physical length stayed between 176 and 672
bytes across the four scenarios.

## Manifest compaction diagnostic

The standalone runner also has a Camus-only mixed-workload diagnostic for the
current manifest compaction boundary. It does not compile redb or RocksDB:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  manifest-compaction \
  --data-directory /path/on/device/camus-benchmark-data
```

The default setup appends 524,288 empty records in 4,096-record epochs and
releases alternating sequence numbers in eight 65,536-ID calls. Each call
therefore encodes singleton release ranges. The eighth frame crosses the
current 8 MiB manifest-log threshold and forces a complete checkpoint rewrite.
One unreleased anchor prevents reclamation from entering that measured window.

After admitting the trigger release, the runner polls for its storage job and
then starts eight independent foreground streams. Each performs 32 verified
append/read/release cycles. Detailed observability is enabled for this command.
The schema-versioned JSON report keeps the trigger release's caller latency,
the release storage-job delta sampled when its reply is observed, compaction
counts, and foreground p50/p95/p99/maximum latencies. Cleanup releases the
anchor, performs reclamation, verifies that no pending records remain, and
shuts down cleanly.

This diagnostic answers whether the current stop-the-root checkpoint rewrite
creates a material foreground tail-latency window for a given root size and
device. It is intentionally manual and is not a stable regression gate: the
8 MiB policy is an implementation detail, setup performs durable writes, and
filesystem sync latency is host-specific. Compare reports only on the same
controlled Linux host and storage configuration. Use argument overrides to
explore scale, and treat a run where
`storage_job_observed_in_flight` is `false` as non-overlapping rather than as a
foreground compaction measurement.

## Metrics and report format

The runner emits schema-versioned JSON containing the Git revision and dirty
state, Rust toolchain, OS/kernel, architecture, CPU model, logical CPU count, data
directory, engine version lines, full workload configuration, and results.
Each case reports:

- wall-clock records/s, workload operations/s, and logical MiB/s;
- HDR-histogram p50, p95, p99, and maximum operation latency in nanoseconds;
- total measured time; and
- median logical file length after clean shutdown.

An *operation* is one append call, one append-batch call, one read-plus-release
cycle, or one reopen-plus-first-read, depending on the workload. File length
is an observability aid, not allocated filesystem blocks. RocksDB compaction,
redb page reuse, and Camus segment reclamation make it unsuitable as a direct
space-amplification verdict.

## Profiles

| Setting | `smoke` | `baseline` | `soak` |
| --- | ---: | ---: | ---: |
| samples per case | 1 | 3 | 5 |
| concurrent callers / logical streams | 4 / 4 | 16 / 16 | 64 / 64 |
| sequential records | 16 | 256 | 2,048 |
| concurrent records | 128 | 4,096 | 65,536 |
| batch records / total records | 16 / 256 | 64 / 8,192 | 256 / 131,072 |
| verified-read records | 256 | 8,192 | 131,072 |
| release records / release batch | 256 / 32 | 8,192 / 256 | 131,072 / 1,024 |
| read batch / drain records | 32 / 256 | 256 / 8,192 | 1,024 / 131,072 |
| warm-restart records | 1,024 | 16,384 | 262,144 |

Defaults use 32-byte metadata and a 4 KiB payload. `--records` replaces every
profile record count for focused runs. Batch and setup sizes are automatically
clamped so one comparison epoch stays at or below 4 MiB. Use `--help` for all
dimension overrides.

## Running a baseline

The Camus-versus-simple-file comparison has no native KV dependency and is the
fastest way to establish the raw append reference:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  run --engines camus,simple-append-file --profile baseline
```

For the four-engine Linux suite, install a C++ compiler and libclang for the
RocksDB binding. A typical Ubuntu host needs `clang`, `libclang-dev`, `cmake`,
and `build-essential`.

Run a correctness smoke test first:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --features rocksdb-engine -- \
  run --engines camus,simple-append-file,rocksdb,redb --profile smoke
```

Then select an explicit directory on the device being characterized and write
the baseline outside the source tree or under ignored `target/` output:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --features rocksdb-engine -- \
  run --profile baseline --samples 5 \
  --engines camus,simple-append-file,rocksdb,redb \
  --data-directory /path/on/device/camus-benchmark-data \
  --output target/benchmark-results/baseline-4k.json \
  --note "dedicated host; ext4; performance governor; background jobs paused"
```

Repeat with representative payload sizes rather than extrapolating one point:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --features rocksdb-engine -- \
  run --profile baseline --payload-bytes 256 \
  --engines camus,simple-append-file,rocksdb,redb \
  --output target/benchmark-results/baseline-256b.json

cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --features rocksdb-engine -- \
  run --profile baseline --payload-bytes 65536 \
  --engines camus,simple-append-file,rocksdb,redb \
  --output target/benchmark-results/baseline-64k.json
```

Do not benchmark on a memory-backed temporary directory. Keep free space,
filesystem mount options, power mode, CPU governor, thermal state, background
indexing, backup activity, and encryption policy stable. Run engines on the
same device, inspect all samples for outliers, and rerun the full report after
a reboot before promoting a long-lived baseline.

## Comparing a candidate

Reports match only cases with identical engine, workload, payload, metadata,
concurrency, logical-stream, batch, and record-count dimensions. The default
gate permits a 15% records/s decrease and 25% p99 increase:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  compare \
  --baseline target/benchmark-results/baseline-4k.json \
  --candidate target/benchmark-results/candidate-4k.json
```

The command prints percentage changes and exits nonzero for missing cases or
threshold violations. Treat one failure as a signal to reproduce, profile,
and inspect I/O rather than as proof of a code regression. Shared hosted CI
runners are appropriate for compiling and smoke-testing the harness, not for
maintaining a storage-performance gate.

Benchmark validation is isolated from the daily project CI. Its workflow runs
only when triggered manually and uses Camus, the simple append file, and redb
for a correctness smoke. Full RocksDB comparisons and performance measurements
remain manual work on a controlled Linux host and never gate ordinary
development.

The comparison runner is intentionally finite and uses a fresh root per case.
It is not the long-running capacity test. Use the separate `smoke/` crate and
[`long-running smoke protocol`](long-running-smoke.md) to observe a single
bounded Camus root across repeated high-water, blocked-admission, reclamation,
and recovery cycles.

## Report handling

Keep raw JSON reports and machine-specific notes under ignored `target/`
output or in external benchmark artifacts. Do not commit hostnames, usernames,
local paths, device identifiers, Git working-tree metadata, or complete
environment captures. Only sanitized aggregate conclusions belong in this
document.
