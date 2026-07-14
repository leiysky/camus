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

The following five-sample Linux/ext4 result uses the baseline profile and a
4 KiB payload. It is a directional comparison, not a universal target or CI
gate. Except for warm restart, each engine cell is `records/s · p99 ms`;
warm-restart cells are `p50 / p99 ms`.

| Workload | Camus | Simple append file | RocksDB | redb | Conclusion |
| --- | ---: | ---: | ---: | ---: | --- |
| Sequential durable append | 874 · 1.834 | 1,056 · 1.564 | 1,071 · 1.522 | 902 · 1.914 | Camus trails the minimal file and RocksDB by about 17–18%. |
| Concurrent append, 1 stream | 8,258 · 3.222 | 1,104 · 27.918 | 6,769 · 3.498 | 868 · 37.683 | Camus leads throughput and p99 latency; its group commit is effective. |
| Concurrent append, 16 streams | 7,307 · 3.273 | 1,130 · 17.826 | 6,657 · 3.740 | 872 · 35.553 | Logical stream fan-out adds little physical I/O cost for Camus. |
| Append batches of 64 | 33,797 · 2.787 | 43,649 · 2.218 | 33,439 · 2.783 | 23,483 · 4.235 | Camus and RocksDB are effectively level; the minimal file sets the lower-overhead reference. |
| Cached verified read | 196,896 · 66.912 | 188,801 · 50.921 | 258,424 · 37.356 | 300,655 · 30.654 | RocksDB and redb lead; Camus read-path work remains an optimization target. |
| Release batches of 256 | 225,984 · 2.114 | 251,132 · 1.571 | 221,507 · 2.030 | 208,969 · 2.365 | All four are in the same throughput range; the simple file has the lowest p99. |
| Read/release drain | 109,871 · 3.672 | 133,881 · 2.992 | 140,549 · 2.628 | 144,440 · 2.853 | Camus trails the comparison engines on end-to-end drain. |
| Warm restart, first batch | 23.904 / 26.477 | 18.924 / 19.186 | 290.980 / 314.835 | 1.715 / 2.043 | Camus restarts far faster than RocksDB but slower than the simpler formats. |

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

## Report handling

Keep raw JSON reports and machine-specific notes under ignored `target/`
output or in external benchmark artifacts. Do not commit hostnames, usernames,
local paths, device identifiers, Git working-tree metadata, or complete
environment captures. Only sanitized aggregate conclusions belong in this
document.
