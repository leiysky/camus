# Camus benchmark runner

This standalone, unpublished, repository-only crate compares Camus's
durable-buffer lifecycle with a benchmark-only simple append file and
durability-enabled RocksDB and redb adapters. It is separate from and excluded
from the published Camus crate, so native RocksDB compilation does not affect
normal builds, tests, or package dependencies.

Run the smoke profile from the repository root:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --features rocksdb-engine -- \
  run --engines camus,simple-append-file,rocksdb,redb --profile smoke
```

RocksDB is strictly opt-in because its native dependency is large. Only
commands that explicitly enable `rocksdb-engine` resolve, download, and compile
it. A plain benchmark build enables redb but not RocksDB.

For a lightweight comparison without native KV dependencies:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  run --engines camus,simple-append-file --profile smoke
```

To exercise Camus through finite application-shaped handoff paths without any
comparison-engine dependency:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  scenarios --profile smoke
```

The four fixed scenarios cover a single-destination outbox, a batched
telemetry spool, restartable large-object staging, and 32-stream write-behind.
They validate opaque record contents, per-stream order, durable commit totals,
restart recovery where applicable, and a complete final drain. Use
`--profile reference` for the documented three-sample sizes.

To measure foreground latency across one forced Camus manifest compaction,
without compiling any comparison engine:

```sh
cargo run --locked --release --manifest-path benchmarks/Cargo.toml \
  --no-default-features -- \
  manifest-compaction
```

This is a manual Camus-only diagnostic. It prepares a large sparse release
history, starts the release expected to cross the current compaction threshold,
and issues verified append/read/release cycles on independent foreground
streams while that storage job is active.

The runner validates recovered pending counts and complete drains, prints a
summary, and writes a versioned JSON report under `target/benchmark-results`
unless `--output` is specified. See
[`docs/benchmarks.md`](../docs/benchmarks.md) for the methodology, workload
definitions, compaction diagnostic, platform controls, and comparison command.

RocksDB measurements are disabled on macOS. Even when its feature is enabled,
the runner excludes RocksDB from automatic engine selection and rejects
explicit selection because the bundled binding does not use the same
`F_FULLFSYNC` durability boundary as Rust's `File::sync_data`.
