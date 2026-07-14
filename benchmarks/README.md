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

The runner validates recovered pending counts and complete drains, prints a
summary, and writes a versioned JSON report under `target/benchmark-results`
unless `--output` is specified. See
[`docs/benchmarks.md`](../docs/benchmarks.md) for the methodology, workload
definitions, platform controls, and comparison command.

RocksDB measurements are disabled on macOS. Even when its feature is enabled,
the runner excludes RocksDB from automatic engine selection and rejects
explicit selection because the bundled binding does not use the same
`F_FULLFSYNC` durability boundary as Rust's `File::sync_data`.
