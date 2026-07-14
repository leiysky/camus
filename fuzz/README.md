# Camus recovery fuzzing

The recovery target uses the standard `cargo-fuzz`/`libfuzzer-sys` setup and
lives outside the main crate workspace.

Install the pinned nightly with
`rustup toolchain install nightly-2026-04-02 --profile minimal` before running
the fuzzers locally.

```sh
cargo install cargo-fuzz --locked --version 0.13.2
cargo +nightly-2026-04-02 fuzz run wal_recovery -- -max_len=16384 -timeout=5
```

`wal_recovery` selects a bounded logical stream, creates bounded
metadata/payload records, groups them into durability epochs, and then
truncates or corrupts the final epoch marker.
Reopening must discard exactly that epoch, preserve older epochs, never invent
record IDs, and return only locations whose complete frames and payload
checksums pass. A clean tail must recover every record.

`manifest_recovery` selects default or nondefault stream release encoding,
writes one batched release record, and truncates or corrupts that final
manifest frame. Recovery must either fail closed or apply the stream-scoped
release record atomically, never invent a record ID, and leave every recovered
payload readable.

```sh
cargo +nightly-2026-04-02 fuzz run manifest_recovery -- -max_len=16384 -timeout=5
```

The harness can be type-checked without installing `cargo-fuzz`:

```sh
cargo check --locked --manifest-path fuzz/Cargo.toml
```
