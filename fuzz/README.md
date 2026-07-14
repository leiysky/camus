# Camus recovery fuzzing

The recovery target uses the standard `cargo-fuzz`/`libfuzzer-sys` setup and
lives outside the main crate workspace.

Install the pinned nightly with
`rustup toolchain install nightly-2026-04-02 --profile minimal` before running
the fuzzers locally.

```sh
cargo install cargo-fuzz --locked --version 0.13.2
cargo +nightly-2026-04-02 fuzz run segment_recovery -- -max_len=16384 -timeout=5
```

`segment_recovery` selects a bounded logical stream, creates bounded
metadata/payload records, groups them into durability epochs, and then
truncates or corrupts the final epoch commit. An incomplete active tail after
an older valid epoch may be repaired; complete checksum corruption and an
incomplete first epoch must fail closed. Clean data must recover every record,
and every returned body must pass lazy checksum verification.

`manifest_recovery` selects a stream, writes one exact release unit, and
truncates or corrupts that final manifest frame. Recovery must truncate an
incomplete final frame, fail closed on complete corruption, or apply the
release atomically. It must never invent a record ID and every returned payload
must remain readable.

```sh
cargo +nightly-2026-04-02 fuzz run manifest_recovery -- -max_len=16384 -timeout=5
```

The harness can be type-checked without installing `cargo-fuzz`:

```sh
cargo check --locked --manifest-path fuzz/Cargo.toml
```
