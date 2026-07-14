# Changelog

All notable changes to Camus are documented here. The project follows
[Semantic Versioning](https://semver.org/) for its Rust API. On-disk format
compatibility is documented separately and does not silently follow crate
version numbers.

## Unreleased

- Initial embedded persistent-buffer API with async `Log` and lightweight
  logical `Stream` handles.
- Waiting, non-empty bounded `Stream::read` snapshots and exact idempotent
  release, providing an at-least-once storage handoff without consumers,
  callbacks, claims, or cursors.
- Root-wide physical segments shared by caller-selected `u64` stream IDs,
  stream-local durable record identities, opportunistic group commit, and
  size/optional-age rollover.
- Explicit unbounded or globally bounded capacity with `Block` and
  `RejectNew`, dynamic maintenance headroom, automatic reclamation, and an
  explicit reclaim barrier.
- Runtime abstraction with a lazy process-wide Tokio default, cancellation-safe
  operation admission, fail-closed poisoning, and draining shutdown.
- Pull-based root and stream snapshots, caller-versus-commit counters,
  pressure/maintenance/recovery telemetry, stable error classifications, and a
  non-owning async health watch; detailed fast-path timing is opt-in.
- Checksummed format v1 with packed little-endian epochs, self-published
  segments, exact release frames, canonical checkpoints, narrow tail repair,
  ordered seal/removal publication, and immutable root identity.
- Normative architecture and file-format specifications, usage and operations
  guides, runnable replay/readiness/multi-stream/maintenance examples, focused
  corruption tests, cross-process locking tests, and segment/manifest recovery
  fuzz targets.
- Standalone durability-matched benchmark runner for Camus, a simple append
  file, RocksDB, and redb, with sequential, concurrent, batch, verified-read,
  exact-release, drain, and warm-restart workloads plus versioned JSON reports
  and regression comparison; the native RocksDB dependency is strictly opt-in,
  and its measurements are disabled on macOS so `fsync` is not compared with
  Rust's stronger `F_FULLFSYNC` boundary.
- Standalone manual long-running smoke crate with cyclic bounded-capacity
  pressure, deterministic delivery validation, interval throughput and latency
  metrics, VictoriaMetrics ingestion verification, sanitized reports, and an
  externally killed large-backlog reopen/verify/drain qualification mode.
- Pre-1.0 release gates covering Linux CI, every workspace lockfile, semver and
  format review, immutable format-v1 golden-root reopen tests, and an explicit
  release-candidate procedure.
- Deterministic subprocess crash and I/O-fault recovery matrices across append,
  seal, release, reclamation, checkpoint, and manifest publication, including
  `ENOSPC`, `EIO`, partial-tail repair, and conservative unknown durability.
- Forward-extensible public errors and snapshot/report structs, plus an
  explicit Linux production support boundary and macOS development-only
  status for 1.0 qualification.
