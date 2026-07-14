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
- Checksummed format v1 with packed little-endian epochs, self-published
  segments, exact release frames, canonical checkpoints, narrow tail repair,
  ordered seal/removal publication, and immutable root identity.
- Normative architecture and file-format specifications, usage and operations
  guides, runnable replay/readiness/multi-stream/maintenance examples, focused
  corruption tests, cross-process locking tests, and segment/manifest recovery
  fuzz targets.
