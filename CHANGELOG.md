# Changelog

All notable changes to Camus are documented here. The project follows
[Semantic Versioning](https://semver.org/) for its Rust API. On-disk format
compatibility is documented separately and does not silently follow crate
version numbers.

## Unreleased

- Initial embedded staging-log API with batched durability epochs.
- Checksummed segment and manifest format version 1.
- Lazy validated payload reads, durable release, ordered segment reclamation,
  and manifest checkpoint compaction.
- Multiple logical streams with independent record identity, segment
  lifecycles, release/reclaim state, and per-stream size/age rollover policy.
- Persisted segment creation times, append-time age checks, explicit idle-age
  checks, and manual rollover without background scheduling.
- Runtime-neutral, level-triggered `wait_for(stream_id)` Futures that wake on
  durable pending work without callbacks, polling, or a background runtime;
  readiness is broadcast observation and does not assign records or coordinate
  consumers.
- End-to-end usage guidance for multi-stream append, bounded draining, async
  readiness, replay, release, maintenance, and poisoned-handle recovery.
- Deterministic crash-window tests, segment and manifest fuzz targets,
  cross-process ownership tests, and poisoned-handle recovery semantics.
