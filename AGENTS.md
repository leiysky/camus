# Camus development guide

This file applies to the entire repository.

## Sources of truth

- `README.md` defines the public project boundary and API expectations.
- `docs/file-format.md` defines the versioned byte layouts, checksums, codecs,
  validation rules, and compatibility boundary.
- `docs/architecture.md` defines the on-disk ordering, recovery, release, and
  reclamation invariants.
- Keep the crate application-neutral. Do not add application schemas, ingest
  protocols, service routing, HTTP handlers, downstream sink policy, or
  consumer scheduling.

## Durability invariants

- Write every record frame and its epoch commit marker before the epoch's one
  `sync_data`; return success only after that sync.
- Sync a release record before excluding records from recovery or reclaiming
  their storage.
- Publish and sync segment removal in the manifest before deleting the segment;
  sync the segment directory after deletion.
- Manifest compaction writes and syncs a complete checkpoint before atomic
  rename, then syncs the root directory.
- Repair only an incomplete active tail. Fail closed on corruption before a
  valid suffix, in a sealed segment, or in authoritative headers.
- Record metadata and payloads are opaque. Do not introduce application-level
  serialization or interpretation in Camus.

## Implementation style

- Prefer direct, linear code and cohesive types. Add abstractions only for a
  demonstrated invariant, second implementation, or concrete test need.
- Preserve unrelated worktree changes. Inspect `git status -sb`, the branch,
  and relevant diffs before editing or staging.
- Never commit directly to `main`. Agent-created branches use
  `agent/<short-description>`; continue an existing task branch when present.
- When dependencies change, update and verify both `Cargo.lock` and
  `fuzz/Cargo.lock`.

## Tests and verification

- Use focused unit tests for record codecs, exact corruption boundaries,
  manifest ordering, and segment lifecycle. Keep a public-API integration test.
- Synchronize concurrent tests with explicit events, never sleeps.
- Before publishing, run:

  ```text
  cargo fmt --all --check
  cargo fmt --all --check --manifest-path fuzz/Cargo.toml
  cargo clippy --locked --all-targets -- -D warnings
  cargo test --locked --lib --tests
  cargo test --locked --release --lib --tests
  cargo test --locked --doc
  RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
  cargo check --locked --manifest-path fuzz/Cargo.toml
  cargo audit --deny warnings
  cargo audit --deny warnings --file fuzz/Cargo.lock
  cargo deny --locked check -A license-not-encountered licenses sources
  cargo deny --locked --manifest-path fuzz/Cargo.toml check licenses sources
  cargo package --locked
  ```

## Git and pull requests

- Stage intended paths explicitly when the worktree is mixed.
- PR titles use `<type>[optional scope]: <description>` with one of `feat`,
  `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`,
  or `revert`.
- PR descriptions state what changed, why, runtime or API impact, compatibility
  impact, and checks. Publishing requires user authorization; default to a
  draft PR and never merge without an explicit request.
