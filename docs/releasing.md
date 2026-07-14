# Release and 1.0 qualification

Camus releases bind two different compatibility surfaces: the Rust API follows
Semantic Versioning, while persistent roots follow the explicit format version
in `ROOT`. A crate release must not silently redefine either surface.

## Repository gates

Before publishing a release candidate or stable version, the repository must:

- require the semantic-title, quality, Linux, macOS, current-stable, and
  dependency-policy checks on `main`;
- disallow direct pushes and force-pushes to `main`;
- enable GitHub private vulnerability reporting when the repository becomes
  public;
- keep the root, fuzz, benchmark, and smoke lockfiles under dependency audit;
  and
- retain a successful scheduled recovery-fuzz run for the candidate revision.

If the repository host cannot enforce these controls for a private project,
make the repository public or move it to a plan that can before declaring 1.0.

## Compatibility gates

Every candidate must pass all of the following:

1. Open the committed format-v1 golden roots, verify pending contents and
   release state, append and release new records, shut down, and reopen cleanly.
2. When a previous candidate or stable release exists, generate representative
   roots with it and open them with the new candidate. The first candidate
   establishes this writer baseline.
3. Run `cargo semver-checks` against the previous candidate or stable release.
   The first candidate establishes the reviewed API baseline; later candidates
   review every reported break before changing a major version.
4. Review public enums, public-field structs, traits, serialized identities,
   default values, errors, cancellation behavior, and durability outcomes.
5. Confirm that byte-layout or semantic changes either preserve format v1
   exactly or introduce a separately designed format version and migration.

The golden fixtures are compatibility evidence, not writer snapshots to be
regenerated when a test fails. Add a new fixture for a new format or historical
writer; never replace old bytes in place.

## Reliability qualification

Ordinary library tests provide a deterministic recovery matrix for:

- process termination immediately after data sync, rename, directory sync,
  release publication, seal publication, removal publication, deletion, and
  checkpoint or manifest replacement boundaries;
- injected `ENOSPC` and `EIO` results from sync, rename, directory-sync, and
  delete operations, with `DurabilityOutcome::Unknown` checked at the API
  boundary; and
- actual partial segment epochs, segment footers, manifest frames, and atomic
  replacement temporaries, followed by exact tail-repair and recovered-state
  assertions.

The hooks exist only in `cfg(test)` builds. Each case runs the mutation in a
separate process so it cannot leak environment-selected behavior into another
test, and then opens the same root in the parent process. A selected crash
point must emit its marker and terminate with the reserved test exit code; a
missed hook cannot accidentally pass.

This matrix proves Camus's recovery decisions for bytes visible after process
termination. It does not emulate a power loss that discards unsynced page-cache
contents, a filesystem that violates documented sync ordering, torn sectors,
or a failing physical device. A candidate revision must additionally complete:

- at least 24 hours of the capacity-cycle smoke workload on Linux/ext4;
- at least 6 hours on macOS/APFS;
- repeated external `SIGKILL` and reopen while append, release, rollover,
  checkpoint, and reclamation are active;
- platform-appropriate power-loss or block-device fault testing for the sync
  ordering claimed by the candidate;
- a representative large-root recovery measurement covering startup time,
  first-read latency, RSS, record count, stream count, and segment topology.

Raw host reports stay outside the repository. Attach a sanitized aggregate
report to the release candidate and record its exact commit and configuration.

## Release candidate sequence

Do not make the first public build the stable compatibility boundary. Publish
at least one `1.0.0-rc.N` candidate and exercise it from a real downstream
application before 1.0. The downstream validation must cover reopen after an
unclean process stop and idempotent handling of replayed effects.

For every candidate:

1. update `CHANGELOG.md` with the version and date;
2. run the complete locked command list in the root README;
3. run `cargo publish --dry-run --locked` and inspect the packaged file list;
4. verify the crate metadata, repository link, README, license, and docs.rs
   rendering;
5. create a signed `v<version>` tag from the reviewed merge commit;
6. publish only with explicit owner authorization;
7. create a GitHub release containing the sanitized qualification summary; and
8. verify that a new empty root and a copied golden root both work with the
   registry artifact, not only the repository checkout.

## Rollback

Never overwrite or yank a release merely because a replacement exists. For an
API-only regression, publish a compatible patch. For a durability or format
defect, stop recommending the affected version, preserve a complete root copy,
publish an advisory, and provide an explicit recovery or migration procedure.
An older binary must not open roots after a format migration unless that
rollback path was designed and tested in advance.
