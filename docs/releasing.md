# Release and 1.0 qualification

Camus releases bind two different compatibility surfaces: the Rust API follows
Semantic Versioning, while persistent roots follow the explicit format version
in `ROOT`. A crate release must not silently redefine either surface.

## Repository gates

Before publishing a release candidate or stable version, the repository must:

- require the semantic-title, quality, Linux, current-stable, and
  dependency-policy checks on `main`;
- disallow direct pushes and force-pushes to `main`;
- enable GitHub private vulnerability reporting when the repository becomes
  public;
- keep the root, fuzz, benchmark, and smoke lockfiles under dependency audit;
  and
- retain a successful scheduled recovery-fuzz run for the candidate revision.

If the repository host cannot enforce these controls for a private project,
make the repository public or move it to a plan that can before declaring 1.0.

## Manual release qualification

The `Release qualification` GitHub Actions workflow is manual-only and never
publishes, tags, or creates a release. Dispatch it at the exact reviewed
candidate commit with:

- `expected_version` equal to `Cargo.toml` and a dated
  `## [<version>] - YYYY-MM-DD` heading in `CHANGELOG.md`;
- `establish_api_baseline` enabled only for the first public candidate; or
- `baseline_version` set to the previous published candidate or stable crate
  for every later release.

The baseline choices are mutually exclusive, and omitting both fails the run.
The workflow executes the complete locked repository checks, applies a fixed
`patch` compatibility policy with `cargo-semver-checks` when a published
baseline exists, builds the crate archive, runs the library and integration
tests from Cargo's extracted package source, and performs a publication dry
run. This policy freezes the public API throughout the `1.0.0-rc.N` chain and
the transition to `1.0.0`. A successful run is repository qualification
evidence; it does not replace the external reliability gates below.

## Compatibility gates

Every candidate must pass all of the following:

1. Open the committed format-v1 golden roots, verify pending contents and
   release state, append and release new records, shut down, and reopen cleanly.
2. When a previous candidate or stable release exists, generate representative
   roots with it and open them with the new candidate. The first candidate
   establishes this writer baseline.
3. Run `cargo semver-checks` against the previous candidate or stable release.
   The first candidate establishes the reviewed API baseline; later 1.0
   candidates must pass with patch compatibility and cannot waive a public API
   break by weakening the release type.
4. Review public enums, public-field structs, traits, serialized identities,
   default values, errors, cancellation behavior, and durability outcomes.
5. Confirm that byte-layout or semantic changes either preserve format v1
   exactly or introduce a separately designed format version and migration.

The golden fixtures are compatibility evidence, not writer snapshots to be
regenerated when a test fails. Add a new fixture for a new format or historical
writer; never replace old bytes in place.

## First public candidate transition

The first public candidate changes statements that are intentionally true only
while the repository is unpublished. Its reviewed release commit must:

- set `Cargo.toml` to `1.0.0-rc.1` and create the dated changelog section;
- replace the unpublished-compatibility notices in `README.md`,
  `docs/file-format.md`, and `docs/operations.md` with the now-binding format-v1
  and public-API policy;
- confirm the crate name and publishing ownership before creating a tag;
- dispatch `Release qualification` with `establish_api_baseline`, after a
  manual review of the exported Rust API and format-v1 specification; and
- after publication, generate a representative root with the registry artifact
  and retain it as a new immutable published-writer fixture without replacing
  the existing pre-public fixture.

Later candidates use the previous published candidate as `baseline_version`
and must open both the pre-public and every published-writer fixture.

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
- repeated external `SIGKILL` and reopen while append, release, rollover,
  checkpoint, and reclamation are active;
- Linux power-loss or block-device fault testing for the sync ordering claimed
  by the candidate;
- a representative large-root recovery measurement covering startup time,
  first-read latency, RSS, record count, stream count, and segment topology.

macOS validation is useful during development but is not a release gate or a
production durability qualification target for 1.0.

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
3. dispatch `Release qualification` at the candidate commit with the exact
   version and applicable API-baseline input, and retain the successful run;
4. inspect the packaged file list and crate archive produced by Cargo;
5. verify the crate metadata, repository link, README, license, and docs.rs
   rendering;
6. create a signed `v<version>` tag from the reviewed merge commit;
7. publish only with explicit owner authorization;
8. create a GitHub release containing the sanitized qualification summary; and
9. verify that a new empty root and a copied golden root both work with the
   registry artifact, not only the repository checkout.

## Rollback

Never overwrite or yank a release merely because a replacement exists. For an
API-only regression, publish a compatible patch. For a durability or format
defect, stop recommending the affected version, preserve a complete root copy,
publish an advisory, and provide an explicit recovery or migration procedure.
An older binary must not open roots after a format migration unless that
rollback path was designed and tested in advance.
