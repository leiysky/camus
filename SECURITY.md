# Security policy

## Supported versions

Until the first stable release, security fixes are made on the latest `0.1.x`
release line and the default branch. Older prerelease revisions are not
maintained independently.

## Reporting a vulnerability

Use the repository's GitHub **Security → Report a vulnerability** flow so the
report, reproducer, and proposed fix remain private. Do not open a public issue
for an undisclosed vulnerability. Include the affected version, platform and
filesystem, a minimal reproducer, expected impact, and whether the problem can
be triggered by an untrusted local process or only by the embedding process.

Before a public release, repository administrators must verify that GitHub
private vulnerability reporting is enabled. If that channel is unavailable,
contact the repository owner privately without including exploit details in a
public discussion.

## Security boundary

Camus protects durability ordering and detects accidental on-disk corruption.
It does not sandbox the embedding application, encrypt records, authenticate
files, or defend a storage root against a malicious process with write access.
XXH3 checksums are non-cryptographic. Applications are responsible for access
control, secrets handling, payload encryption when required, destination
authorization, and safe replay of at-least-once effects.

New Camus directories and files request owner-only Unix modes (0700 and 0600).
Camus does not tighten an existing root, so deployments must audit inherited
ownership, ACLs, mount options, backups, and snapshot access.
