# Format-v1 `1.0.0` reclaimed empty root

These files are immutable historical input, not expected output. They were
written only through the public API of `camus 1.0.0` downloaded from crates.io.
The registry package checksum recorded by Cargo was:

```text
599c670393a3b4d3ac2be7eba00e274308b4acc7b10ff701c217e15672550002
```

The release tag points to commit
`27ca1b378057bc1f31470f5bccdf7b16f6f51ea0`. The root uses a 256 KiB bounded
capacity, 13 KiB segments, a 12 KiB maximum epoch and commit size, and a 16-ID
release limit. Its history contains:

- stream 21 sequences 0 and 1, both released;
- stream 22 sequences 0 through 2, all released;
- a completed explicit reclamation pass;
- no live or active data segment; and
- a compact checkpoint retaining both durable stream high-waters and the next
  physical segment ID.

Compatibility tests copy the root, verify the empty pending state and durable
stream identities, append the next sequence on each stream, release it, shut
down, and reopen. Never regenerate or replace this fixture to accommodate a
reader change.
