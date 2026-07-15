# Format-v1 `1.0.0` sealed pending root

These files are immutable historical input, not expected output. They were
written only through the public API of `camus 1.0.0` downloaded from crates.io.
The registry package checksum recorded by Cargo was:

```text
599c670393a3b4d3ac2be7eba00e274308b4acc7b10ff701c217e15672550002
```

The release tag points to commit
`27ca1b378057bc1f31470f5bccdf7b16f6f51ea0`. The root uses a 256 KiB bounded
capacity, 13 KiB segments, a 12 KiB maximum epoch and commit size, a 16-ID
release limit, and a 20 ms age rollover while it is written. Its history
contains:

- stream 11 sequences 0 and 1, both pending;
- one complete append batch containing 1 KiB and 2 KiB payloads;
- one footer-sealed segment with a durable `SegmentSealed` manifest frame; and
- no active segment.

Compatibility tests copy the root, verify the sealed records, lazily create a
successor with the current writer, release all data, shut down, and reopen.
Never regenerate or replace this fixture to accommodate a reader change.
