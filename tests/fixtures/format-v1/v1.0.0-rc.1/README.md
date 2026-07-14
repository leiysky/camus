# Format-v1 `1.0.0-rc.1` published-writer root

These files are immutable historical input, not expected output. They were
written only through the public API of `camus 1.0.0-rc.1` downloaded from
crates.io. The registry package checksum recorded by Cargo was:

```text
7492d560eb74bb9cd7fcbbd456c422f58010394c6d2169c12bb80370f55ca9f9
```

The release tag points to commit
`a5c091c404ff70b9eb918fc134717bc2a1d5cd22`. The root uses a 256 KiB bounded
capacity, 13 KiB segments, a 12 KiB maximum epoch and commit size, and a 16-ID
release limit. Its history contains:

- stream 21 sequence 0, released and physically reclaimed with segment 0;
- stream 21 sequence 1, still pending with an 8 KiB payload;
- stream 34 sequence 0, released in the live segment;
- stream 34 sequence 1, still pending;
- a compact checkpoint carrying release and removed-segment high-water state;
  and
- one active live segment containing the remaining records.

Compatibility tests copy the root, open it, verify pending data and release
state, append and release new data, shut down, and reopen. Never regenerate or
replace this fixture to accommodate a reader change. Add another fixture for a
new published writer or format version.
