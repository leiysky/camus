# Format-v1 `1.0.0` active multi-stream root

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

- stream 3 sequence 0, released with an empty payload;
- stream 3 sequences 1 and 2, pending with binary and short payloads;
- stream 5 sequence 0, pending;
- stream 5 sequence 1, released;
- two exact release frames in the manifest suffix; and
- one unsealed active segment interleaving both streams.

Compatibility tests copy the root, verify released gaps, payloads, and stream
order, append and release new data, shut down, and reopen. Never regenerate or
replace this fixture to accommodate a reader change.
