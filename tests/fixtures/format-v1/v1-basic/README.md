# Format-v1 basic compatibility root

These files are immutable historical input, not expected output. They were
written by Camus commit `dbc887333de27603f7426351b12207e8f9d5ed0f` before the
first published format-v1 release.

The root uses a 256 KiB bounded capacity, 13 KiB segments, a 12 KiB maximum
epoch and commit size, and a 16-ID release limit. Its history contains:

- stream 7 sequence 0, released and physically reclaimed with its segment;
- stream 7 sequences 1 and 2, still pending;
- stream 9 sequence 0, released in the live segment;
- stream 9 sequence 1, still pending;
- a compact checkpoint carrying release and removed-segment high-water state;
  and
- one active live segment containing the remaining records.

Compatibility tests copy the root, open it, verify pending data and release
state, append and release new data, shut down, and reopen. Never regenerate or
replace this fixture to accommodate a reader change. Add another fixture for a
new historical writer or format version.
