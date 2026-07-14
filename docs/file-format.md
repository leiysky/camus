# Camus file format version 1

This document is the normative byte-level specification for Camus on-disk
format version 1. It defines canonical paths, frame layouts, checksum inputs,
manifest schemas, validation rules, and the boundary between repair and
corruption.

The [architecture](architecture.md) remains authoritative for publication
ordering, durability, recovery, release, and reclamation. The
[README](../README.md) remains authoritative for the public project boundary
and API contract. If the documents appear to disagree, this document governs
byte encoding and the architecture governs state transitions and filesystem
ordering.

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. Byte
ranges use half-open notation: `[a, b)` includes `a` and excludes `b`.

## Design constraints

Version 1 is designed around five properties:

- one append call becomes one atomic, stream-local recovery epoch;
- payloads can remain unread during recovery;
- segment lifecycle and releases survive process crashes;
- only a physically final torn or checksum-damaged tail can be repaired
  automatically; and
- unsupported or ambiguous authoritative data fails closed.

The format does not encode consumer ownership, delivery attempts, application
schemas, cross-stream ordering, compression, encryption, or authentication.
Record metadata and payload bytes are opaque to Camus.

## Encoding conventions and limits

All unsigned integers are little-endian. Frames are packed without alignment
or padding. Lengths include exactly the byte ranges stated below.

Every checksum is unseeded XXH3-64. A stored checksum is the resulting `u64`
encoded little-endian. For multiple inputs, `XXH3(a || b)` means one streaming
hash over the exact concatenation of `a` followed by `b`; it is not a hash of
separate digests. XXH3 detects accidental corruption but provides no
cryptographic authenticity.

Version 1 enforces these limits before allocating or interpreting variable
data:

| Item | Limit |
| --- | ---: |
| File header | 32 bytes |
| Segment frame prefix | 48 bytes |
| Manifest frame prefix | 32 bytes |
| Segment or manifest frame metadata | 16,777,216 bytes |
| Record ID | 1 to 16,384 UTF-8 bytes |
| Segment IDs emitted in one removal event | 65,536 |

The removal-event limit is a canonical writer bound: larger removal sets are
split into consecutive events. A reader is still bounded by the 16 MiB
metadata limit. Every length addition and file-offset addition MUST fit in a
`u64`; overflow is corruption, not wraparound.

## Canonical root layout

```text
<root>/
  camus.lock
  MANIFEST
  segments/
    segment-00000000000000000000.log
    ...
  streams/
    stream-0000000007/
      segment-00000000000000000000.log
      ...
```

Logical stream `0` uses `segments/`. Logical streams `1..=u32::MAX` use
`streams/stream-<stream-id>`, where the ID is zero-padded to 10 decimal digits.
Segment names contain a zero-padded 20-digit decimal `u64` segment ID.

The following files are transactional implementation artifacts and are not
additional sources of state:

- `MANIFEST.create` while creating the first manifest;
- `MANIFEST.compact` while writing a manifest checkpoint; and
- `segment-<20-digit-id>.log.tmp` while creating a segment.

`camus.lock` establishes exclusive process ownership but has no version-1 wire
payload. `MANIFEST` is authoritative for which canonical segment files exist
and whether each is active or sealed. Directory enumeration is reconciliation
input only.

## Shared file header

Every segment and manifest starts with this 32-byte header:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | Exact file-type magic |
| 8 | 2 | `version` | `1` |
| 10 | 2 | `header_len` | `32` |
| 12 | 4 | `owner` | File-type-specific |
| 16 | 8 | `sequence` | File-type-specific |
| 24 | 8 | `header_checksum` | `XXH3(header[0..24])` |

The header checksum covers the magic, version, length, owner, and sequence.
An incomplete header, checksum mismatch, unsupported version, or invalid
file-type interpretation is authoritative corruption and MUST NOT be repaired.

### Segment header interpretation

- `magic` is ASCII `CAMSEG01` (`43 41 4d 53 45 47 30 31`).
- `owner` is the logical stream ID as a `u32`.
- `sequence` is the segment ID as a `u64`.
- Both values MUST match the manifest state and canonical path used to open the
  file.

Version 1 uses the historical term `shard_id` for this stream ID in some
manifest JSON. There is no distinct physical-shard namespace.

### Manifest header interpretation

- `magic` is ASCII `CAMMAN01` (`43 41 4d 4d 41 4e 30 31`).
- `(owner, sequence) = (0, 0)` identifies an ordinary event-log manifest.
- `owner > 0` identifies a checkpoint manifest. If it contains `N` checkpoint
  frames, `owner = N + 1` and `sequence` is the checkpoint descriptor checksum
  defined below.

The `+1` encoding reserves owner `0` for the ordinary manifest. An empty
checkpoint therefore has owner `1`, and its sequence is XXH3-64 of the empty
byte string. Because `owner` is a `u32`, `N` cannot exceed
`u32::MAX - 1`.

## Segment files

The canonical committed grammar is:

```text
segment = file_header epoch*
epoch   = record_frame+ epoch_commit_frame
```

An active file may physically end with an incomplete or uncommitted epoch
after a crash. A sealed file MUST match the committed grammar exactly. Epochs
never cross a segment boundary or logical-stream boundary.

### Segment frame prefix

Every record and epoch-commit frame starts with this 48-byte prefix:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMREC01` |
| 8 | 2 | `version` | `1` |
| 10 | 1 | `kind` | `1` or `2` |
| 11 | 1 | `flags` | `0` |
| 12 | 4 | `metadata_len` | Little-endian `u32`, at most 16 MiB |
| 16 | 8 | `payload_len` | Little-endian `u64` |
| 24 | 8 | `frame_len` | `48 + metadata_len + payload_len` |
| 32 | 8 | `payload_checksum` | Kind-specific |
| 40 | 8 | `descriptor_checksum` | `XXH3(prefix[0..40] || metadata)` |

The physical frame is:

```text
48-byte prefix || metadata[metadata_len] || payload[payload_len]
```

The component lengths and redundant `frame_len` MUST agree. The descriptor
checksum binds all prefix fields, including `payload_checksum`, plus the exact
metadata bytes. It deliberately does not read the payload bytes.

### Kind 1: record

For a record frame, `payload_checksum` MUST equal `XXH3(payload)`, including
for an empty payload. Its metadata is encoded as:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | `record_id_len`, little-endian `u32` |
| 4 | `record_id_len` | Non-empty UTF-8 record ID |
| `4 + record_id_len` | Remaining bytes | Opaque caller metadata |

The complete metadata area, including the four-byte ID length, MUST not exceed
16 MiB. A record ID MUST not exceed 16,384 UTF-8 bytes.

Record IDs are scoped by logical stream. The public format contract requires a
producer never to reuse an ID during that stream's lifetime, even after
release or reclamation. Recovery rejects duplicate IDs among the segment files
still represented by the manifest; it cannot prove uniqueness against history
that has already been reclaimed.

### Kind 2: epoch commit

An epoch-commit frame has these fixed fields:

- `metadata_len = 24`;
- `payload_len = 0`;
- `frame_len = 72`; and
- `payload_checksum = 0`.

Its metadata is:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `epoch_start` | Absolute offset of the epoch's first record prefix |
| 8 | 8 | `frame_count` | Number of record frames; greater than zero |
| 16 | 8 | `descriptors_checksum` | See below |

`descriptors_checksum` is XXH3-64 over the complete 48-byte prefixes of the
epoch's kind-1 record frames in physical order:

```text
XXH3(record_prefix_1 || ... || record_prefix_N)
```

It excludes the commit prefix. The commit frame's own
`descriptor_checksum` still protects its 24 metadata bytes. `epoch_start`
MUST equal the offset immediately after the file header or previous valid
commit frame, and `frame_count` MUST equal the exact number of pending record
frames.

### Segment recovery and lazy payload validation

Recovery scans from offset 32, validates each prefix and its metadata, and
accumulates record descriptors without loading or hashing payloads. It
publishes accumulated records only after a valid matching epoch commit. On a
lazy `read` or `read_many`, Camus revalidates the segment header, frame prefix,
metadata checksum and metadata encoding, verifies that the supplied physical
location matches the descriptor, reads the payload, and checks
`payload_checksum`.

Consequently, payload corruption is reported when that payload is read. It is
not treated as a repairable active tail during open.

The repair boundary is intentionally narrow:

| Condition | Active segment's final epoch | Sealed segment or earlier epoch |
| --- | --- | --- |
| Incomplete prefix or frame | Truncate to `epoch_start` and sync | Fail closed |
| Invalid prefix, inconsistent length, or descriptor checksum | Truncate only if no later valid commit exists; then sync | Fail closed |
| EOF after complete records but before their commit | Truncate to `epoch_start` and sync | Fail closed |
| Checksum-valid unknown kind, invalid record metadata, or invalid commit semantics | Fail closed | Fail closed |
| Invalid file header | Fail closed | Fail closed |

Recovery may scan for evidence of a valid later commit to distinguish a torn
tail from corruption before a suffix. It MUST NOT use that scan to skip bytes,
reorder frames, or salvage a later epoch.

## Manifest file

The manifest stores stream and segment lifecycle, persisted segment creation
times, and stream-scoped release markers. It contains the shared header
followed by zero or more 32-byte-prefixed events.

### Manifest frame prefix

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMMRC01` |
| 8 | 2 | `version` | `1` |
| 10 | 1 | `kind` | `1..=6` as defined below |
| 11 | 1 | `flags` | `0` |
| 12 | 4 | `metadata_len` | Little-endian `u32`, at most 16 MiB |
| 16 | 8 | `frame_len` | `32 + metadata_len` |
| 24 | 8 | `frame_checksum` | `XXH3(prefix[0..24] || metadata)` |

The physical frame is `32-byte prefix || metadata[metadata_len]`. Manifest
frames have no payload. The redundant total and component lengths MUST agree.

### JSON metadata rules

Manifest metadata is a UTF-8 JSON object. Canonical Camus writers emit compact
JSON without insignificant whitespace and use the field order shown below.
Readers treat object-member order and insignificant whitespace as
non-semantic, but MUST reject malformed JSON, duplicate fields, unknown
fields, missing required fields, incorrect JSON types, and integers outside
the target unsigned range. `created_at_unix_millis`, where present, is Unix
time in whole milliseconds stored as a `u64`.

The event kinds are:

| Kind | Name | Checkpoint | Event-log suffix |
| ---: | --- | :---: | :---: |
| 1 | Default-stream release | Yes | Yes |
| 2 | Segment rotation | No | Yes |
| 3 | Segment removal | No | Yes |
| 4 | Segment snapshot | Yes | No |
| 5 | Segment timestamp | No | Yes |
| 6 | Nondefault-stream release | Yes | Yes |

#### Kind 1: default-stream release

```json
{"record_ids":["id"]}
```

`record_ids` is a non-empty array of unique, valid record IDs. The default
stream must already be declared. Repeating an ID in a later release event is
semantically idempotent.

#### Kind 2: segment rotation

```json
{"shard_id":7,"previous_segment_id":9,"new_segment_id":10,"created_at_unix_millis":123}
```

The canonical field order is `shard_id`, `previous_segment_id`,
`new_segment_id`, then optional `created_at_unix_millis`. `shard_id` is the
logical stream ID.

- Stream initialization uses `previous_segment_id: null`, requires an empty
  segment set, and requires `new_segment_id: 0`.
- Later rotation requires `previous_segment_id` to name the active segment and
  `new_segment_id = previous_segment_id + 1`.
- Applying the event seals the previous segment and makes the new segment the
  sole active segment.
- Current writers include `created_at_unix_millis`. Readers accept its absence
  for early version-1 roots.

#### Kind 3: segment removal

```json
{"shard_id":7,"segment_ids":[1,2]}
```

`segment_ids` is non-empty and contains no duplicates. Every listed segment
must exist in `shard_id` and be sealed. The active segment cannot be removed.
Canonical writers split more than 65,536 IDs into consecutive removal events.

#### Kind 4: segment snapshot

```json
{"shard_id":0,"segment_id":9,"lifecycle":"Sealed"}
```

The canonical field order is `shard_id`, `segment_id`, `lifecycle`, then
optional `created_at_unix_millis`. `lifecycle` is exactly `"Active"` or
`"Sealed"`. Each `(shard_id, segment_id)` pair may occur only once in a
checkpoint. A snapshot outside the checkpoint prefix is corruption.

#### Kind 5: segment timestamp

```json
{"shard_id":7,"segment_id":9,"created_at_unix_millis":123}
```

The segment must already exist, and no creation timestamp may already be
recorded for it.

#### Kind 6: nondefault-stream release

```json
{"stream_id":7,"record_ids":["id"]}
```

`stream_id` must be nonzero and already declared. `record_ids` follows the
same rules as kind 1. The distinct event preserves compatibility with the
original stream-0 release encoding.

### Manifest state invariants

Events are applied in physical order. A valid resulting manifest state MUST
satisfy all of these rules:

- every declared stream has exactly one active segment;
- that active segment has the greatest retained segment ID in its stream;
- initialization and rotation allocate contiguous IDs, although removal of
  sealed segments may leave gaps in a later checkpoint;
- a timestamp references a retained segment and is assigned at most once;
- default-stream releases use kind 1, while nondefault releases use kind 6;
  and
- release state never references an undeclared stream.

The manifest records lifecycle and release state, not application-level
delivery facts. Public APIs validate that released IDs are known before
writing, while the wire transition itself only requires a valid ID and a
declared stream.

### Checkpoint manifests

For a checkpoint header, let `N = owner - 1`. Exactly the first `N` manifest
frames after the header form the checkpoint. Only kinds 1, 4, and 6 are valid
there. The checkpoint descriptor checksum is:

```text
XXH3(checkpoint_prefix_1 || ... || checkpoint_prefix_N)
```

Each input is the complete 32-byte frame prefix. Metadata is bound
transitively through each prefix's `frame_checksum`. The result MUST equal the
header `sequence`. After all `N` frames are applied, the checkpoint MUST form a
complete state satisfying the manifest invariants before any ordinary suffix
event is accepted.

Canonical checkpoints use this deterministic order:

1. segment snapshots sorted by numeric stream ID and then segment ID;
2. default-stream release IDs in UTF-8 byte order, one ID per event; and
3. nondefault streams in numeric order, then their release IDs in UTF-8 byte
   order, one ID per event.

Frames after the checkpoint prefix are ordinary event-log suffix events. They
are not included in the header descriptor checksum.

### Manifest recovery and repair

The manifest header and complete checkpoint prefix are authoritative and never
repairable. A checkpoint count mismatch, incomplete checkpoint frame,
checksum failure, disallowed kind, invalid schema, or invalid completed state
MUST fail closed.

After an ordinary header or complete checkpoint, only a structurally
incomplete or checksum-damaged final event may be truncated. Redundant lengths
are used to decide whether a damaged candidate can be the final event. If a
valid later event can be found after the damaged offset, recovery fails closed
instead of truncating. A complete checksum-valid event with an unknown kind,
invalid JSON or field, snapshot in the suffix, or invalid lifecycle transition
always fails closed. Every manifest truncation is followed by `sync_data`.

As with segments, scanning for a later valid frame is corruption evidence, not
a salvage mechanism.

## Filesystem publication protocol

Correct bytes are insufficient without the ordering below. The complete
contract and crash-window rationale are in the
[architecture](architecture.md).

| Operation | Required publication order |
| --- | --- |
| Create manifest | Write header to `MANIFEST.create`; `sync_data`; rename to `MANIFEST`; sync root directory |
| Create segment | Write header to `.log.tmp`; `sync_data`; rename to `.log`; sync segment directory |
| Initialize or rotate stream | Publish the new segment first; append rotation event; `sync_data` manifest |
| Append epoch | Write every record frame; write one commit frame; perform one segment `sync_data`; only then report success |
| Release | Append release event; `sync_data` manifest; only then exclude records from pending recovery |
| Reclaim | Append all required removal events; `sync_data` manifest; delete declared segment files; sync affected segment directories |
| Compact manifest | Write complete header and checkpoint to `MANIFEST.compact`; `sync_data`; atomic rename over `MANIFEST`; sync root directory |
| Repair a tail | Truncate only at the permitted boundary; `sync_data` the repaired file |

An I/O error does not prove whether a durability boundary was crossed. After
an uncertain failure, the open handle is poisoned and the root must be reopened
so recovered bytes, rather than in-memory assumptions, decide the outcome.

## Directory reconciliation

After the manifest validates, every manifest-declared segment MUST exist at
its canonical path and have a matching authoritative header. A missing
declared segment fails closed.

Temporary segment files and header-only segments left by interrupted creation
may be removed with the appropriate directory sync. An unmanifested canonical
tail segment containing bytes beyond its 32-byte header fails closed: those
bytes might contain a durable epoch whose lifecycle event has an uncertain
outcome. Segments already excluded by a durable removal event remain removed
state even if stale directory entries reappear.

## Compatibility and evolution

Version 1 deliberately reserves no extension behavior that an old reader
would silently ignore:

- both frame `flags` bytes must remain zero;
- unknown segment or manifest kinds fail closed;
- unknown manifest JSON fields fail closed; and
- magic values, field meanings, checksum ranges, canonical JSON field order,
  and stable vectors are frozen.

Therefore a new kind, field, flag, checksum algorithm, compression or
encryption mode, or changed field interpretation is not a compatible v1
extension. It requires a new explicitly specified format and an upgrade plan.
Readers MUST reject unsupported authoritative versions; they MUST NOT guess a
format or reinterpret bytes in place.

Opening a root does not silently migrate its format. Any future incompatible
migration must operate explicitly, preserve a closed-root backup, and define
both forward and rollback behavior. Crate semantic versions do not implicitly
change the on-disk format version.

## Stable version-1 vectors

These lowercase hexadecimal vectors are part of the compatibility contract.
They are also asserted by `version_one_wire_codecs_have_stable_bytes`.

### Segment header

Input: magic `CAMSEG01`, version `1`, stream `7`, segment `9`.

```text
43414d534547303101002000070000000900000000000000c91e29e7a55c38be
```

### Record metadata and prefix

Input: record ID `id`, opaque metadata `00 ff`, payload `abc`.

Encoded record metadata:

```text
02000000696400ff
```

The 48-byte record prefix (`payload_len = 3`, `frame_len = 59`) is:

```text
43414d5245433031010001000800000003000000000000003b0000000000000050392f89945faf789e9d7293d6a7e623
```

The complete record frame is that prefix followed by metadata
`02000000696400ff` and payload `616263`.

### Epoch-commit metadata

Input: `epoch_start = 32`, `frame_count = 1`, and
`descriptors_checksum = 0x0102030405060708`.

```text
200000000000000001000000000000000807060504030201
```

### Manifest release prefix

Canonical metadata:

```text
{"record_ids":["id"]}
```

Its kind-1 manifest prefix is:

```text
43414d4d52433031010001001500000035000000000000000b9fe05cff160f8e
```

## Conformance checklist

A version-1 implementation is not conforming unless it:

- uses checked arithmetic and enforces metadata bounds before allocation;
- validates every redundant length and exact checksum input range;
- publishes records only after a matching epoch commit;
- preserves opaque metadata and payload bytes exactly;
- validates all manifest schemas and lifecycle transitions;
- never repairs an authoritative header, sealed segment, checkpoint, semantic
  error, or damage before a valid suffix;
- follows the filesystem sync and rename ordering above; and
- keeps the stable vectors byte-for-byte unchanged.
