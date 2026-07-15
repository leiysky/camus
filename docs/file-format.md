# Camus file format version 1

This document is the normative byte-level specification for Camus on-disk
format version 1. It defines canonical paths, packed layouts, checksum inputs,
codecs, validation rules, and the compatibility boundary.

The [architecture](architecture.md) is authoritative for filesystem ordering,
durability, recovery, release, and reclamation. The
[README](../README.md) is authoritative for the public project and API
boundary. If the documents appear to disagree, this document governs bytes and
the architecture governs state transitions.

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. Byte
ranges use half-open notation: `[a, b)` includes `a` and excludes `b`.

## Format boundary

Format v1 encodes:

- one immutable 128-bit root identity;
- root-wide physical data segments;
- logical stream IDs and stream-local record sequence intervals;
- exact opaque metadata and payload bytes;
- append-epoch recovery boundaries;
- exact record-level release state;
- segment seal and removal state; and
- durable stream sequence high-waters.

It does not encode consumer identity, attempts, subscriptions, claims,
application schemas, cross-stream ordering, compression, encryption, or
authentication.

Release `1.0.0-rc.1` establishes this format v1 as the published compatibility
boundary. There is no migration or compatibility requirement for bytes written
by earlier unpublished development revisions. The compatibility rules at the
end of this document apply to roots written by `1.0.0-rc.1` and later
compatible releases.

## Primitive encoding

All integers are unsigned and little-endian. Structures are packed exactly as
shown with no alignment bytes or padding. Every length and offset calculation
MUST use checked `u64` arithmetic. Overflow is corruption, never wraparound.

`RootId` is an opaque 16-byte value stored and copied verbatim. It has no UUID
field-endianness interpretation.

Every checksum and digest is seeded XXH3-64. The fixed format-v1 seed is:

```text
u64::from_le_bytes(*b"CAMUSV1!") = 0x21315653554d4143
```

The seed is implied by v1, is not stored, is not configurable, and is not
secret. A checksum value is encoded as a little-endian `u64`. In this document,
`H(bytes)` means seeded XXH3-64 over exactly `bytes`; `H(a || b)` means one
streaming hash over the exact concatenation, not a hash of separate digests.

XXH3 detects accidental corruption. It is not a MAC and does not protect
against deliberate modification by an actor who can recompute checksums.

Versioned eight-byte magics fix the size and interpretation of every structure
that follows them. Except for the root superblock's explicit format version,
v1 structures contain no redundant version, flags, reserved fields, or header
lengths.

## Validation and configured limits

Readers MUST validate a fixed prefix and its checksum before trusting a length
from that prefix. A declared variable region MUST fit exactly within its
validated enclosing file or structure. Readers SHOULD stream or skip large
regions rather than allocate from an unchecked length.

`max_epoch_bytes`, `max_release_records`, `max_commit_bytes`, and
`segment_bytes` are writer/admission bounds, not stored format fields. Lowering
a bound when reopening does not make already durable larger units corrupt;
recovery validates them from their enclosing file lengths and checksums. The
new bound applies to future operations. Root capacity is separately validated
at open as specified by the architecture.

Canonical writers enforce these configuration relationships:

```text
segment_bytes >= 48 + max_epoch_bytes + 48
max_commit_bytes >= max(
  max_epoch_bytes,
  largest Release frame produced by max_release_records
)
```

The first expression reserves both the segment header and seal footer around
one maximum epoch. All arithmetic MUST be checked.

## Canonical root layout

```text
<root>/
  ROOT
  camus.lock
  MANIFEST.chk
  MANIFEST.log
  segments/
    segment-00000000000000000000.log
    ...
```

Segment names contain a zero-padded 20-digit decimal physical segment ID.
Canonical writers allocate IDs monotonically from zero. `u64::MAX` is reserved
as the exhausted `next_segment_id` sentinel and MUST NOT appear in a segment
file name, segment header, or manifest lifecycle body.

The recognized transactional names are:

- `ROOT.tmp`;
- `MANIFEST.chk.tmp`;
- `MANIFEST.log.tmp`; and
- `segments/segment-<20-digit-id>.log.tmp`.

They are publication artifacts, not independent state sources. No other file
name is part of format v1. `camus.lock` provides process ownership and has no
versioned payload.

## Root superblock

`ROOT` is exactly 40 bytes:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMROOT1` |
| 8 | 8 | `format_version` | Exactly `1` |
| 16 | 16 | `root_id` | Opaque random bytes |
| 32 | 8 | `superblock_checksum` | `H(ROOT[0..32])` |

Trailing bytes, an incomplete superblock, an unsupported version, or a
checksum failure are authoritative corruption. Recovery MUST NOT synthesize a
replacement `RootId` for an existing root.

Creation writes the complete bytes to `ROOT.tmp`, syncs the file, atomically
renames it to `ROOT`, and syncs the root directory. A complete filesystem copy
therefore retains the same root identity.

## Public RecordId serialization

Although individual records do not store a complete ID, the public opaque
token has this fixed 32-byte serialization:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | `root_id`, verbatim |
| 16 | 8 | `stream_id`, little-endian `u64` |
| 24 | 8 | `sequence`, little-endian `u64` |

There is no token checksum. Root and stream scope plus durable sequence state
are validated before an operation using the token is admitted. The numeric
components are not a physical address or public ordering API.

## Data segment grammar

A canonical segment is either active or sealed:

```text
active_segment = SegmentHeader Epoch+
sealed_segment = SegmentHeader Epoch+ SegmentFooter

Epoch = EpochHeader Record+ EpochCommit
Record = RecordDescriptor Metadata Payload
```

Segments are root-wide and may contain epochs for different logical streams.
Every canonical segment contains at least one complete epoch. A segment with
only a header is invalid. Epochs and append commit groups never cross segment
files.

### SegmentHeader

Every segment begins with this 48-byte header:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMSEG01` |
| 8 | 16 | `root_id` | Must equal `ROOT.root_id` |
| 24 | 8 | `segment_id` | Must equal canonical file name; not `u64::MAX` |
| 32 | 8 | `created_at_unix_millis` | Persisted age baseline |
| 40 | 8 | `header_checksum` | `H(header[0..40])` |

`created_at_unix_millis` is the wall-clock baseline captured for the segment's
first append group. It has rollover-policy meaning only; it is not a record
timestamp.

### RecordDescriptor

Every record begins with this 40-byte descriptor:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `metadata_len` | Opaque metadata byte length |
| 8 | 8 | `payload_len` | Opaque payload byte length |
| 16 | 8 | `metadata_checksum` | `H(metadata)` |
| 24 | 8 | `payload_checksum` | `H(payload)` |
| 32 | 8 | `descriptor_checksum` | `H(descriptor[0..32])` |

The exact encoded record is:

```text
40-byte descriptor
metadata[metadata_len]
payload[payload_len]
```

Metadata and payload MAY be empty; their checksum is still the seeded hash of
the empty byte string. Complete record length is derived as
`40 + metadata_len + payload_len`. There is no record magic, kind, ID, total
length, or padding. The enclosing epoch provides those boundaries and logical
identity.

### EpochHeader

Each append request starts with this 48-byte header:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMEPH01` |
| 8 | 8 | `stream_id` | Any `u64` value |
| 16 | 8 | `first_sequence` | First record identity in this epoch |
| 24 | 8 | `record_count` | Greater than zero |
| 32 | 8 | `records_bytes` | Exact sum of complete encoded records |
| 40 | 8 | `header_checksum` | `H(header[0..40])` |

`first_sequence + record_count - 1` MUST fit in `u64`. Record ordinal `i`
within the epoch has sequence `first_sequence + i`. Intervals for one stream
MUST be strictly increasing and nonoverlapping in physical append order.
Before allocating a record index, a reader MUST also prove with checked
arithmetic that `records_bytes >= 40 * record_count` and that the declared
epoch boundary fits its enclosing segment.
Contiguous allocation relative to checkpoint and removal state is validated as
specified under checkpoint and log replay. A stream with no earlier durable
identity starts at sequence zero.

`records_bytes` MUST equal the checked sum of all 40-byte descriptors and their
declared opaque bodies. It does not include either epoch envelope.

### EpochCommit

Each epoch ends with this 40-byte commit:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMCMT01` |
| 8 | 8 | `epoch_start` | Absolute segment offset of its `EpochHeader` |
| 16 | 8 | `epoch_bytes` | Header, records, and commit |
| 24 | 8 | `epoch_digest` | Defined below |
| 32 | 8 | `commit_checksum` | `H(commit[0..32])` |

The redundant length MUST satisfy:

```text
epoch_bytes = 48 + records_bytes + 40
```

The commit MUST begin exactly at `epoch_start + 48 + records_bytes`, and the
next structure MUST begin exactly at `epoch_start + epoch_bytes`.

Let `D1..Dn` be the exact 40-byte record descriptors in physical order. The
epoch digest is:

```text
H(complete 48-byte EpochHeader || D1 || ... || Dn)
```

Each descriptor binds its stored metadata and payload checksums, so the epoch
digest transitively binds body integrity without reading body bytes during
structural recovery.

A record becomes recoverable only as part of a complete valid epoch. An
uncommitted body never advances sequence high-water.

### SegmentFooter

A sealed segment ends with this 48-byte footer:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMSEA01` |
| 8 | 8 | `segment_id` | Must equal header and canonical name |
| 16 | 8 | `segment_bytes` | Exact complete file length including footer |
| 24 | 8 | `epoch_count` | Exact number of epochs; greater than zero |
| 32 | 8 | `segment_digest` | Defined below |
| 40 | 8 | `footer_checksum` | `H(footer[0..40])` |

Let `C1..Cn` be the exact 40-byte `EpochCommit` structures in physical order.
The segment digest is:

```text
H(complete 48-byte SegmentHeader || C1 || ... || Cn)
```

`segment_bytes` MUST equal actual file length, and the footer MUST begin at
`segment_bytes - 48`. No bytes may follow it. A valid footer makes the file
physically immutable. Manifest state determines whether the completed seal has
been published logically.

## Structural segment validation

Normal open validates the segment header, epoch headers, every record
descriptor, epoch commits, and any footer. It uses checked lengths to skip
metadata and payload without hashing them. It MUST still prove that every body
range fits before the predicted commit and file boundary.

When a pending record is read, Camus revalidates its structural location, reads
the complete metadata and payload, and checks both stored body checksums before
returning either. A body mismatch is authoritative corruption of a committed
epoch. It is never repaired by truncation.

The active-tail repair boundary is exact:

| Condition | Manifest-active final segment | Sealed or earlier segment |
| --- | --- | --- |
| Incomplete final `EpochHeader` | Truncate to its start; sync | Fail closed |
| Valid header but incomplete final descriptor or body | Truncate to epoch start; sync | Fail closed |
| Complete records but incomplete/missing final commit | Truncate to epoch start; sync | Fail closed |
| Incomplete trailing footer after complete epochs | Truncate to prior commit end; sync | Fail closed |
| Complete structure with bad checksum, digest, length, magic, or semantics | Fail closed | Fail closed |
| Damage before evidence of a later valid structure | Fail closed | Fail closed |
| Invalid authoritative segment header | Fail closed | Fail closed |

Repair never publishes a partial epoch and never searches past damage to
salvage later bytes. A complete valid footer without matching
`SegmentSealed` state is not damage; recovery completes that manifest
publication as specified by the architecture.

## Manifest log

`MANIFEST.log` contains a fixed header followed by zero or more atomic frames:

```text
ManifestLogHeader ManifestFrame*
```

### ManifestLogHeader

The header is 40 bytes:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMLOG01` |
| 8 | 16 | `root_id` | Must equal `ROOT.root_id` |
| 24 | 8 | `base_seq` | Sequence expected for the first frame |
| 32 | 8 | `header_checksum` | `H(header[0..32])` |

The initial empty log has `base_seq = 1`. A fresh log published after a
checkpoint normally uses `last_applied_seq + 1`. When sequence space is
exhausted, no further manifest mutation may be written.

If frames are present, the first frame's `manifest_seq` MUST equal `base_seq`
and every subsequent value MUST be exactly one greater. Replay relative to a
checkpoint is defined below.

### ManifestFrameHeader

Every frame uses this 48-byte header followed immediately by its body:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMCTL01` |
| 8 | 8 | `manifest_seq` | Strict nonzero root-wide sequence |
| 16 | 8 | `kind` | Exactly `1`, `2`, or `3` |
| 24 | 8 | `body_len` | Exact kind-specific body length |
| 32 | 8 | `body_checksum` | `H(body)` |
| 40 | 8 | `header_checksum` | `H(header[0..40])` |

Complete frame length is `48 + body_len`. There is no repeated length or
padding. The header checksum MUST validate before `body_len` is trusted. The
body checksum MUST validate before kind-specific fields are applied.

### Kind 1: Release

All fields are `u64`:

```text
offset  size  field
0       8     stream_id
8       8     released_count
16      8     range_count
24      16*N  ranges[N] = { start, len }
```

The exact body length is `24 + 16 * range_count`. `released_count` and
`range_count` are greater than zero. A range represents sequences
`start..=start + len - 1`; `len` is nonzero and its end MUST fit in `u64`.

Ranges are sorted by `start`, nonoverlapping, nonadjacent, and therefore
maximally coalesced. The checked sum of every `len` equals
`released_count`. Every represented ID belongs to `stream_id`, was durable by
that replay point, and was pending immediately before this frame. Empty or
all-no-op release requests do not write a frame.

`max_release_records` limits newly written frames. Recovery MUST accept a
structurally valid older frame larger than the current writer limit; the body
length and enclosing file remain the decoding bounds.

### Kind 2: SegmentSealed

The body is exactly 32 bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | `segment_id` |
| 8 | 8 | `segment_bytes` |
| 16 | 8 | `epoch_count` |
| 24 | 8 | `segment_digest` |

All four fields MUST exactly match the validated `SegmentFooter` of the named
segment. The segment must previously be manifest-active. Repeating or
contradicting the transition is corruption.

### Kind 3: SegmentRemoved

All fields are `u64`:

```text
offset  size  field
0       8     segment_id
8       8     highwater_count
16      16*N  highwaters[N] = { stream_id, sequence_highwater }
```

The exact body length is `16 + 16 * highwater_count`. The count MAY be zero.
Entries are sorted by strictly increasing `stream_id` and contain no duplicate
stream. Each included high-water is the inclusive greatest durable sequence
that must remain known before deleting this segment's last physical evidence.
Entries MUST advance previously persisted high-water state; canonical writers
omit no-op entries.

The named segment must be manifest-sealed and every physical record in it must
be durably released. Applying the frame makes the segment authoritatively
absent before its file is deleted. A second removal or removal of an active or
partly pending segment is corruption.

There is no `SegmentCreated`, `StreamCreated`, or standalone
`StreamHighWater` frame. A canonical segment file proves creation, complete
epochs prove stream existence, checkpoints fold high-waters, and removal
carries the advances required for safe deletion.

## Manifest checkpoint

`MANIFEST.chk` contains one header and one canonical complete-state body:

```text
CheckpointHeader CheckpointBody
```

### CheckpointHeader

The header is 56 bytes:

| Offset | Size | Field | Validation |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `CAMCHK01` |
| 8 | 16 | `root_id` | Must equal `ROOT.root_id` |
| 24 | 8 | `last_applied_seq` | Greatest manifest sequence represented; initially `0` |
| 32 | 8 | `body_len` | Exact remaining file length |
| 40 | 8 | `body_checksum` | `H(body)` |
| 48 | 8 | `header_checksum` | `H(header[0..48])` |

The complete file length MUST equal `56 + body_len`. The checkpoint header,
body, and checksum are authoritative and never tail-repaired.

### CheckpointBody prefix

The body begins with three `u64` fields:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | `next_segment_id` |
| 8 | 8 | `stream_count` |
| 16 | 8 | `segment_count` |

`next_segment_id` is the next root-wide ID available for allocation. It is
zero before any segment is published and otherwise strictly greater than every
segment ID ever published. `u64::MAX` means the physical ID space is exhausted
and is never itself allocated.

The prefix is followed by exactly `stream_count` high-water entries and then
exactly `segment_count` variable-length segment entries. No trailing bytes are
permitted.

### Stream high-water entries

Each entry is 16 bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | `stream_id` |
| 8 | 8 | `sequence_highwater` |

Entries are sorted by strictly increasing `stream_id`. There is exactly one
entry for every durable-known stream, including streams with no extant data.
`sequence_highwater` is the inclusive greatest sequence durable by checkpoint
publication. The presence of the entry distinguishes a stream whose first and
greatest durable sequence is zero from an unknown stream.

### Segment entries

Each segment entry begins with this 72-byte fixed part:

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | `segment_id` |
| 8 | 8 | `lifecycle` |
| 16 | 8 | `segment_bytes` |
| 24 | 8 | `epoch_count` |
| 32 | 8 | `segment_digest` |
| 40 | 8 | `record_count` |
| 48 | 8 | `released_count` |
| 56 | 8 | `release_encoding` |
| 64 | 8 | `encoding_unit_count` |

Entries are sorted by strictly increasing `segment_id` and describe every
extant canonical segment at checkpoint publication.

`lifecycle` is:

| Value | Meaning |
| ---: | --- |
| 1 | Active |
| 2 | Sealed |

At most one entry is active, and it has the greatest extant segment ID. An
active entry stores zero in all three footer fields: `segment_bytes`,
`epoch_count`, and `segment_digest`. A sealed entry copies those fields exactly
from its validated footer.

`record_count` is the number of complete physical records represented by this
checkpoint for the segment. It is nonzero. A sealed segment must contain
exactly that count. An active segment may gain later complete epochs; records
beyond the checkpoint count begin pending and are affected only by later
manifest frames.

`released_count` is the exact number of released physical ordinals among
`0..record_count`. The encoding that follows the fixed part represents exactly
that set.

### Release encoding 1: ranges

`release_encoding = 1`. Each of `encoding_unit_count` units is 16 bytes:

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | `start_ordinal` |
| 8 | 8 | `len` |

Ordinals are zero-based in physical record order within the segment. Ranges
are nonempty, sorted, disjoint, nonadjacent, and entirely below
`record_count`. Their checked total equals `released_count`.

Zero released records use range encoding with `encoding_unit_count = 0`.

### Release encoding 2: bitmap

`release_encoding = 2`. Exactly `encoding_unit_count` little-endian `u64`
words follow, where:

```text
encoding_unit_count = ceil(record_count / 64)
```

Bit `j` of word `i`, with the least significant bit numbered zero, represents
physical ordinal `64 * i + j`. Every unused high bit in the final word is zero.
The total number of set bits equals `released_count`.

### Canonical release-encoding choice

For a nonempty released set, let `range_count` be the maximally coalesced range
count and `bitmap_bytes = 8 * ceil(record_count / 64)`. A canonical checkpoint
uses ranges exactly when:

```text
16 * range_count <= bitmap_bytes
```

It uses the bitmap otherwise. The range representation wins a size tie. A
reader MUST reject a valid-looking but noncanonical choice or noncanonical
range/bitmap contents.

## Checkpoint and log replay

Recovery validates the checkpoint first, then the manifest-log header and
every complete frame.

Let `F = checkpoint.last_applied_seq` and `B = log.base_seq`:

- `B` MUST NOT create a gap after `F`;
- if the log is fresh, `B = F + 1` unless sequence space is exhausted;
- an old log left by interrupted compaction may begin at or before `F`;
- frames with sequence at or below `F` are fully validated but not applied
  again; and
- the first frame above `F` MUST be `F + 1`, followed by a contiguous suffix.

When `F = u64::MAX`, no newer manifest frame is representable. A newly
compacted empty log uses `base_seq = u64::MAX` and MUST remain empty.

A frame-sequence gap, reversal, duplicate inside the log, arithmetic overflow,
or incompatible base is corruption. A checksum-valid frame with an invalid
state transition is also corruption.

The checkpoint baseline and replayed frames are reconciled with data files:

- checkpoint segment entries describe files extant at checkpoint time;
- a canonical segment absent from the checkpoint is valid only as a
  monotonically allocated self-published segment after that checkpoint;
- `SegmentSealed` must agree with its footer;
- a `SegmentRemoved` frame permits the canonical file to be absent and makes a
  leftover file deletable; and
- a missing segment without durable removal is corruption.

Release state in checkpoint entries addresses the checkpoint record-count
baseline. Later Release frames address logical sequences and may release both
baseline records and records appended afterward.

For sequence validation, the first `record_count` physical records of every
checkpoint segment entry form its checkpoint baseline. That boundary MUST fall
between complete epochs, never inside one. Every baseline record has a sequence
at or below its stream's checkpoint high-water. Baseline intervals are
strictly ordered and nonoverlapping; gaps are permitted because older physical
segments may already have been removed.

Every epoch wholly after the checkpoint baseline, including every epoch in a
self-published post-checkpoint segment, advances its stream from the current
inclusive high-water by exactly one contiguous interval. A replayed
`SegmentRemoved` high-water may supply that advance when the segment file is
already absent. No recovered interval may overlap or move behind checkpoint,
earlier epoch, or removal evidence.

## Manifest tail repair

Only an incomplete final physical frame in `MANIFEST.log` is repairable:

- fewer than 48 bytes remaining at the final frame boundary; or
- a complete valid frame header whose declared body ends beyond physical EOF.

Recovery truncates such a tail to the preceding complete frame boundary and
syncs the log before continuing. A complete header with a bad checksum, a
complete body with a bad checksum, an unknown kind, invalid body length,
noncanonical body, sequence fault, or invalid transition fails closed. The
checkpoint and both container headers are never repaired.

## Publication protocols

Correct bytes are insufficient without the following order. A reported
success requires every listed durability step for that operation.

| Operation | Required order |
| --- | --- |
| Create root identity | Write complete `ROOT.tmp`; sync file; rename to `ROOT`; sync root directory |
| Create segment directory | Create `segments/`; sync root directory before publishing any segment |
| Create initial checkpoint | Write complete `MANIFEST.chk.tmp`; sync file; rename to `MANIFEST.chk`; sync root directory |
| Create/replace manifest log | Write complete `MANIFEST.log.tmp`; sync file; rename to `MANIFEST.log`; sync root directory |
| Self-publish segment | Write header and first complete append group to segment `.tmp`, plus footer if immediately sealed; sync file; rename canonical; sync `segments/` |
| Append group | Write every record and epoch commit; one `sync_data` on the containing segment; only then report covered appends successful |
| Seal segment | Stop appends; write footer; sync segment; append complete `SegmentSealed`; sync manifest log |
| Release group | Append every complete `Release` frame; one manifest-log `sync_data`; only then exclude records and report success |
| Reclaim segment | Append `SegmentRemoved`; sync manifest log; delete canonical segment; sync `segments/` |
| Compact checkpoint | Write complete checkpoint temp; sync; rename over checkpoint; sync root directory; only then replace manifest log through its own temp/sync/rename/directory-sync sequence |
| Repair active tail | Truncate only at a permitted boundary; `sync_data` the repaired file |

Root initialization creates and publishes the empty checkpoint and manifest
log before `open` reports success. The initial checkpoint has
`last_applied_seq = 0`, `next_segment_id = 0`, and zero stream and segment
entries; the initial log has `base_seq = 1` and no frames. A recognized partial
empty-root initialization may be completed only when no canonical segment or
nonempty manifest state exists. Otherwise missing authoritative artifacts fail
closed.

An I/O error does not prove whether a durability boundary or rename became
durable. After a post-admission failure, the open root is poisoned and reopen
uses recovered bytes as authority.

## Directory reconciliation

Recovery first distinguishes canonical names from recognized temporary names.
It MAY remove a recognized temporary file that never became canonical, with
the required directory sync. A temporary file is never replayed as an
additional segment, checkpoint, or log.

Every canonical artifact MUST have a valid header with the root identity and
file-name identity required above. Unknown canonical-looking files,
unexplained segment IDs, duplicate physical identities, and mismatched root IDs
fail closed.

A canonical segment covered by a durable `SegmentRemoved` frame is logically
absent even if its directory entry remains; recovery completes deletion and
syncs `segments/`. Conversely, absence of a checkpoint- or manifest-live
segment is corruption.

## Capacity accounting implications

The capacity model charges exact current lengths of canonical and recognized
transactional format files. It therefore includes `ROOT`, both manifest files,
all segment bytes, and temporary files while they coexist with canonical
predecessors. It does not charge nonexistent padding between a short footer and
the configured segment ceiling.

The worst next-checkpoint reserve is derived from the fixed layouts above and
bitmap release state for every current physical record. The largest next
manifest-group reserve is the maximum of:

- `48 + 24 + 16 * max_release_records` for a maximally fragmented Release;
- `80` bytes for SegmentSealed; and
- four times `64 + 16 * stream_count_in_largest_removable_segment` for the
  bounded SegmentRemoved maintenance batch.

The active segment contributes a separate 48-byte footer reserve. These
derived bytes are part of bounded root capacity but are not stored in the
format.

## Compatibility and evolution

Format v1 has no extension points that an old reader silently ignores:

- every magic and structure size is exact;
- no trailing bytes or padding are accepted;
- unknown manifest kinds fail closed;
- noncanonical ranges, bitmaps, counts, and ordering fail closed;
- checksum algorithms, seeds, and input ranges are fixed; and
- field semantics and numeric kind/lifecycle/encoding values are fixed.

Changing a magic, layout, checksum algorithm or range, record codec, manifest
kind, checkpoint field, compression or encryption behavior, or semantic field
interpretation requires a new explicit format version and upgrade design.
Readers MUST reject unsupported authoritative versions and MUST NOT guess or
reinterpret bytes in place.

Opening a root never silently migrates its format. Crate semantic versions do
not implicitly change the on-disk version.

## Conformance checklist

A format-v1 implementation is not conforming unless it:

- uses checked arithmetic for every length, count, sequence, and offset;
- validates a fixed checksum before trusting its variable length;
- preserves opaque metadata and payload bytes exactly;
- publishes records only from complete valid epochs;
- assigns contiguous stream-local sequences without reuse;
- verifies opaque bodies before returning them;
- applies exact release state and canonical range/bitmap codecs;
- validates root identity, manifest sequence fencing, and every lifecycle
  transition;
- never repairs a complete corrupt structure, authoritative header,
  checkpoint, sealed segment, or damage before a valid suffix;
- follows every file and directory sync ordering above; and
- fails closed rather than inventing state for an ambiguous artifact.
