# Camus glossary

This glossary keeps storage terms consistent across the public API,
architecture, format specification, and operations guide.

## Storage root

One locked directory containing immutable root identity, the manifest pair,
and a root-wide physical segment sequence. Capacity and lifecycle are scoped to
the complete root.

## Logical stream

A caller-selected `StreamId(u64)` namespace for sequence identity, ordering,
pending observation, and exact release. It is not a directory, shard, I/O lane,
consumer, subscription, or quota boundary.

## Stream handle

A lightweight cloneable client that binds a logical `StreamId` to one root
reactor. It owns no consumer identity, claim, cursor, or independent pending
copy.

## Durability epoch

One non-empty `append` or `append_batch` request. Its record frames and commit
marker recover atomically and are covered by one data durability barrier,
possibly shared with other consecutive epochs through group commit.

## Record ID

A stable opaque 32-byte storage identity containing root identity, logical
stream identity, and Camus-assigned stream-local sequence. It is used for exact
release, not physical addressing, consumer position, or application
idempotency.

## Pending record

A complete recoverable record without a durable release. Pending state is
shared by every handle for that stream.

## Waiting read

`Stream::read(ReadLimits)`: both the readiness wait and the bounded verified
data read. It returns a non-empty owned snapshot and does not claim records.

## Release

A durable exact declaration that specified records no longer need to remain in
the shared pending set. It is not an acknowledgement tied to a consumer or a
prefix cursor.

## At-least-once storage handoff

Append success keeps a record recoverable until release is durable. A process
failure between a downstream effect and release may expose it again. Camus
guarantees storage retention, not consumer execution or exactly-once effects.

## Data log

The root-wide sequence of immutable physical segment files containing record
epochs. Logical streams may interleave in one segment.

## Manifest

The checkpoint plus append-only control log that durably records exact release,
segment seal, and segment removal state. It contains no application schema or
consumer policy.

## Seal

The two-stage transition that writes and syncs a segment footer, then publishes
and syncs `SegmentSealed` in the manifest.

## Reclamation

Removal of a fully released sealed physical segment after a durable
`SegmentRemoved` frame. Reclamation is automatic; explicit `Log::reclaim` is an
optional maintenance barrier.

## Maintenance headroom

Dynamic bytes reserved inside bounded root capacity for the next checkpoint
rewrite, largest manifest frame, and active footer. Applications cannot spend
this space on new data.

## Admission

The point after which dropping an operation Future abandons only its result.
Before admission, cancellation is side-effect free; after admission the reactor
finishes the finite storage operation.

## Poisoned root

An open lifecycle that failed closed after uncertain I/O, corruption, lazy body
verification failure, or runtime progress failure. It rejects storage work
until all handles close and the root is reopened through recovery.
