# 5. Storage

## Context

Two layers. `LocalStore` is the node's **in-memory projection**: every key's promised and accepted
ballots, the value or tombstone behind them, and the indexes anti-entropy and scans read. `Wal` is
the **durable record** it is rebuilt from -- a write-ahead log of segment files plus periodic
snapshots.

The store is the source of truth at runtime; the log is the source of truth across a restart. Every
write path therefore appends to the log **first** and mutates memory only if the append was
accepted. Memory holding a write the log will never replay is the one thing this layer must never
produce -- a degraded node still answers serializable reads, scans and anti-entropy digests from
memory, so such a phantom would be observable.

## In-memory state

`LocalStore` keeps one `KeyState` per key: `promised`, `accepted`, the value (`HashedBytes`, bytes
plus a cached hash), and whether it is a tombstone. Around that map sit three secondary indexes,
maintained on key creation and eviction so the hot path pays nothing:

| Index | For |
|---|---|
| `keysByRange` -- range -> keys, sorted | anti-entropy digests and paging |
| `sortedKeys` -- all keys, sorted | bounded, resumable `scanLocal` |
| `tombstones` -- insertion-ordered | making "nothing to collect" one lookup instead of a walk |

`states` itself is a `HashMap` rather than sorted: it is probed on every promise and accept, where a
cached-hash O(1) probe beats a tree's byte-by-byte comparisons.

**Capacity** is an estimate, not a measurement: each entry is charged its own bytes plus
`ENTRY_FOOTPRINT_BYTES` (512, rounded up on purpose) for the objects around it, against
`storeHeapFraction` of max heap. Deliberately not the JVM's live heap -- that moves with the
collector, so the same write would be refused before a GC and taken after one, and two replicas
would disagree about one pair. A delete is never refused for want of room, or a full store would
have no way back.

## Durability

```
data-dir/
├── incarnation            which run of this storage this is
├── *.wal                  segments, rolled at walMaxFileBytes (64 MiB)
└── *.snapshot             snapshots, snapshotRetentionCount (2) kept
```

Five record kinds (`Wal.Entry`): `Promise`, `Accept`, `Purge`, `BallotBump` (proposer's reserved
ballot range) and `PromiseCeiling` (acceptor's forced promise ceiling -- see
[3. CASPaxos](03-caspaxos.md)).

Writes are buffered and forced periodically (`walForceInterval`); between syncs, durability rests on
the quorum. `append` returns whether the entry was **accepted into the log**, not whether it reached
disk -- a caller that keeps an in-memory projection must not apply the entry when it returns `false`.

An unrecoverable I/O failure puts the log into a one-way **degraded** state: appends refuse, the
acceptor NACKs, `/health` answers 503, and nothing but a restart clears it. Refusing to be a member
is the deliberate trade against becoming an unreliable one.

### Snapshots are fuzzy

A snapshot is written in batches that yield to the loop between them, so it is **not** a
point-in-time view: different keys may reflect different logical times. That is safe because the
snapshot records the WAL position it started at, every mutation after that position stays in the
tail, and recovery loads the snapshot and then replays the tail, where a later ballot overrides
stale snapshot state. The tail is truncated only after the snapshot commits.

The alternative -- copying all state up front -- would stall the loop for as long as there are keys.

## Recovery

`beginRecovery()` returns a `ThrottledLoader` that the node drives in batches, so a large log does
not monopolize the loop:

1. **Snapshot**, if present, entry by entry.
2. **WAL tail** from the snapshot's position, applying ballot comparison so a later accept wins.
3. Derive the **recovery promise floor** from the replayed ceiling, then hand control to
   [floor recovery](03-caspaxos.md#promise-durability-the-ceiling-and-the-floor) if there was no
   state at all.

Absence of state is **reported, not refused** -- an empty directory is either a new node or one whose
state was deleted, nothing on disk tells them apart, and a node that starts empty recovers a floor
from the cluster before serving anything. What *does* fail here is state that is present and
contradictory: an unreadable marker, or a WAL that will not replay. Replay running out of heap also
stops the node (`StoreCapacityExceededException`) rather than being killed by the JVM halfway.

A node that adopted its floor from the cluster has a hole in its history. Keys at or below that
floor that **no peer holds** are dropped (`dropUnaccountedFor`): if such a value had ever been
chosen, a quorum accepted it and some peer would still have it. Without this the node *pushes* what
it kept -- anti-entropy calls it divergence and repairs it outward.

## Source map

| Concern | File |
|---|---|
| In-memory state, indexes, capacity | `discas-node/src/main/java/io/github/green4j/discas/node/LocalStore.java` |
| Per-key state | `discas-node/src/main/java/io/github/green4j/discas/node/KeyState.java` |
| Bytes plus cached hash | `discas-node/src/main/java/io/github/green4j/discas/node/HashedBytes.java` |
| Log contract and record kinds | `discas-node/src/main/java/io/github/green4j/discas/node/wal/Wal.java` |
| File implementation | `discas-node/src/main/java/io/github/green4j/discas/node/wal/FileWal.java` |
| Segment writing and rolling | `discas-node/src/main/java/io/github/green4j/discas/node/wal/WalWriter.java`, `CompactionManager.java` |
| Snapshots | `discas-node/src/main/java/io/github/green4j/discas/node/wal/FileSnapshotWriter.java`, `FileSnapshotReader.java` |
| On-disk framing and versions | `discas-node/src/main/java/io/github/green4j/discas/node/wal/StorageFormat.java`, `WalRecordCodec.java` |
| Storage identity | `discas-node/src/main/java/io/github/green4j/discas/node/wal/IncarnationMarker.java` |
| Tunables | `discas-node/src/main/java/io/github/green4j/discas/node/wal/StorageConfig.java` |

## Tests

| Question | Suite |
|---|---|
| Does a rejected append ever reach memory? | `discas-int-test/src/test/java/io/github/green4j/discas/node/LocalStoreWalRejectionTest.java` |
| Every shape a data directory can arrive in | `discas-node/src/test/java/io/github/green4j/discas/node/wal/StorageRecoveryMatrixTest.java` |
| Snapshot + tail replay, compaction | `discas-node/src/test/java/io/github/green4j/discas/node/wal/FileWalIntegrationTest.java` |
| A snapshot written while writes continue | `discas-int-test/src/test/java/io/github/green4j/discas/node/FuzzySnapshotConcurrencyTest.java` |
| A real `SIGKILL`, then open what it left | `discas-int-test/src/test/java/io/github/green4j/discas/crash/KillNineCrashTest.java` |
| Capacity accounting and replay refusal | `discas-int-test/src/test/java/io/github/green4j/discas/node/StoreCapacityTest.java` |

## How to change it safely

- **Append, check, then mutate.** Every write path follows it; a shortcut produces state no replay
  can reproduce, and the store keeps serving it.
- **Changing the on-disk framing is a compatibility change.** `StorageFormat.FORMAT_VERSION` and
  `LAYOUT_VERSION` exist so an incompatible file is refused rather than misread -- bump them, and see
  [11. Limitations](11-limitations.md).
- **Never remove a key outside `purge`.** Anti-entropy will copy it back from a replica that still
  has it; the tombstone is what makes a deletion converge.
- **A new secondary index is maintained at key creation and eviction, not on the write path.** That
  is the property that keeps promise/accept O(1).
