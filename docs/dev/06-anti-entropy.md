# 6. Anti-entropy and tombstones

## Context

A round commits at a quorum, so a minority may not hold the value at all -- and with
[no log](03-caspaxos.md) there is nothing to stream at them afterwards. Anti-entropy is what
converges them: peers compare what they hold, and anything that differs is repaired by an **ordinary
Paxos round**, not by copying bytes.

That last point is the safety argument. Detection is a best-effort heuristic -- it may miss
divergence, compare stale digests, or run against an unreachable peer -- and none of that can produce
a wrong result, because the repair is a real round subject to the same ballots as any write. A
missed divergence is caught by the next cycle.

## The comparison, in three stages

Comparing every key with every peer every cycle would be unaffordable, so the comparison narrows:

1. **Range digests.** The key space is split into `NUM_RANGES` (256) ranges by `KeyHash`. Each node
   keeps a cached SHA-256 per range over its sorted key digests, recomputed only for ranges marked
   dirty. `DigestReq`/`DigestResp` exchanges all 256 in one message; ranges that match are done.
2. **Key digests, paged.** For a range that differs, `KeysReq`/`KeysResp` walks it one bounded page
   at a time (`MAX_ANTI_ENTROPY_KEYS_PER_PAGE` = 1024, and a byte budget besides). Paging is not
   tidiness: a dense range in one message would exceed the peer transport's frame and inflight
   budgets and get the connection torn down by its own backpressure.
3. **Repair.** Each key that differs gets a Paxos round, which re-decides it at a fresh ballot.

A cycle runs on a timer (`repairInterval`) and reschedules **after** the previous one completes, so
a slow cycle never overlaps itself. Progress is tracked by one set of in-flight ranges -- completion
is derived from it emptying, rather than from a parallel counter that could disagree.

Sending goes to every peer, but the cycle proceeds on whoever answered: requiring all of them would
block all repair whenever any peer is slow.

## What counts as divergence

**A tombstone and an absent key are the same state: no value.** So one side holding a tombstone the
other has already collected is *agreement*, and this is what makes collection stick -- otherwise a
member that purged would be handed the tombstone straight back by one that had not, undoing every
collection as fast as it was decided.

Nothing is lost by not pushing a tombstone to a replica holding nothing for the key: there is
nothing there to suppress. The replica that has *not* seen the delete is the one still holding the
**value**, and that is divergence by the same rule -- which is the whole job tombstones exist for.

## Tombstones

A delete writes a tombstone rather than removing the key, because the tombstone is what out-votes
the stale value on a replica that has not seen the delete. Left alone that grows the key space
forever, at roughly 280 bytes a key.

**The collection rule is one sentence: a tombstone may be collected only when no replica can still
resurrect the value it suppresses.** A quorum is the wrong answer here, and this is the one place in
the store where it is -- the danger is precisely the replica *outside* the quorum, which still holds
the old value. So the condition is **every member**, and a member that does not answer blocks the
collection exactly like one that refuses.

Three parts:

- **`TombstoneSweeper`** picks one candidate per `tombstoneSweepInterval` -- the oldest tombstone this
  node *owns* by `SweepAffinity` (members sorted by `NodeId`, key hashed to one of them), falling
  back to its oldest if it owns none. Ownership makes the ceilings add up rather than overlap: the
  cluster collects `N` keys per interval instead of one. The fallback is not politeness -- it is what
  keeps an unacknowledged purge self-healing.
- **`TombstoneCollector`** asks every member `PurgeCheckReq` and decides on the answers:
  `PurgeAnswer.HELD` (holds the tombstone durably -- a claim about the disk, so the log is forced
  before it is made), `ABSENT` (holds nothing, so nothing to resurrect), `RETAINED` (holds something
  the decision was not about -- blocks).
- **`LocalStore.purge`** applies it: the key leaves the map, its indexes and its range digest.

`ABSENT` and `RETAINED` are kept apart on purpose: a member whose storage was replaced answers
`ABSENT`, and calling that a refusal would block every collection in the cluster for as long as that
member lives.

Nothing about a sweep is durable and no decision is acknowledged or retried. A half-applied purge
re-decides by itself -- the members that applied it answer `ABSENT`, those that missed it answer
`HELD`, and both permit.

There is **no minimum age**. What makes collection safe is that every member permits it, not how
long anyone waited; a clock would not survive a restart, after which every tombstone would wait
again while the operator surface called a stalled cluster healthy.

## Source map

| Concern | File |
|---|---|
| Cycles, digests, paging, repair | `discas-node/src/main/java/io/github/green4j/discas/node/AntiEntropy.java` |
| Range assignment | `discas-common/src/main/java/io/github/green4j/discas/common/KeyHash.java` |
| Digests and range indexes | `discas-node/src/main/java/io/github/green4j/discas/node/LocalStore.java`, `KeyDigest.java` |
| Which node sweeps which key | `discas-node/src/main/java/io/github/green4j/discas/node/SweepAffinity.java` |
| Candidate selection and pacing | `discas-node/src/main/java/io/github/green4j/discas/node/TombstoneSweeper.java` |
| The all-members decision | `discas-node/src/main/java/io/github/green4j/discas/node/TombstoneCollector.java` |
| Answers | `discas-node/src/main/java/io/github/green4j/discas/node/PurgeAnswer.java` |
| What one sweep reports | `discas-node/src/main/java/io/github/green4j/discas/node/TombstoneSweep.java` |

## Tests

| Question | Suite |
|---|---|
| Do divergent replicas converge, and does a cycle always finish? | `discas-int-test/src/test/java/io/github/green4j/discas/node/AntiEntropyTest.java` |
| Is a range compared page by page? | `discas-int-test/src/test/java/io/github/green4j/discas/node/AntiEntropyPagingTest.java` |
| What is the decision, given each combination of answers? | `discas-int-test/src/test/java/io/github/green4j/discas/node/PurgeDecisionTest.java` |
| What does one member answer about its own storage? | `discas-int-test/src/test/java/io/github/green4j/discas/node/PurgeCheckTest.java` |
| Collection across every way a member can leave and return | `discas-int-test/src/test/java/io/github/green4j/discas/node/TombstoneCollectionMatrixTest.java` |
| Is an operator told enough to act? | `discas-int-test/src/test/java/io/github/green4j/discas/node/TombstoneCollectionReportingTest.java` |

## How to change it safely

- **Repair stays a Paxos round.** The moment detection copies bytes instead, every heuristic in
  stages 1-2 becomes a correctness risk rather than a performance choice.
- **Do not weaken the all-members rule to a quorum.** The replica that makes collection unsafe is by
  definition the one outside the quorum.
- **A tombstone and an absent key must keep comparing equal.** Treating them as divergence turns
  collection into a loop where members hand tombstones back to each other.
- **Any new peer exchange is paged and bounded.** An unbounded response is a torn connection at
  scale, not a slow one.
