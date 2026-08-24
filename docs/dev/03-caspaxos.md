# 3. CASPaxos

## Context

CASPaxos ([Rystsov 2018](https://arxiv.org/abs/1802.07000)) is single-decree Paxos used as a
**replicated register with a transform**. Classic Paxos
agrees once on one value; CASPaxos runs that agreement repeatedly over the same register, and each
round applies a caller-supplied function to whatever the quorum agrees is current. There is no
leader, no log, and no replication stream.

That buys two things a log-based store cannot: any member can coordinate any write (so there is no
election and no leader failover), and a write costs two round trips with no ordering dependency on
other keys. It costs the two things a log gives you: **no global revision** -- so no consistent
cross-key snapshot and no ordering between keys -- and no log to replay onto a lagging replica, which
is why [anti-entropy](06-anti-entropy.md) exists.

Almost every limitation in [11. Limitations](11-limitations.md) traces back to "no log". It is worth
holding on to while reading the rest of this guide.

## The algorithm

One register per key. A **ballot** is `(counter, NodeId)` -- `Ballot` -- totally ordered, counter
first, node id breaking ties, so no two proposers can issue the same ballot. A round is:

1. **Prepare.** The coordinator picks a ballot above anything it has seen and sends `PrepareReq` to
   every member. An acceptor **promises** not to accept below that ballot and answers with what it
   currently holds -- its accepted ballot and value.
2. **Transform.** With promises from a quorum (`N/2 + 1`), the coordinator takes the value at the
   highest accepted ballot among them and applies the caller's function to it.
3. **Accept.** It sends `AcceptReq` with the result at its ballot. Once a quorum has accepted, the
   value is chosen and the caller is answered.

A read is the same round with the identity function -- that is what makes it linearizable, and what
makes it as expensive as a write. `ReadConsistency.SERIALIZABLE` skips the round and answers from
local committed state instead.

## What is specific to this implementation

### One round per operation, classified when it fails

`Proposer` drives one round per client operation, with bounded pending rounds (`MAX_PENDING`,
10 000) so an overloaded node applies back-pressure instead of growing without limit. When a round
fails it carries a `RoundFailure` saying *which phase* died, and that distinction is the whole
of the client's error taxonomy:

| `RoundFailure` | Meaning | Outcome |
|---|---|---|
| `BALLOT_NACK` | lost a ballot duel before Accept | **determinate** -- nothing was accepted |
| `INSUFFICIENT_RESPONDERS` | prepare never reached a quorum | determinate, and a property of *this* coordinator |
| `ACCEPT_TIMEOUT` | got as far as Accept and did not complete | **unknown** -- some acceptor may hold it |
| `PROPOSAL_EXPIRED` | outlived `proposalExpiry` before anything was proposed | determinate |

Collapsing these would make "did not happen" indistinguishable from "may yet happen". See
[7. The client](07-client.md) for how each maps to a `ClientErrorCode` and which ones fail over.

### Version-fenced CAS, and no value-compared CAS

A coordinator the client has given up on **keeps driving its proposal**. Against a value-compared
CAS that is fatal: when the register returns to the expected value, the abandoned attempt's compare
succeeds a second time and silently reverts whoever wrote in between. No caller can detect it
afterwards -- "not applied" and "applied, then reverted" are the same observation.

So the operation does not exist. The CAS here fences on the key's **version** -- its accepted
`Ballot` -- and an intervening write has already overtaken that ballot, so a late duplicate is
rejected rather than applied. That is what makes a fenced write safe to re-send when the outcome is
unknown, and it is why the client walks coordinators on a fenced write but not on an unfenced one.

### Promise durability: the ceiling and the floor

A promise is acknowledged to the proposer **before** the WAL is forced, so an unclean shutdown can
lose promises the node already made. A forgotten promise lets the acceptor accept a ballot it had
promised against, and two different values can be chosen -- a **safety** failure, not a durability
one, because a promise is never replicated and so no quorum can repair it.

The fix is to bound what could have been promised rather than to persist each promise:

- An acceptor reserves the right to promise up to a **ceiling** with a *forced* write
  (`Wal.Entry.PromiseCeiling`), in chunks of `CEILING_CHUNK` (1024) ballots, so the sync is paid once
  per chunk rather than once per promise.
- On recovery, replay derives a **floor** from the ceiling it finds: every promise that ever existed
  had a counter at or below it, so refusing at or below the floor covers every promise lost with an
  unforced tail. `Acceptor` therefore checks two floors -- the key's own `promised` ballot, and the
  recovery floor that applies to *every* key.
- The proposer reserves its own ballot range the same way (`Wal.Entry.BallotBump`,
  `BALLOT_RESERVATION_CHUNK`), so it can never reissue a counter it already handed out.

A node that starts with **no durable state at all** has no ceiling to derive a floor from. It asks
the cluster: `CeilingRecovery` collects `CeilingResp` until it can conclude either *the bound
survived somewhere* (`N - quorum + 1` witnesses agree an upper bound) or *it survived nowhere*
(every member holds nothing, so no earlier round can ever complete and the floor is zero).
`CeilingEvidence` grades what each answer is worth. Until then the node is `AWAITING_FLOOR`: running,
reachable, answering that one question, and serving nothing.

`incarnation_id` (`IncarnationMarker`, a file inside the data directory) is what separates "restarted"
from "wiped": a member returning with the same node id and a fresh incarnation has had its storage
replaced, which is safe -- it will recover a floor. The same incarnation reporting a *lower* ceiling
than it already proved is storage that came back holding less than it left with, which is refused
(`CEILING_ROLLED_BACK`).

### Anti-entropy instead of replication

With no log there is nothing to stream to a replica that missed an accept. A round commits at a
quorum, so a minority may not have the value at all, and nothing in the write path goes back for it.
[Anti-entropy](06-anti-entropy.md) is the process that converges them: range digests, then key
digests, then an ordinary Paxos round to repair whatever diverged.

### Tombstones

A delete cannot simply remove the key: anti-entropy would see a replica that still holds the value,
call it divergence, and copy it back. So a delete writes a **tombstone**, and the tombstone is what
out-votes the stale value. That makes deletes non-reclaiming by default, which is why tombstone
collection exists and why its rule is *every member*, not a quorum -- see
[6. Anti-entropy and tombstones](06-anti-entropy.md).

## Source map

| Concern | File |
|---|---|
| Ballot | `discas-common/src/main/java/io/github/green4j/discas/common/Ballot.java` |
| Round driving, retries, back-pressure | `discas-node/src/main/java/io/github/green4j/discas/node/Proposer.java` |
| Promise/accept, the two floors, the ceiling | `discas-node/src/main/java/io/github/green4j/discas/node/Acceptor.java` |
| Failure classification | `discas-node/src/main/java/io/github/green4j/discas/node/RoundFailure.java` |
| Floor recovery for a stateless start | `discas-node/src/main/java/io/github/green4j/discas/node/CeilingRecovery.java`, `CeilingEvidence.java` |
| Storage identity | `discas-node/src/main/java/io/github/green4j/discas/node/wal/IncarnationMarker.java` |
| Peer wire messages | `discas-node/src/main/java/io/github/green4j/discas/node/PeerMessage.java` |

## Tests

| Question | Suite |
|---|---|
| Does a round classify its own failure correctly? | `discas-int-test/src/test/java/io/github/green4j/discas/node/RoundFailureTest.java` |
| Does a lost unforced tail stay safe? | `discas-int-test/src/test/java/io/github/green4j/discas/node/PromiseDurabilityTest.java` |
| Does a stateless start reach a correct floor, in every combination? | `discas-int-test/src/test/java/io/github/green4j/discas/node/CeilingRecoveryMatrixTest.java` |
| Is the abandoned-round ABA hazard actually closed? | `discas-int-test/src/test/java/io/github/green4j/discas/AbandonedRoundAbaTest.java` |
| Does a cluster under a killing nemesis admit only linearizable histories? | `discas-int-test/src/test/java/io/github/green4j/discas/crash/ClusterKillNineChaosTest.java` |

## How to change it safely

- **Anything touching ballots or promises is a safety change, not a performance one.** The property
  at stake -- a node never accepts below what it may have promised -- cannot be repaired by a quorum
  after the fact, so it has no second line of defence.
- **A new failure mode needs a `RoundFailure` constant, not a reuse of an existing one.** The
  taxonomy is exhaustive by design and the chaos suites switch over it without a `default`, so a new
  code has to be classified rather than inheriting somebody else's retry semantics.
- **Never widen what the acceptor will accept** to make a test pass. The floors exist because the
  fault they cover is invisible locally: replay of a short log reports success.
