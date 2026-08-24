# 7. The client

## Context

`DisCasClient` is the library callers use: KV, prefix scan, watch and distributed locks. It has no
leader to find and no session to keep -- a request goes to one member, which coordinates the round on
the caller's behalf, so any reachable member is as good as any other.

Everything returns a `CompletableFuture` and nothing blocks. All I/O and all completion bookkeeping
happen on a single `EventLoop`, either the client's own or one shared with a co-located node
(`ownsLoop = false`) -- which is how an embedded deployment avoids a second thread entirely.
Completions run on that loop, so a dependent stage attached with a non-async `then*` must not block.

## Routing and failover

A key is hashed (`KeyHash`, the same function the cluster partitions by) to pick a starting
coordinator; retries walk on from there. Per peer the client keeps a consecutive-failure count and
an instant it becomes eligible again -- a backoff that is chiefly a *memory between calls*, so a
caller retrying in a tight loop cannot turn into a connect storm.

Two deadlines bound an operation: `perAttemptTimeout` (5 s) per coordinator, and `requestDeadline`
(30 s) for the whole thing, not reset by retries.

**Whether a failure fails over is the interesting part**, and it is decided by what the failure says
about *this coordinator*:

| Code | Fails over? | Why |
|---|---|---|
| `NOT_READY` | yes | that node is still replaying; costs nothing to ask elsewhere |
| `NO_QUORUM_AT_COORDINATOR` | yes | *that node* cannot see a majority; another may |
| `UNAVAILABLE` | no (but see below) | says nothing about connectivity, and each attempt costs a whole round timeout |
| `BALLOT_LOST` | no | contention is a property of the key; moving adds a competitor |
| `PROPOSAL_EXPIRED` | no | the operation was slow, which is not evidence about this node |
| `ACCESS_DENIED`, `INVALID_ARGUMENT` | no | properties of the request; identical everywhere |
| `STORE_FULL` | no | every replica holds the same keys, so a full node is a full cluster |

**The exception is a version-fenced write**, which keeps walking coordinators even on `UNAVAILABLE`:
a duplicate provably cannot apply, so re-sending is free. That is the whole payoff of the fence.

A dropped connection is not waited out -- the transport reports it and every request riding on it
moves at once, rather than each discovering the loss when its own timer expires.

## Write determinism

The one question a writer must be able to answer is **did it happen?** Every `ClientErrorCode` is
determinate -- it did not happen -- except **`UNAVAILABLE`**, which means the round reached Accept and
may still be completed by a later proposer. That is the code a caller cannot resolve by re-reading.

`proposalExpiry` is what bounds it: past that horizon the coordinator stops driving, so an
indeterminate answer has an end rather than lasting forever, and `PROPOSAL_EXPIRED` says the deadline
was reached with the write firmly on the "did not happen" side.

For a fenced write none of this needs handling -- re-send until a definite answer or the deadline.
For an unfenced `put`/`delete` there is no safe automatic recovery, and the caller is told so rather
than handed a silent retry. The pattern for recovering one is an author marker: write something that
identifies you, then read it back. `CoordinatorFailoverExample` is that pattern, executable.

`update` is the read-transform-write loop the fence otherwise leaves to the caller, and the same
question decides its retry rule: **it retries on a lost compare and on nothing else.** A lost
compare is a fact the cluster stated, so re-reading and re-applying is correct. Any failure --
`UNAVAILABLE` above all -- is the absence of a fact, and re-running the transform would apply it
twice. That is what `update` cannot promise and a `cas` can: a duplicate `cas` carries an overtaken
ballot and is provably a no-op, whereas an `update` is two round trips with a caller's function in
between. Running out of retry budget raises `UpdateContendedException`, deliberately not a timeout
-- "the key is hot" and "the cluster did not answer" call for opposite reactions, so they must not
arrive as the same type.

## Scan

`scan` is the one operation aggregated by the **client**: each node answers from local state with no
consensus round, and the client merges. It completes once `N/2 + 1` nodes have answered and fails
below that (`ScanCoverage.QUORUM`, the default); `ANY_AVAILABLE` returns whatever came back.

**Guaranteed:** every key whose commit completed before the scan started appears -- quorum
intersection. **Not guaranteed:** cross-key atomicity (no global revision -- different keys may
reflect different instants, and a consistent snapshot is *undefinable* here, not merely
unimplemented), and committed values -- a page carries the key and its version only, because a node
reports what it has *accepted*, which a later ballot may still supersede.

Paging merges safely by trusting the merged result only up to the smallest last-key among nodes
that reported more available; beyond that a node may hold keys it did not send.

## Watch

A coalescing blocking query, not an event stream: the caller passes the `Version` it last saw and
gets the current state once the key moves past it, or the unchanged state when the wait elapses.
Intermediate values may be skipped -- a CASPaxos register keeps only its current value, so there is
no history to replay.

A benign spurious wake exists: a linearizable read that repairs a lagging quorum re-accepts the
current value at a new ballot, so the version advances although nothing was written.

## Locks

A lock is **a value in one key**, written with the same version-fenced CAS as anything else -- not a
protocol feature. It therefore inherits that write's failure story rather than having one of its own.

Two things are worth internalising:

- **The lease is not the safety mechanism; the fencing token is.** A holder paused past its lease by
  a GC pause or a VM freeze has a perfectly correct clock and can be displaced while its `Lock`
  object still looks valid. Guard the protected resource with `fencingToken()` -- a strictly
  increasing generation -- not with the fact that you hold the object.
- **Lease deadlines cross processes**, so they are expressed in *corrected* time. `ClusterClock`
  derives an offset from the coordinator's clock reported in the hello response, so two clients
  measure against the same reference; each also detects a step in its own clock by comparing wall
  and monotonic deltas, and drops a stale offset rather than applying it.

Every remote lock operation is a CAS conditioned on the holder's `LockToken`, so a displaced
holder's release or renew fails instead of stomping its successor. Both answer with a
`LockWriteResult`, not a boolean: `HELD_BY_OTHER`, `EXPIRED`, `NOT_HELD`, `NOT_LOCK_RECORD` and
`CONTENDED` call for different reactions, and the refusal carries the record it saw so a displaced
holder learns who displaced it from the same round trip. A lapsed lease is renewable by nobody --
from the moment it ran out a waiter was entitled to take over -- but is still releasable by its own
token, because writing the marker is the right cleanup either way.

## Source map

| Concern | File |
|---|---|
| The whole client surface | `discas-client/src/main/java/io/github/green4j/discas/client/DisCasClient.java` |
| Error codes and their meaning | `discas-common/src/main/java/io/github/green4j/discas/common/client/ClientErrorCode.java` |
| Version | `discas-client/src/main/java/io/github/green4j/discas/client/Version.java` |
| Timings | `discas-client/src/main/java/io/github/green4j/discas/client/DisCasClientConfig.java` |
| Scan policy | `discas-client/src/main/java/io/github/green4j/discas/client/ScanCoverage.java` |
| Corrected time | `discas-client/src/main/java/io/github/green4j/discas/client/ClusterClock.java` |
| Locks | `discas-client/src/main/java/io/github/green4j/discas/client/lock/DistributedLock.java`, `LockValueCodec.java` |
| Result types | `discas-client/src/main/java/io/github/green4j/discas/client/` -- `GetResult`, `CasResult`, `ScanPage`, `ScanResult` |
| Observability seam | `discas-client/src/main/java/io/github/green4j/discas/client/ClientObserver.java` |

## Tests

| Question | Suite |
|---|---|
| Which failures fail over, and which must not | `discas-int-test/src/test/java/io/github/green4j/discas/client/UnavailableFailoverTest.java` |
| Backoff and the walk over coordinators | `discas-int-test/src/test/java/io/github/green4j/discas/client/PeerBackoffTest.java` |
| A dropped connection does not cost a timeout | `discas-int-test/src/test/java/io/github/green4j/discas/client/ConnectionLostFailoverTest.java` |
| The ABA interleaving the fence exists for | `discas-int-test/src/test/java/io/github/green4j/discas/AbandonedRoundAbaTest.java` |
| Scan coverage and merge safety | `discas-int-test/src/test/java/io/github/green4j/discas/client/ScanCoverageTest.java` |
| Locks end to end, including a stolen lease | `discas-int-test/src/test/java/io/github/green4j/discas/client/DisCasClientLockTest.java` |
| That `update` retries a lost compare and nothing else | `discas-int-test/src/test/java/io/github/green4j/discas/client/UpdateTest.java` |
| Two clients whose clocks disagree | `discas-int-test/src/test/java/io/github/green4j/discas/client/LockClockSkewIntegrationTest.java` |

## How to change it safely

- **A new `ClientErrorCode` needs a failover decision, explicitly.** The question is only ever
  "does this say something about *this* coordinator, and is the outcome known" -- get that wrong and
  a caller either loses availability or replays a write it should not.
- **Never fail over an unfenced write on `UNAVAILABLE`.** That is the duplicate-apply hazard, and it
  is silent.
- **Do not add a value-compared CAS.** See [3. CASPaxos](03-caspaxos.md#version-fenced-cas-and-no-value-compared-cas).
- **Do not let `scan` return values.** The guarantee it can honestly make is about key existence;
  values would imply a snapshot that does not exist.
- **Do not make `update` retry anything but a lost compare.** A timeout looks retryable and is not:
  the transform would run twice against one unknown outcome. If that ever seems worth relaxing, the
  operation to reach for is `cas`, which is safe to re-send by construction.
- **One name per meaning, one result type per answer.** Different names mean different semantics,
  the `*Result` suffix means "the answer to this call", and short forms that drop part of the
  answer are not kept -- that is why `get` always returns the version and `scan` always returns a
  page.
