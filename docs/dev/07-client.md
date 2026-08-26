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

**Silence is decided by the same rule, with less to go on.** A coordinator that answers names the
phase it failed in, which is what lets even an unfenced write move on `NOT_READY`. A coordinator
that goes quiet past `perAttemptTimeout`, or whose connection drops, names nothing -- it may be
driving a round right now -- so only a fenced write and a read move on. An unfenced write that had
already reached the wire ends there with `RequestFailedException.Cause.INDETERMINATE`, immediately
rather than at the deadline: more waiting cannot turn an unknown outcome into a known one. A
request the client never managed to send is safe whatever it is, and still moves.

A dropped connection is not waited out -- the transport reports it and every request riding on it
is decided at once, rather than each discovering the loss when its own timer expires.

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
- **The owner id is a correctness argument, not a label**, which is why the acquire methods require
  one and no overload invents it. An acquire is a write whose outcome can be unknown, and the owner
  id is the only field of the record the caller chooses *before* the write -- the token is minted
  inside the call and the generation is not knowable in advance -- so it is the only thing a later
  read can match the caller against. Its precondition is uniqueness among concurrent holders.

`recoverLock(key, ownerId)` is what that buys: it reads the key and, if the live record is the
caller's, rebuilds a `DistributedLock` from it with the record's own token and generation, so the
recovered lock releases and renews as the original would. The remaining lease is anchored on a
monotonic reading taken before the read, which understates it -- the same direction a fresh acquire
rounds towards.

The split between `HELD_BY_SELF` and `recoverLock` is deliberate and is the one place this design
chose safety over convenience. `tryLock` could hand back the rebuilt lock directly, but then two
components that merely shared an owner id would each be given the same lease and mutual exclusion
would be lost silently -- and the ids people pick (`"worker-3"`) make that a plausible mistake. So
the acquire path reports the fact and hands back nothing, and the recovery path, which a caller only
reaches by naming the operation, is where the id is trusted. `lockAttempt` also terminates on
`HELD_BY_SELF` rather than polling: no later attempt can change it, and the caller is the one who
would be renewing the lease in the way.

Every remote lock operation is a CAS conditioned on the holder's `LockToken`, so a displaced
holder's release or renew fails instead of stomping its successor. Both answer with a
`LockWriteResult`, not a boolean: `HELD_BY_OTHER`, `EXPIRED`, `NOT_HELD`, `ALREADY_RELEASED`,
`NOT_LOCK_RECORD` and
`CONTENDED` call for different reactions, and the refusal carries the record it saw so a displaced
holder learns who displaced it from the same round trip. A lapsed lease is renewable by nobody --
from the moment it ran out a waiter was entitled to take over -- but is still releasable by its own
token, because writing the marker is the right cleanup either way.

The release marker carries the tenancy it ended -- owner, token, acquire time, generation -- with
only the lease zeroed, and `isReleased()` is that zero and nothing else. It used to blank the owner
and the token, which cost the same thing an anonymous acquire cost: a retried release could only be
told "no lock here", the answer a key that was never locked gives. Keeping the token buys
`ALREADY_RELEASED`, so a retry after a lost answer learns its first attempt landed; keeping the
owner keeps an `UNLOCKED` key auditable, the same reason `EXPIRED` keeps its record. What it cannot
buy is an answer once another holder has come and gone, because a record remembers only its latest
tenant -- `NOT_HELD` is the honest reply there, and is documented as such.

The lock surface is kept to what a caller can act on, and four things were dropped for failing that
test. `Lock.validate()` asked the cluster "is this still mine?" -- an honest question with a
dishonest use, because the answer is stale on arrival, and having it in the interface made
check-then-act the easy thing to write when `fencingToken()` is the thing that actually holds.
`LockInfoView` split a holder's view into acquire-time and current, but the only field that ever
differed between the two was the wall-clock lease deadline, which by its own javadoc is the wrong
clock for its holder; take that away and the split collapses, so both went and `remainingLease()` is
the one lease reading a holder gets. `LockAcquireResult.token()` and its `lockInfo()` duplicated
`lock().token()` and the lock's own snapshot, so the survivor was renamed `observed()` and now means
one thing -- the record that stood in the way -- and is null on success. And `lock(key, ttl, ZERO,
owner)` is routed into `tryLock` rather than through the waiting path, so the two forms cannot drift:
before, the zero-wait case spent an extra read to answer `TIMED_OUT` where a single attempt says
`HELD_BY_OTHER` or `HELD_BY_SELF`.

The layout version stays at 1 deliberately. Nothing about the fields changed, only which values go
in them, so a build that predates this decodes the marker fine and merely applies the older,
stricter `isReleased()`. It then reads the record as a lock with a lease of zero, which no clock is
behind, so the key is still free to acquire -- the property `LockValueCodecTest` pins. Bumping the
version would have been worse: an older reader would decode a new marker to `null` and report
`NOT_LOCK_RECORD`, which blocks acquires outright instead of degrading a status.

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
- **Never move an unfenced write that has reached the wire.** Not on `UNAVAILABLE`, not on silence,
  not on a dropped connection. That is the duplicate-apply hazard, and it is silent. The rule lives
  in two places for the two kinds of failure -- `worthAnotherCoordinator` for an answered one,
  `mayMoveToAnotherCoordinator` for no answer at all -- and a new retry path needs one of them.
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
