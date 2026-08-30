# 11. Limitations and compatibility

## Context

These are boundaries, not a backlog. Each is a consequence of a decision made on purpose, and the
entry says which -- so that hitting one is a rediscovery of a known trade rather than a surprise.
Most of them trace back to the same root: **CASPaxos has no log**, so there is no global revision and
no ordering between keys.

## Data model

**No multi-key transactions, and no total order across keys.** Agreement is per register. Two writes
to different keys have no defined order, and there is no revision that means "the cluster at instant
T". Not an unimplemented feature -- undefinable in this model.

**`scan` is a key enumeration, not a snapshot.** It merges each node's local state across a quorum,
so every key committed before it started appears, but different keys may reflect different instants
and the page carries no values. See [7. The client](07-client.md#scan).

**Watch is a coalescing blocking query, not an event stream.** A register keeps only its current
value, so there is no history to replay and intermediate values may be skipped.

**There is no value-compared CAS**, and there will not be -- see
[3. CASPaxos](03-caspaxos.md#version-fenced-cas-and-no-value-compared-cas).

**Same-key contention duels.** Leaderlessness removes the serialization point a leader provides, so
concurrent writers to one key can lose ballots to each other. `BALLOT_LOST` is determinate and cheap
to retry, but a hot single key is the wrong shape for this store.

## Membership

**`N` is frozen for a node's lifetime, and online resizing is not planned.** It is carried in the
peer handshake, and two members that disagree refuse to connect -- a mixed-`N` cluster would form two
quorums over one key. Changing `N` is a coordinated restart, or `discas-admin init` against a dump.

This is *not planned* rather than deferred: adding members to a CASPaxos cluster makes every write
slower, because a round talks to a quorum of a larger `N`. `N` is a design-time decision.

**A reload covers member addresses, not the member set.** It is rejected unless the file names
exactly `N` members including this node.

**Every member is a voter.** No observers, learners or witnesses -- so read capacity does not scale
by adding nodes, and every added node costs write latency. Also not planned, for the same reason.

## Operational

**A node that starts with no durable state cannot serve until a quorum answers it.** That is the
promise floor (see [3. CASPaxos](03-caspaxos.md#promise-durability-the-ceiling-and-the-floor)), and
it has no timeout and no override: an operator who has accepted the loss says so by removing what is
left, which turns the cluster into the "survived nowhere" case.

**A degraded WAL is one-way.** Nothing short of a restart clears it, and while degraded the node
NACKs consensus and answers `503`. Refusing to be a member is the deliberate trade against becoming
an unreliable one.

**Storage restored from a copy is the one fault a node cannot see about itself** -- it is
byte-continuous, internally consistent and older, so its own replay reports success.
`PromiseCeilingHistory` catches it at a peer's handshake, but that memory is per process, so a
rollback coinciding with a restart of every peer that remembered passes unseen. Narrowed, not closed.

**Lease locks are lease locks.** A holder paused past its lease by a GC pause or a VM freeze can be
displaced while its handle still looks valid. `ClusterClock` removes the *clock disagreement* part of
the problem by correcting against the coordinators, but the pause part is inherent -- guard the
protected resource with `fencingToken()`.

## Compatibility

Three versioned surfaces, each refusing what it cannot read rather than misreading it:

| Surface | Where | Rule |
|---|---|---|
| Peer/client wire | `TransportProtocol.PROTOCOL_VERSION` | exchanged once per connection in the hello; a mismatch closes it with `PROTOCOL_MISMATCH`. This gate is what lets the codecs resolve enums strictly |
| On-disk | `StorageFormat.FORMAT_VERSION`, `LAYOUT_VERSION` | an incompatible file is refused at open |
| Dump file | `DumpCodec.FORMAT_VERSION` | refused on read |

These numbers distinguish one *released* format from another, so each is bumped on the first
incompatible change after a release and never before one -- a value above the number of shipped
formats claims a compatibility history with no migration code behind it. The constants are the
authority on their own values, which is why none is repeated here: a version written down in two
places is a version that will disagree with itself.

Wire enums carry an **explicit byte**, never `ordinal()`, so reordering constants cannot change what
travels. `ClientErrorCode` is the deliberate exception to strictness on the read side: unknown values
decode to `INTERNAL`, because codes are added without a version bump and a newer node legitimately
sends a byte an older client has not heard of.

There is **no rolling-upgrade story across a protocol bump**: two members with different
`PROTOCOL_VERSION` cannot carry messages to each other, so a bump means a full-cluster restart.

## What is deliberately absent

Not gaps -- decisions:

- multi-tenancy, quotas, per-namespace isolation
- a bundled distribution, service discovery, or a packaged operator
- any runtime dependency; the HTTP server, CLI parser, metric registry and logger are written here
- a failure detector: no heartbeats and no eviction. A round either reaches a quorum or does not,
  which is a better answer than a liveness signal because it measures the thing a caller cares about

## Where the numbers are

[13. Performance](13-performance.md) -- the envelope `discas-performance` publishes, the conditions
every figure was taken under, and the four shape claims that follow from the protocol rather than
from the hardware.
