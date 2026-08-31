# 4. The node

## Context

`DisCasNode` is one cluster member, assembled from parts that each do one job: an `Acceptor` and a
`Proposer` for [consensus](03-caspaxos.md), a `LocalStore` over a `Wal` for
[state](05-storage.md), `AntiEntropy` and the tombstone machinery for
[convergence](06-anti-entropy.md), a `ClientHandler` for the client protocol, and a `PeerTransport`
for the mesh.

It is a Java object, not a daemon. `DisCasNodeStarter` wraps it into a process, but an application
can construct one in the same JVM as its own code and reach it with no sockets.

**Everything runs on one `EventLoop` thread.** Not a convention -- the enforced rule. Node state is
therefore unsynchronized and safe, and the price is that no callback may block: a handler that
sleeps stops consensus, timers and peer I/O together.

## The event loop

`EventLoop` is a single thread running a fixed cycle: poll every registered `IoDriver`, run up to 64
queued tasks, fire due timers, and -- if none of that produced work -- back off (spin, then yield,
then park for a doubling interval clamped to the next timer's deadline).

The drivers are the transports. A node registers two: the peer mesh and the client-facing server.
That is exactly why a driver must never block -- `pollNow()` uses `selectNow()`, because a blocking
wait in one driver would delay the other's ready data.

Work reaches the loop three ways: `execute(task)` from any thread, `schedule`/`scheduleRepeat`
timers, and driver polling. A node arms three repeating timers at startup -- snapshots
(`snapshotInterval`), promise eviction (`promiseEvictionInterval`) and WAL force
(`walForceInterval`) -- plus the anti-entropy cycle and the tombstone sweep, which reschedule
themselves after each completion rather than on a fixed repeat.

## Lifecycle

`NodeState` is the whole model, and readiness, what traffic is answered, the probes and the log all
derive from it:

```
REPLAYING --has state----------------------> SERVING --> CLOSING
   |                                          ^
   +--none--> AWAITING_FLOOR --floor adopted--+
                 ^     |
                 +-----+ keeps asking

any state --> FAILED   (nothing continues; the loop shuts down)
```

- **REPLAYING** -- reading the snapshot and the WAL tail in batches, yielding to the loop between
  them so the node stays responsive. Peers are refused a handshake (`NOT_REPLAYED`) because a node
  that cannot state a promise ceiling cannot be checked.
- **AWAITING_FLOOR** -- started with no durable state; asking the cluster for a promise floor. It is
  *running and reachable but not a member of the quorum*: it answers that one question and refuses
  everything else. `/health` is 200 and `/ready` is 503, which is the correct pair.
- **SERVING** -- normal.
- **FAILED** -- evidence in conflict with itself: a corrupt WAL, an unreadable marker, or a log that
  needs more heap than this node has. The node stops rather than serving something it cannot
  reproduce.

A request arriving before `SERVING` is **answered**, not dropped: clients get `NOT_READY` and move
on immediately instead of waiting out a per-attempt timeout against every node in turn.

## Request flow

```
client -> ClientIngress -> ClientHandler -> ClientAuthorizer  (trusted ClientId, not the claimed one)
                                         -> Proposer.write / cas / read
                                              -> prepare  -> peers (Acceptor)
                                              -> transform
                                              -> accept   -> peers (Acceptor)
                                         -> ResponseSink
```

`ClientHandler` is the boundary where the wire becomes storage: a `ClientMessage` carries
`ByteBuffer`s, everything below speaks `HashedBytes` (bytes plus a cached hash). It converts once,
up front, and checks size limits before adopting anything.

Authorization keys off the `ClientId` **bound to the connection at CLIENT_HELLO**, never the
`senderId` inside the message -- see [8. Transport and security](08-transport-security.md).

## Observability seams

Everything a node does is reported through one interface, `NodeObserver`, whose methods are all
`default` no-ops. Concrete behaviour is composed as a chain of decorators, each doing one thing and
forwarding:

```java
NodeObserver observer =
        new PeerStateObserver(clusterSize,
        new MetricsNodeObserver(registry,
        new LoggingNodeObserver(log, attention, NodeObserver.NONE)));
```

- `PeerStateObserver` -- accumulates which peers have completed a handshake and derives whether this
  node can still reach a quorum. This is the source of truth for `/ready`.
- `MetricsNodeObserver` -- counters, resolved once at construction so a callback on the loop is an
  increment and nothing else. Keys are never labels: that would be one time series per key.
- `LoggingNodeObserver` -- lifecycle facts go to the log; anything needing an operator to *act* goes
  through `OperatorAttention`, which attaches the action and exposes one metric sample while the
  condition lasts.

`OperatorState` is the closed set of conditions that need action; each carries its own remedy text
and, where the condition is ordinarily transient, a window (`normalFor`) it must persist for before
anything is reported. That is why there is no warning level: transience is handled by the window.

Three endpoints, served on a port of their own (`NodeEndpoints`): `/metrics` (Prometheus text),
`/health` (**liveness** -- local only, so a partition cannot become a cluster-wide crashloop) and
`/ready` (**readiness** -- additionally quorum-connected).

## Source map

| Concern | File |
|---|---|
| Assembly, lifecycle, timers | `discas-node/src/main/java/io/github/green4j/discas/node/DisCasNode.java` |
| Lifecycle states | `discas-node/src/main/java/io/github/green4j/discas/node/NodeState.java` |
| Client protocol -> rounds | `discas-node/src/main/java/io/github/green4j/discas/node/ClientHandler.java` |
| The loop | `discas-common/src/main/java/io/github/green4j/discas/common/EventLoop.java` |
| Timings and identity | `discas-node/src/main/java/io/github/green4j/discas/node/NodeConfig.java` |
| Observer seam | `discas-node/src/main/java/io/github/green4j/discas/node/NodeObserver.java` |
| Readiness | `discas-node/src/main/java/io/github/green4j/discas/node/PeerStateObserver.java` |
| Operator conditions | `discas-common/src/main/java/io/github/green4j/discas/common/operator/OperatorState.java`, `OperatorAttention.java` |
| Probes | `discas-node/src/main/java/io/github/green4j/discas/node/observability/NodeEndpoints.java` |
| Standalone process | `discas-node/src/main/java/io/github/green4j/discas/node/starter/DisCasNodeStarter.java` |

## Tests

| Question | Suite |
|---|---|
| Does a not-yet-ready node answer rather than go silent? | `discas-int-test/src/test/java/io/github/green4j/discas/node/NodeNotReadyTest.java` |
| Does readiness track handshakes, not sockets? | `discas-int-test/src/test/java/io/github/green4j/discas/node/PeerStateObserverTest.java` |
| Do the probes report what they claim? | `discas-int-test/src/test/java/io/github/green4j/discas/node/observability/ObservabilityEndpointsTest.java` |
| Does every client operation map to the right error code? | `discas-int-test/src/test/java/io/github/green4j/discas/node/ClientHandlerErrorCodeTest.java` |

## How to change it safely

- **Never block on the loop.** An operation that needs to wait registers a timer or hands the work
  to another thread and marshals the result back with `execute`. Anything else stops the node.
- **A new operator-facing condition is an `OperatorState` constant**, not an ad-hoc `error` line --
  the enum is what keeps the log line, the metric sample and the documented remedy from drifting.
  A condition with no action for an operator is a metric, not a state.
- **Do not add a `NodeObserver` implementation that also does something else.** One decorator, one
  concern, forwarding via `DelegatingNodeObserver`; that is what lets a deployment pick logging
  without metrics or the reverse.
- **Adding a timer** means deciding whether it reschedules after completion (anti-entropy, sweeps --
  a slow cycle must not overlap itself) or repeats on a fixed period (snapshots, WAL force).
