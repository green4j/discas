# 6. Embedding a node

## Why you would

A node is a Java object with an event loop, not a daemon. Running one inside your service removes a
hop and a deployment unit: your process *is* a cluster member, and a co-located client reaches it
without a socket. The trade is that your process's lifecycle becomes the node's -- a restart of your
service is a restart of a member.

You need `discas-node` on the classpath in addition to `discas-client`.

### The trade: a fixed, identity-bound deployment shape

Embedding suits a set of services coordinating *each other*, where the participants and the cluster
are the same processes. What it asks in return is that those processes accept the shape a cluster
member has: a stable `node_id` (bound into the certificate SAN when mTLS is on), a reachable
address, its own durable volume, and a **fixed `N`**, because `N` defines quorum and is frozen while
the cluster runs.

So an embedded member is not a stateless replica, and that decides the topology:

| | How it works | Cost |
|---|---|---|
| **Pinned subset** (recommended) | `N` designated instances (3 or 5) embed a node; every other instance is a **pure client** | none to the elastic part of your fleet -- quorum stays constant while it autoscales |
| **Every instance a member** | simplest to wire | **the fleet size *is* `N`**: every scale event changes quorum and needs a coordinated restart |

The pinned subset is supported directly rather than being a workaround: a client may be configured
with a subset of the cluster, because `DisCasClient` derives its scan quorum from the `N` the nodes
report rather than from its own peer list. On Kubernetes it means the `N` members are a StatefulSet
with a volume per pod, while the rest of the fleet stays an ordinary autoscaled Deployment running
pure clients -- only the small fixed part is stateful.

"Every instance a member" makes your service *become* the StatefulSet: fixed replica count, no
horizontal autoscaling, no scale-to-zero. That is a reasonable shape on dedicated or fixed-size
infrastructure and a poor one on an elastic platform.

Two properties work in your favour either way. Member addresses are held **unresolved** and resolved
fresh at every dial, so stable per-pod DNS names survive rescheduling even though the IP changes --
and an address change can be hot-reloaded without dropping the cluster. And because the peer
protocol is **plain TCP with no UDP**, nothing needs `hostNetwork` or exposed gossip ports, which is
the step that most often complicates a Consul deployment on Kubernetes.

### When not to embed

Run the node as its own process instead -- with [the agent](../agent/README.md) in front of it if
your callers are not on the JVM -- whenever the store's membership should not be tied to the
lifecycle of a participating service. A configuration source is the clearest example: you do not
want the cluster underneath your config to resize because one autoscaling service scaled. If you
want the store scaled, upgraded and operated independently of the workloads using it -- the usual
expectation on a cloud platform -- that is the standalone shape, and the
[Operator Guide](../operator/README.md) is written for it.

Rolling updates are fine in both topologies: restarting instances one at a time holds `N` constant
and quorum intact. What you cannot do without a coordinated restart is change `N` itself --
[3. Cluster and members](../operator/03-cluster.md#changing-n).

## What you must supply

`DisCasNodeFactory.create` takes three things, and every deployment differs only in what it passes:

| | What it is | Choices |
|---|---|---|
| `NodeConfig` | identity and every timing | `new NodeConfig(nodeId, clusterId, clusterSize)` or `NodeConfig.builder()` |
| a **peer bootstrap** | how members reach each other | `InProcessPeerBootstrap` or `TcpPeerBootstrap` |
| a `Wal` | durability | `FileWal`, or your own |

Optionally a fourth, a `NodeObserver` -- and it matters more than it looks; see
[7. Writing your own starter](07-custom-starter.md#observability-is-opt-in).

## In-process: a cluster inside one JVM

For tests, for a single-process demo, and for anything where the "cluster" is not really
distributed. No sockets are involved at all.

```java
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerBootstrap;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;

List<NodeId> nodeIds = List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));
ClusterId clusterId = ClusterId.of("embedded");

List<DisCasNode> nodes = new ArrayList<>();
for (NodeId id : nodeIds) {
    nodes.add(DisCasNodeFactory.create(
            new NodeConfig(id, clusterId, nodeIds.size()),
            new InProcessPeerBootstrap(InMemoryMembers.ofNodes(nodeIds)),
            myWal()));
}

// Clients first, nodes started after: an in-process node registers itself with the
// process-wide registry when it is created, so the client can already find it.
DisCasClient client = DisCasClientFactory.create(
        ClientId.of("embedded-client"),
        new InProcessClientBootstrap(nodeIds));

for (DisCasNode node : nodes) {
    node.start();
}
```

**Construct every node before starting any of them.** A node registers with
`InProcessClientRegistry` at construction (the factory does it for you), and it needs its peers
registered before it can talk to them.

## TCP: a real cluster member

```java
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpMemberInfo;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.client.ClientTransportConfig;

// 1. Bind the peer socket FIRST, so its address is a fact before anything validates it.
ListenSocket peerSocket = ListenSocket.bind(new InetSocketAddress("0.0.0.0", 7001));

// 2. The member list every node validates at construction.
Map<NodeId, InetSocketAddress> peers = Map.of(
        NodeId.of("1"), new InetSocketAddress("10.0.0.1", 7001),
        NodeId.of("2"), new InetSocketAddress("10.0.0.2", 7001),
        NodeId.of("3"), new InetSocketAddress("10.0.0.3", 7001));

DisCasNode node = DisCasNodeFactory.create(
        new NodeConfig(NodeId.of("1"), ClusterId.of("prod"), peers.size()),
        new TcpPeerBootstrap(peerSocket, InMemoryMembers.ofTcp(peers),
                TcpTransportConfig.defaults()),
        wal);

// 3. Only if external clients connect to this node.
TcpClientServerTransport clientServer = DisCasNodeFactory.createClientServer(node,
        new TcpClientServerBootstrap(
                new InetSocketAddress("0.0.0.0", 8001),
                ClientTransportConfig.defaults()));

node.start();
```

`TcpPeerBootstrap` also takes a plain `InetSocketAddress` and binds it for you, so the address a
bootstrap carries is always settled. **Pre-bind when you need to read the address back** -- port `0`
in a test, an ephemeral port in a two-phase assembly: bind every node's listener, read what each
actually got, then hand every node the resulting members map, which each one validates at
construction. Ownership transfers to the transport either way: closing the node closes the socket.
(The client-facing bootstrap accepts a pre-bound `ListenSocket` too, but only on the overload that
takes an authenticator and a security provider as well.)

`InMemoryMembers` is a fixed list. For a list that reloads from a file without a restart, use
`FileMembers`. **`N` is frozen at startup**: a reloaded list that does not define exactly `N`
members, or that no longer contains this node, is rejected whole and the node keeps the list it had.
Within that, re-addressing a member is the change the reload is *for* -- a peer that moved is
reconnected at its new address. Swapping one member id for another passes the same guard and will be
applied, but it is a reconfiguration of the quorum performed without consensus; do not use it as
one.

## Durability

```java
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

FileWal wal = new FileWal(StorageConfig.builder()
        .baseDirectory(Path.of("/var/lib/discas/node-1"))
        .walMaxFileBytes(64 * 1024 * 1024)
        .snapshotRetentionCount(2)
        .build());
wal.initialize();       // must be called before the node uses it
```

That directory is the node's identity across restarts -- see
[what a node does with it](../dev/05-storage.md#recovery). Never copy one to another node.

**There is no published non-durable `Wal`.** If you want a node that keeps nothing -- a test, a demo,
a cache-like deployment where losing everything is acceptable -- implement the `Wal` interface
yourself. The rule is one sentence: *every method answers as a storage that has just been born and
has forgotten everything.* `replayTail` replays nothing, `openSnapshot` returns `null`, `append` and
`force` succeed, `isDegraded` is false. Two are not no-ops and must answer honestly:
`incarnation()` returns a **fresh** id on every construction -- that is the true answer, not a
degenerate one -- and `ceilingIsProven()` returns `false`, because a log that recorded nothing cannot
prove a ceiling. `beginSnapshot()` still has to hand back a working `SnapshotWriter`. Budget closer
to a hundred lines than thirty.

Be aware of what you are giving up: such a node proves no promise ceiling of its own and takes a
floor from the cluster on every restart, so it cannot be the member that carries the cluster's
memory.

## A co-located client

```java
DisCasClient client = DisCasClientFactory.createColocated(
        ClientId.of("my-service"), node.loop(), nodeIds);
```

This shares the node's event loop, so the whole embedded deployment is **one thread**. Closing the
client leaves the loop to the node.

The rule that completions must not block now has teeth: a blocking continuation stops consensus,
timers and peer I/O together. Hop to your own executor for anything real.

## Lifecycle

```java
node.start();     // begins replay; returns immediately, the node is not yet serving
node.healthSource().state();   // REPLAYING -> AWAITING_FLOOR? -> SERVING
node.close();     // drains the loop and releases everything registered with it
```

`start()` does not mean ready. A node replays its log first, and if it started with no durable state
it then asks the cluster for a promise floor -- it is reachable but serves nothing until that
completes. Wait by asking rather than sleeping:

```java
// The honest readiness check: a read that succeeds.
client.get("__ready_probe__").get(30, TimeUnit.SECONDS);
```

Register anything whose life should match the node's:

```java
node.addLifecycleCloseable(clientServer);   // closed when the node closes
```

## What you have not wired yet

The factory gives you a working node with **no observability, no client authentication and no
authorization** -- silent and open. That is right for a test and wrong for production.
[7. Writing your own starter](07-custom-starter.md) is the rest.
