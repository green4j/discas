# 7. Writing your own starter

[6](06-embedding-a-node.md) gets you a running node. This is what stands between that and something
you would operate: diagnostics, probes, client security, hot reload, and an orderly shutdown.

The reference implementation is `DisCasNodeStarter`
(`discas-node/src/main/java/io/github/green4j/discas/node/starter/DisCasNodeStarter.java`) -- read it
alongside this. If your launcher needs the same shape, consider running that one instead and
configuring it; write your own when the node has to live inside a process you already own.

## Observability is opt-in

`DisCasNodeFactory.create` without an observer gives you `NodeObserver.NONE`, which reports
**nothing**. That is the right default for an embedded node -- its diagnostics belong to the
application -- and the wrong one for a daemon: a node that fails recovery and exits without saying so
anywhere is not operable.

The chain is built by decoration, each layer doing one thing and forwarding:

```java
import io.github.green4j.discas.node.LoggingNodeObserver;
import io.github.green4j.discas.node.MetricsNodeObserver;
import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.PeerStateObserver;
import io.github.green4j.discas.common.logging.Log;
import io.github.green4j.discas.common.metrics.MetricRegistry;
import io.github.green4j.discas.common.operator.OperatorAttention;

Log log = new Log(nodeId.value());
MetricRegistry metrics = new MetricRegistry();
OperatorAttention attention = new OperatorAttention(log);

PeerStateObserver peerState = new PeerStateObserver(clusterSize,
        new MetricsNodeObserver(metrics,
        new LoggingNodeObserver(log, attention, NodeObserver.NONE)));

DisCasNode node = DisCasNodeFactory.create(config, peerBootstrap, wal, peerState);
```

Order matters only in that `PeerStateObserver` must be the one you keep a reference to -- the probes
read readiness from it.

**Use one `OperatorAttention` for the whole process.** "The members file does not parse" and "a
peer's storage rolled back" are one operator's problem and belong on one surface with one alert rule
over it. The same register is passed to the reload seam below.

It needs a tick, because a condition with a window says nothing until it has been true long enough:

```java
node.loop().scheduleRepeat(OperatorAttention.CHECK_INTERVAL, attention::checkDue);
```

On the node's loop rather than a thread of its own -- every raise already happens there.

## Probes

```java
import io.github.green4j.discas.node.observability.NodeEndpoints;
import io.github.green4j.discas.common.observability.ObservabilityConfig;
import io.github.green4j.discas.common.observability.ObservabilityServer;

ObservabilityServer observability = ObservabilityServer.start(
        ObservabilityConfig.builder().bindAddress("127.0.0.1").port(9600).build(),
        NodeEndpoints.router(nodeId, node.healthSource(), peerState, metrics));

node.addLifecycleCloseable(observability);
```

Three endpoints: `/metrics`, `/health` and `/ready`. **Wire the liveness probe of your orchestrator
to `/health`, never to `/ready`** -- `/health` is deliberately local-only, and pointing a
`livenessProbe` at a quorum-aware endpoint turns a network partition into a cluster-wide crashloop,
killing every node exactly when each could still have served stale reads and rejoined.

Build the endpoints **last** among the node's wiring, so they only ever observe a fully built node,
and register them as a lifecycle closeable so the listening socket goes away with it.

The default bind address is loopback, on purpose: these endpoints report the cluster's topology and
every peer's identity. Widening it should be a deliberate act.

## Client access

Three independent things, all of them absent by default:

```java
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.client.auth.FileClientTokenStore;
import io.github.green4j.discas.node.acl.FileClientAcl;

// 1. Authentication -- who is calling.
FileClientTokenStore tokens = new FileClientTokenStore(tokenFile, reloadObserver);
ClientAuthenticator authenticator = new TokenClientAuthenticator(tokens);

// 2. Channel security -- TLS, or plaintext by omission.
ClientSecurityProvider clientSecurity = new TlsClientSecurityProvider(tlsConfig);

// 3. Authorization -- what that identity may touch.
node.registerClientAcl(new FileClientAcl(aclFile, reloadObserver));

TcpClientServerTransport clientServer = DisCasNodeFactory.createClientServer(node,
        new TcpClientServerBootstrap(clientBindAddress, ClientTransportConfig.defaults(),
                authenticator, clientSecurity));
node.addLifecycleCloseable(clientServer);
```

**An unbound ACL is permissive** -- every operation on every key is allowed. That preserves the
open contract for tests, and means forgetting `registerClientAcl` in production is a silent
wide-open cluster. If you accept it deliberately, say so in a log line at startup.

## Hot reload

Anything file-backed reloads through one mechanism, and all of it reports through one
`ReloadObserver`:

```java
import io.github.green4j.discas.common.io.LoggingReloadObserver;
import io.github.green4j.discas.common.io.MetricsReloadObserver;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.node.membership.FileMembers;

ReloadObserver reload = new MetricsReloadObserver(metrics,
        new LoggingReloadObserver(log, attention, ReloadObserver.NONE));

FileMembers members = new FileMembers(membersFile, reload);
node.addLifecycleCloseable(members);
```

Build this **first** -- the very first thing a node does is load its member list, so the seam has to
exist before the node does.

What reloads: the member list (addresses only), client tokens, the client ACL, and TLS material. A
reload that fails to parse keeps the last good value and raises an operator condition; the process
keeps running on what it had, which is why the condition matters.

For certificates, add rotation once the node exists, so the swap runs on its loop:

```java
CertRotationManager rotator = new CertRotationManager(
        reloadableTlsContext, tlsMaterialSource, initialMaterial,
        RenewalPolicy.defaults(), node.loop()::execute, reload);
rotator.start();
node.addLifecycleCloseable(rotator);
```

## Shutdown

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    try {
        node.close();          // drains the loop, closes everything registered with it
    } catch (Exception ignored) {
        // best-effort
    }
    Closeables.closeAll(toClose, failure -> attention.raise(
            OperatorState.SHUTDOWN_INCOMPLETE, null, "a resource threw while closing", failure));
}, "discas-node-shutdown"));
```

Two rules. **Close the node first** -- it drains its loop, so anything still queued runs before the
resources it touches disappear. And **report a resource that throws while closing** rather than
swallowing it: it means something still holds the data directory, which is exactly what makes the
next start fail confusingly. `Closeables.closeAll` closes in reverse acquisition order and continues
past failures.

## A checklist

| | Default if you skip it |
|---|---|
| `NodeObserver` chain | silent -- no logs, no metrics, no readiness |
| `OperatorAttention` tick | windowed conditions never become due |
| `ObservabilityServer` | no `/metrics`, `/health` or `/ready` |
| `ClientAuthenticator` | any caller is accepted at face value |
| `ClientSecurityProvider` | plaintext |
| `registerClientAcl` | **every key writable by everyone** |
| `ReloadObserver` | reload failures are invisible; the node serves stale config silently |
| lifecycle registration | sockets and watchers outlive the node |
| shutdown hook | no orderly drain |
