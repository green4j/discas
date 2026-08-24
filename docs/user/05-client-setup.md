# 5. Client setup

## Transports

Two, and they are chosen by which bootstrap you hand the factory.

**TCP** -- the normal case, a client talking to a cluster over the network:

```java
DisCasClient client = DisCasClientFactory.create(
        ClientId.of("orders-service"),
        new TcpClientBootstrap(nodes, ClientTransportConfig.defaults()));   // Map<NodeId, InetSocketAddress>
```

**In-process** -- no sockets at all, for a client in the same JVM as the nodes it talks to. Used by
most of the test suite and by an embedded deployment:

```java
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;

DisCasClient client = DisCasClientFactory.create(
        ClientId.of("embedded"),
        new InProcessClientBootstrap(List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"))));
```

The in-process bootstrap takes node **ids** only -- the nodes register themselves in a process-wide
registry, so there is nothing to address. Setting that up is [6](06-embedding-a-node.md).

## Timeouts

```java
import io.github.green4j.discas.client.DisCasClientConfig;

DisCasClientConfig config = DisCasClientConfig.builder()
        .perAttemptTimeout(Duration.ofSeconds(2))
        .requestDeadline(Duration.ofSeconds(10))
        .scanTimeout(Duration.ofSeconds(5))
        .build();

DisCasClient client = DisCasClientFactory.create(clientId, bootstrap, config);
```

| Setting | Default | What it bounds |
|---|---|---|
| `perAttemptTimeout` | 5 s | one coordinator's attempt |
| `requestDeadline` | 30 s | the whole operation, **not** reset by retries |
| `scanTimeout` | 10 s | a scan's fan-out |
| `peerRetryMinBackoff` / `peerRetryMaxBackoff` | 50 ms / 2 s | how long a failing coordinator is skipped |
| `lockMinBackoff` / `lockMaxBackoff` | 20 ms / 80 ms | retry pacing inside `lock(...)` |
| `watchMinBackoff` / `watchMaxBackoff` | 200 ms / 1 s | re-poll pacing inside `watch(...)` |
| `shutdownAwaitTimeout` | 5 s | how long `close()` waits for the loop |

`build()` rejects a `requestDeadline` at or below `perAttemptTimeout` -- that combination cuts the
first coordinator off before its turn is up and turns every operation into an immediate failure.

**Set `requestDeadline` under your own deadline.** If you serve HTTP with a 10-second budget, a
client whose scan also settles at 10 seconds means your caller gets a gateway timeout instead of the
partial answer that was about to arrive.

## Authentication

Which mode applies is decided by the **node**; the client supplies what that mode needs.

**Token** -- a shared secret in the connection handshake:

```java
new TcpClientBootstrap(nodes, ClientTransportConfig.defaults(), "the-token");
```

**mTLS** -- the identity comes from your certificate's CN, so the `ClientId` you pass must match it:

```java
import io.github.green4j.discas.common.transport.tls.TlsClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsContexts;
import java.security.KeyStore;

KeyStore keys = KeyStore.getInstance("PKCS12");
try (InputStream in = Files.newInputStream(keyStorePath)) {
    keys.load(in, keyStorePassword);
}
KeyStore trust = KeyStore.getInstance("PKCS12");
try (InputStream in = Files.newInputStream(trustStorePath)) {
    trust.load(in, trustStorePassword);
}

TlsConfig tls = TlsConfig.of(TlsContexts.build(keys, keyStorePassword, trust));

new TcpClientBootstrap(nodes, ClientTransportConfig.defaults(), null,
        new TlsClientSecurityProvider(tls));
```

For server-authenticated TLS with a token instead of a client certificate, use
`TlsClientSecurityProvider.serverAuthOnly(tls)` and pass the token as well.

**Plaintext, no credential** -- pass neither. The node must be running its permissive mode (which is
what `TcpClientServerBootstrap` gives you when you pass no authenticator). Anything else refuses the
**connection handshake** with `ACCESS_DENIED` rather than failing an individual operation, so the
symptom is `serverRejectedHello` on the observer and every request failing, and no retry will help.

Authorization is separate and lives on the node: a per-client prefix ACL decides *what* your
`ClientId` may touch, independently of how you authenticated.

## Observability

Everything the client does is reported through `ClientObserver`, whose methods are all no-op
defaults. Decorators compose:

```java
import io.github.green4j.discas.client.LoggingClientObserver;
import io.github.green4j.discas.client.MetricsClientObserver;
import io.github.green4j.discas.client.ClientObserver;

ClientObserver observer =
        new MetricsClientObserver(metricRegistry,
        new LoggingClientObserver(log, operatorAttention, ClientObserver.NONE));

new TcpClientBootstrap(nodes, ClientTransportConfig.defaults(), token, security, observer);
```

The default is **silent** -- an embedded client's diagnostics belong to the embedding application, so
nothing is printed unless you ask. `StderrClientObserver.INSTANCE` is the quick way to see something
while developing.

Worth wiring in production: `requestFailedOver` and `sendFailed` (a coordinator is unhealthy),
`connectionLost`, `serverRejectedHello` (your credentials or cluster id are wrong),
`clusterSizeDisagreement` (your node list does not match the cluster), and `clientClockStepped`
(your clock moved, which matters if you hold locks).

## Lifecycle

```java
try (DisCasClient client = DisCasClientFactory.create(clientId, bootstrap)) {
    // ...
}
```

`close()` fails every in-flight request rather than leaving callers hanging, then shuts down the
event loop it owns. It throws `ClientLifecycleException` if the loop does not stop within
`shutdownAwaitTimeout` -- worth knowing, because that throw comes out of a try-with-resources.

**Sharing a loop.** A client co-located with a node can run on the node's loop instead of starting a
thread of its own:

```java
DisCasClient client = DisCasClientFactory.createColocated(
        ClientId.of("embedded"), node.loop(), List.of(NodeId.of("1"), NodeId.of("2")));
```

Then `close()` releases the client but leaves the loop to its owner. Everything else is unchanged,
including the rule that completions run on that loop and must not block it -- which now means not
blocking the node either.

## Threading

One client is thread-safe and meant to be shared. Its I/O and its completion bookkeeping happen on
one event loop thread, so:

- calling an operation from any thread is fine;
- the returned future completes **on the loop**, so a non-async `thenApply`/`thenAccept` body runs
  there -- keep it short and non-blocking;
- to do real work on a completion, hop: `thenApplyAsync(fn, yourExecutor)`;
- `join()`/`get()` from the loop thread itself deadlocks. That only comes up inside a callback, and
  it is the one way to hang the client.
