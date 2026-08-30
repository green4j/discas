# 1. Getting started

## Depending on discas

Published to Maven Central under `io.github.green4j`:

```groovy
// build.gradle
def discasVersion = '<the version you want>'   // see Maven Central for what is published

dependencies {
    implementation "io.github.green4j:discas-client:$discasVersion"
    // and, only if you embed a node:
    implementation "io.github.green4j:discas-node:$discasVersion"
}
```

`discas-all` bundles common, node, client, agent and admin into one jar if you would rather
depend on a single coordinate.

**Gradle composite build** instead, while you are following `main`:

```groovy
// settings.gradle
includeBuild '../discas'
```

Or build the jars and put them on the classpath:

```bash
cd discas && ./gradlew build
CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)
```

You need `discas-client` to call a cluster, and additionally `discas-node` to run one. Both
re-export `discas-common`, where `ClientId`, `NodeId`, `ReadConsistency` and `ClientErrorCode` live,
so you do not declare it yourself. Java 11 or newer -- the modules are compiled with `--release 11`.

## A client

```java
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;

import java.net.InetSocketAddress;
import java.util.Map;

Map<NodeId, InetSocketAddress> nodes = Map.of(
        NodeId.of("1"), new InetSocketAddress("10.0.0.1", 8001),
        NodeId.of("2"), new InetSocketAddress("10.0.0.2", 8001),
        NodeId.of("3"), new InetSocketAddress("10.0.0.3", 8001));

DisCasClient client = DisCasClientFactory.create(
        ClientId.of("orders-service"),
        new TcpClientBootstrap(nodes, ClientTransportConfig.defaults()));
```

**Give it every node**, not one. There is no leader, so the client picks a coordinator per request
and moves on to another when one cannot serve it -- a shorter list still works but limits failover,
and `scan` needs a majority of the *real* cluster size to answer. The client learns that size from
the node it connects to, so it can tell a subset from the whole.

The `ClientId` is who you are: it appears in the connection handshake, and it is what authentication
and authorization key off. Pick something stable and specific to the caller.

One client is meant to be **long-lived and shared** -- it holds the connections, the pending-request
table and the peer rotation. Do not create one per request.

## Reading and writing

Keys and values are **bytes**. Most operations take the key as either a `String` (encoded UTF-8) or
a `ByteBuffer`; values are always `ByteBuffer`, apart from `cas`, which also accepts a `String`. A
two-line helper saves repeating the encode:

```java
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

static ByteBuffer utf8(final String s) {
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
}

static String str(final ByteBuffer b) {
    return b == null ? null : StandardCharsets.UTF_8.decode(b).toString();
}
```

```java
// Write. Returns the version it committed at.
Version at = client.put("service/leader", utf8("node-7")).join();

// Read. value() is null when the key is absent; version() is where it stands now.
String leader = str(client.get("service/leader").join().value());
```

`join()` is fine in an example. In real code stay asynchronous, and remember the completion runs on
the client's loop:

```java
client.get("service/leader")
        .thenApplyAsync(read -> decode(read.value()), applicationExecutor)   // hop off the loop
        .thenAccept(this::useLeader);
```

## Shutting down

```java
client.close();
```

`close()` fails every request still in flight rather than leaving callers on futures that can never
complete. It also shuts down the event loop, unless the client was built to share one -- see
[5. Client setup](05-client-setup.md#lifecycle).

## Where to go next

- **You are about to write a read-modify-write** -- [2. Keys and values](02-key-value.md) first.
  Doing it with `get` then `put` is the one mistake this store is built to make unnecessary.
- Enumerating or following keys -- [3. Scan and watch](03-scan-and-watch.md).
- Leader election, ownership, mutual exclusion -- [4. Locks](04-locks.md).
- TLS, tokens, timeouts, metrics -- [5. Client setup](05-client-setup.md).
- A cluster member inside your own process -- [6. Embedding a node](06-embedding-a-node.md).
