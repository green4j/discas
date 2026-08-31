# 1. Introduction

## Context

`discas` is a CASPaxos-based replicated key-value store in Java, for the **shared state a set of
microservices agrees on** -- locks, leadership, configuration, ownership. It is not a general
database and not a multi-tenant platform service: every key is a small register, every replica
holds every key, and `N` is a handful of nodes.

Two properties shape the whole implementation. **There is no leader** -- any member can coordinate
any write, so there is no election, no failover of a special role, and a client may address
whichever node it can reach. And **there is no log** -- CASPaxos agrees on one register at a time,
so there is no global revision, no cross-key ordering, and no replication stream. Everything else
in this guide follows from those two, especially [anti-entropy](06-anti-entropy.md) (which replaces
log replication) and the [scan/watch limits](07-client.md).

## The pieces

| Piece | What it is | Module |
|---|---|---|
| **Node** | one cluster member: acceptor, proposer, store, anti-entropy, tombstone collection | `discas-node` |
| **Client** | the library callers use: KV, scan, watch, locks | `discas-client` |
| **Agent** | an HTTP/JSON front end over a client, Consul-shaped, for callers in any language | `discas-agent` |
| **Admin** | an operator CLI: `dump`, `load`, `init` | `discas-admin` |

A node is embeddable -- it is a Java object with an event loop, not a daemon -- so a service can run
one in-process and reach it through an in-process transport with no sockets at all. The same class
is what `DisCasNodeStarter` wraps to make a standalone process.

## Module graph

```
discas-common --+-- discas-node ----+-- discas-admin
                |                   |
                +-- discas-client --+
                |                   +-- discas-example
                +-- discas-agent          discas-performance
                +-- (discas-int-test depends on all)
```

`discas-common` depends on nothing. It carries what both ends of a wire must agree on -- the client
message family, identities, framing, transport and security base types -- plus the infrastructure
neither end wants to own twice: the event loop, the logger, the metric registry, the CLI parser and
the HTTP server.

**`discas-node` and `discas-client` do not depend on each other.** That is deliberate and load
bearing: it is why the client cannot reach into node internals, and why anything they share
(`ClientMessage`, `ClientIngress`, `ResponseSink`, `InProcessClientRegistry`) has to live in
`common`. When you are tempted to add a dependency between them, the thing you want probably
belongs in `common`.

## Where the code is

| Concern | Start here |
|---|---|
| A cluster member, wired up | `discas-node/src/main/java/io/github/green4j/discas/node/DisCasNode.java` |
| The consensus round | `discas-node/src/main/java/io/github/green4j/discas/node/Proposer.java`, `Acceptor.java` |
| In-memory state | `discas-node/src/main/java/io/github/green4j/discas/node/LocalStore.java` |
| Durability | `discas-node/src/main/java/io/github/green4j/discas/node/wal/FileWal.java` |
| The client API | `discas-client/src/main/java/io/github/green4j/discas/client/DisCasClient.java` |
| The single-threaded loop everything runs on | `discas-common/src/main/java/io/github/green4j/discas/common/EventLoop.java` |
| Standalone entry points | `discas-node/src/main/java/io/github/green4j/discas/node/starter/DisCasNodeStarter.java` and the agent/admin equivalents under `discas-agent/.../agent/starter/` and `discas-admin/.../admin/starter/` |

## How to read this code

Three habits pay off quickly.

**Follow the event loop, not the call stack.** Almost nothing blocks. A node owns one
`EventLoop` thread and every piece of its state is confined to it, so "who calls this" matters less
than "which loop turn does this happen on". Methods that must run there say so and enforce it
(`requireEventLoop`).

**The javadoc carries the argument.** Non-obvious decisions are documented at the type or method
that embodies them rather than in a separate document -- `ClientErrorCode` explains the error
taxonomy, `LocalStore` explains why a delete leaves a tombstone, `HttpServer`'s class javadoc is a
full manual for the server. This guide gives you the shape and points at those.

**Names of states and codes are the vocabulary.** `NodeState`, `RoundFailure`, `ClientErrorCode`,
`PeerHelloRespStatus`, `PurgeAnswer`, `OperatorState` are small enums that each name a complete set
of outcomes. Reading those six files is the fastest way to learn what can happen.

## Next

- [2. Development environment](02-development.md) -- build, run, debug
- [3. CASPaxos](03-caspaxos.md) -- the algorithm and what is specific here
- [12. Reference](12-reference.md) -- repository and package maps
