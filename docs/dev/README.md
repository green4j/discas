# discas Developer Guide

For people **working on** discas: where the logic lives, how it works, how to change it, how to know
the change is correct, and what it can break. Not an operator manual -- a reader running a cluster
wants the operator set instead.

It is written to be read in order the first time and used as a reference afterwards. It is
deliberately short: detail that can be recovered from the source is left in the source, and what
survives here is the argument that cannot.

## The five questions

| Question | Where |
|---|---|
| Where is the logic I need? | [12. Reference](12-reference.md) -- repository and package maps |
| How does it work? | [3](03-caspaxos.md)-[9](09-agent-and-admin.md), one file per component |
| How do I change it? | each component file ends with *How to change it safely* |
| How do I know it is correct? | [10. Testing](10-testing.md), and each component's *Tests* table |
| What will my change break? | [11. Limitations and compatibility](11-limitations.md) |
| What does it cost? | [13. Performance](13-performance.md) |

## Contents

| | |
|---|---|
| [1. Introduction](01-introduction.md) | what discas is, the pieces, the module graph, how to read the code |
| [2. Development environment](02-development.md) | repository layout, build, running a node and a local cluster, debugging |
| [3. CASPaxos](03-caspaxos.md) | the algorithm, and everything specific to this implementation |
| [4. The node](04-node.md) | lifecycle, the single event loop, request flow, observability seams |
| [5. Storage](05-storage.md) | the in-memory store, the WAL, snapshots, recovery |
| [6. Anti-entropy and tombstones](06-anti-entropy.md) | how replicas converge, and how deletes are eventually reclaimed |
| [7. The client](07-client.md) | routing and failover, write determinism, scan, watch, locks |
| [8. Transport and security](08-transport-security.md) | framing, the peer handshake, TLS, authentication and authorization |
| [9. Agent and admin](09-agent-and-admin.md) | the HTTP front end, and the operator CLI |
| [10. Testing](10-testing.md) | levels, tags and profiles, every run mode, what proves what |
| [11. Limitations and compatibility](11-limitations.md) | the boundaries, and the three versioned surfaces |
| [12. Reference](12-reference.md) | maps, key interfaces, wire protocols, glossary |
| [13. Performance](13-performance.md) | the measured envelope, and the shape it describes |

## Shape of a component file

Each of 3-9 follows the same order, so you can jump to the part you need:

> **Context** -- why this exists, in a few paragraphs
> **Architecture** -- the pieces and how they relate
> **Flow** -- what happens, in order
> **Source map** -- concern -> file
> **Tests** -- question -> suite
> **How to change it safely** -- the invariants a change must not break

## Two things to know before reading

**Everything in a node runs on one event loop thread.** State is unsynchronized because of it, and
nothing may block. See [4. The node](04-node.md).

**There is no log.** CASPaxos agrees on one register at a time, so there is no global revision and no
ordering between keys. Most limitations follow from this one fact -- see
[3. CASPaxos](03-caspaxos.md).
