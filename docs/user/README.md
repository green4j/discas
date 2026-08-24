# Using discas

For people calling discas from their own code. Two APIs, and you may need either or both:

| | |
|---|---|
| **Client API** | you have a cluster and want to read, write, scan, watch or take locks -> [1](01-getting-started.md)-[5](05-client-setup.md) |
| **Node API** | you want a cluster *member* inside your own service or your own launcher -> [6](06-embedding-a-node.md)-[7](07-custom-starter.md) |

Not here: the agent's HTTP/JSON surface, for callers that are not on the JVM -- that is
[docs/agent](../agent/README.md). Nothing in this set
is about operating a cluster; for *why* something behaves the way it does, the
[Developer Guide](../dev/README.md) has the reasoning.

## Contents

| | |
|---|---|
| [1. Getting started](01-getting-started.md) | depend on it, build a client, first read and write |
| [2. Keys and values](02-key-value.md) | `get`, `put`, `cas`, `delete`, versions -- and what each failure means |
| [3. Scan and watch](03-scan-and-watch.md) | enumerating keys, following changes, and what neither promises |
| [4. Locks](04-locks.md) | leases, fencing tokens, and the recovery every caller needs |
| [5. Client setup](05-client-setup.md) | transports, timeouts, authentication, observability, lifecycle |
| [6. Embedding a node](06-embedding-a-node.md) | run a cluster member inside your process |
| [7. Writing your own starter](07-custom-starter.md) | what a production launcher has to wire |

## Three things to know first

**Everything is asynchronous.** Every operation returns a `CompletableFuture` and nothing blocks.
Completions run on the client's event loop, so a stage attached with a non-async `then*` must not
block it -- hop with `thenApplyAsync(fn, yourExecutor)` if you need to.

**One compare-and-set, and it fences on a version.** There is no value-compared CAS. This is the
single most important thing about writing to discas safely, and [2](02-key-value.md) is mostly
about it.

**"Failed" is not one thing.** A write that failed either provably did not happen, or has an unknown
outcome. Only one error code means the second, and the difference decides whether retrying is free
or dangerous -- see [the decision table](02-key-value.md#what-to-do-with-a-failure).
