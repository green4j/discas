# The agent HTTP API

`discas-agent` is a local sidecar: one long-lived client to the cluster, fronted by an HTTP/JSON
surface on `127.0.0.1:8500`. It exists so a caller that is not on the JVM can use discas with
nothing but `curl`.

Four route prefixes, and that is the whole surface:

| | |
|---|---|
| `/v1/kv/{key}` | read, write, compare-and-set, delete, list, watch -> [2](02-kv.md), [3](03-watch.md) |
| `/v1/lock/{key}` | acquire, renew, release, inspect -> [4](04-locks.md) |
| `/v1/agent/health` | is this **agent process** alive -> [5](05-running.md#health) |
| `/v1/agent/reload` | re-read the nodes file and TLS material -> [5](05-running.md#finding-the-cluster) |

## Contents

| | |
|---|---|
| [1. Getting started](01-getting-started.md) | an agent running, and your first write and read |
| [2. Keys and values](02-kv.md) | `GET`/`PUT`/`DELETE`, `?raw`, `?keys`, `?stale`, and `?cas` |
| [3. Blocking queries](03-watch.md) | `?version=`/`?wait=` and the `X-DisCas-Version` header |
| [4. Locks](04-locks.md) | leases, the token header, and the fencing generation |
| [5. Running an agent](05-running.md) | the flags a caller needs, credentials, metrics |
| [6. Errors](06-errors.md) | every status this API returns, and which one is dangerous |

## Three things to know before the first call

**The agent is not a server, it is a client with a socket.** It holds no data. Every request becomes
a consensus operation against the cluster, so a `PUT` here costs what a `PUT` costs anywhere in
discas, and an agent that answers says nothing about whether the cluster can.

**One compare-and-set, and it fences on a version, never on a value.** `?cas=<version>` is the only
conditional write. [2](02-kv.md) is mostly about why that is the shape and how to use it.

**`503` is the status to read carefully.** It covers five cluster conditions, four of which mean the
write provably did not happen and one of which means nobody knows. Which one you are in depends on
whether you fenced the write -- [6](06-errors.md) is the table.

Everything here is a thin skin over the Java client. When a guarantee needs explaining rather than
stating, [the client documentation](../user/README.md) has the reasoning, and
[the developer guide](../dev/09-agent-and-admin.md) has the implementation.
