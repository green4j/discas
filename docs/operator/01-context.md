# 1. discas in context

discas is a small, strongly consistent **key/value store** built on CASPaxos. An operator's mental
model can be short, because the system is: `N` identical members, each holding every key, agreeing
one register at a time by majority.

Almost everything on the following pages follows from three properties. **There is no leader** --
every member can coordinate a write, so there is no election to lose and no "recover the leader"
procedure. **There is no log** -- CASPaxos agrees on one register at a time, so there is no global
revision, no ordering between keys, and no follower that can be "behind" by a log index. And **`N`
is frozen while the cluster runs** -- quorum is `N/2 + 1`, two members that disagree about `N` refuse
to connect, and resizing is a planned operation with an outage rather than a live reconfiguration.

There is also **no failure detector**. Nothing decides that a member is gone; readiness is derived
from completed handshakes, and a node that cannot see its peers waits for as long as that lasts. The
judgement *"this member is not coming back"* is always yours.

> ### If you are coming from Consul
>
> The word **agent** means the opposite thing. In Consul the agent *is* the cluster member; in
> discas the cluster member is **`discas-node`**, and **`discas-agent`** is an optional HTTP sidecar
> for callers that are not on the JVM. There is no service discovery, no catalog, no DNS interface
> and no gossip layer -- discas is a KV store, not a service mesh. Consul's `snapshot` has no exact
> analogue either: `discas-admin dump` walks the key space rather than capturing a Raft snapshot,
> and is not a point-in-time copy.

---

## The four programs

| | What it is | Who runs it |
|---|---|---|
| `discas-node` | a member of the cluster. Holds the data, takes part in consensus | you, `N` of them -- [2. The node](02-node.md) |
| `discas-agent` | an optional HTTP/JSON front end to the cluster, for non-JVM callers | you, usually one per host -- [11. The HTTP agent](11-http-agent.md) |
| `discas-admin` | a one-shot command: `dump`, `load`, `init` | you, by hand -- [8. discas-admin](08-admin.md) |
| `DisCasClient` | the Java client library, in the application's own JVM | your developers -- see [docs/user/](../user/README.md) |

### Two ways to invoke each of them

Every command in this handbook is written in its short form -- `discas-node`, `discas-agent`,
`discas-admin`. Those are the start scripts the build produces, and they are what `discas-admin
init` writes into a generated `RUN.md`:

```sh
./gradlew :discas-node:installDist :discas-agent:installDist :discas-admin:installDist

# put these on PATH:
#   discas-node/build/install/discas-node/bin/discas-node
#   discas-agent/build/install/discas-agent/bin/discas-agent
#   discas-admin/build/install/discas-admin/bin/discas-admin
```

**The scripts are a convenience, not a requirement.** Each is a thin wrapper around a main class, so
anywhere this guide shows `discas-node <args>` you may equally run:

```sh
CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)

java -cp "$CP" io.github.green4j.discas.node.starter.DisCasNodeStarter   <args>
java -cp "$CP" io.github.green4j.discas.agent.starter.DisCasAgentStarter <args>
java -cp "$CP" io.github.green4j.discas.admin.starter.DisCasAdminStarter <args>
```

The arguments, the `DISCAS_*` variables and the behaviour are identical -- the wrapper adds nothing
but the classpath. Use the `java -cp` form when you are running from a build tree, embedding discas
in an existing image, or need to set JVM options the wrapper does not expose. Later pages show the
short form and name this alternative once per command rather than repeating it.

A node can also be **embedded** directly in an application's JVM rather than run as a process. The
two shapes speak the same protocol and this handbook applies to both; where it says "start the
node", an embedded deployment starts a JVM. What embedding does not change is that the node is
**identity-bound**: it needs a stable `node_id`, a reachable address and its own durable volume, and
`N` is fixed. On Kubernetes that is a StatefulSet with a headless Service and a volume per pod -- not
a Deployment behind a horizontal autoscaler.

## Ports and who talks to whom

discas reserves no well-known port numbers; every one below is something you chose.

```
   client / agent ──────▶ client port ──┐
                                        │
                             ┌──────────▼──────────┐
                             │     discas-node     │
                             └───▲─────────────┬───┘
                       peer port │             │ observability port
                                 │             ▼
                        other members    Prometheus / probes
```

| Port | Set by | Carries | Exposure |
|---|---|---|---|
| Peer | `--peer-bind` | consensus, anti-entropy, handshakes | members only, all regions |
| Client | `--client-bind` (**required**) | requests from clients and agents | your applications |
| Observability | `--observability-bind`, default `127.0.0.1:9600` | `/metrics`, `/health`, `/ready` | **loopback** -- it exposes peer identities and topology |
| Agent HTTP | `--http-bind` on `discas-agent` | `/v1/kv`, `/v1/lock`, `/v1/agent/health` | local callers |
| Agent observability | default `127.0.0.1:9601` | `/metrics` | loopback; 9601 so it does not collide with a node on the same host |

The peer port and the client port are **separate trust domains** with their own CAs, stores and
authentication -- see [5. Access](05-access.md).

## What lives where

| | Path | Set by |
|---|---|---|
| Data | one directory per member: `incarnation`, `wal/`, `snap/` | `--wal-dir` |
| Membership | `members.conf` | `--members-file` |
| Client credentials | token file or directory, ACL file | `--client-token-file` / `--client-token-dir`, `--client-acl-file` |
| Key material | PKCS12 key and trust stores | `--tls-*`, `--client-tls-*` |

A member's data directory is its identity -- it holds the `incarnation` marker -- and belongs to one
`node_id` forever. Everything else in the table is configuration, and most of it is hot-reloaded.

## What discas does not do

Worth knowing before you look for it:

- **No online resize.** `N` is a design-time decision. [3. Cluster](03-cluster.md#changing-n).
- **No incremental backup, no point-in-time recovery.** There is no revision to diff against and no
  log to replay from, so every dump is a full one. [9. Backup](09-backup.md).
- **No restore of a member's directory.** A member is never brought back from a copy; the pairs go
  in through `load` instead. This is a safety property, not a gap.
- **No cross-key transactions.** One register at a time is the whole model.
- **No service discovery, health checking of your services, or DNS.** Different product.

---

## Where to go next

| You want to | Page |
|---|---|
| Stand up a cluster that does not exist yet | [8. discas-admin](08-admin.md#init) |
| Start, stop or repair one node | [2. The node](02-node.md) |
| Understand a member that dropped out | [3. Cluster and members](03-cluster.md) |
| Understand a write outage | [4. Quorum and durability](04-quorum.md) |
| Set up authentication and TLS | [5. Access](05-access.md) |
| Decide what to alert on | [6. Monitoring](06-monitoring.md) |
| Look up a flag, a metric or a state | [14. Reference](14-reference.md) |
| Follow a procedure under pressure | [13. Runbooks](13-runbooks.md) |
