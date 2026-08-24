# 6. Monitoring and logs

discas exposes three things worth watching, and they answer three different questions. `/health`
answers *does this process need restarting?* `/ready` answers *should traffic come here?* And the
**operator-attention** metric family answers *is a person owed something right now, and what?*

That third one is the important design decision on this page. Every condition that needs a person is
a **named state**, and the name appears in three places -- the log line, a metric sample, and a row
in this handbook -- all derived from the same source, so they cannot drift apart. **A state with no
action does not exist.** A bad thing nobody can act on is a rate, and rates live in the counters.

The practical consequence: **one alert rule covers everything.**

```
discas_operator_attention > 0
```

It needs no `for:` clause. Each state carries its own *normal for* window and says nothing until it
has been true that long -- which is why a rolling restart is silent, and a member that is genuinely
gone is not. There is no warning level, deliberately: a third level would mean "act, but not much",
which no alert rule can use, and it would put the judgement in the tone of a log line rather than in
the process that knows how long the condition has been true.

---

## How to check

```sh
curl -s localhost:9600/health    # liveness
curl -s localhost:9600/ready     # readiness
curl -s localhost:9600/metrics   # Prometheus/OpenMetrics text
```

The observability endpoints bind to **`127.0.0.1:9600` by default** -- loopback, because they expose
peer identities and topology. `--observability-bind`, `--observability-workers` and
`--observability-enabled` configure them, each with a `DISCAS_*` variable. The agent carries the
same three flags for its own `/metrics`, defaulting to `127.0.0.1:9601` so the two do not collide on
one host.

### `/health` and `/ready`

| | 200 when | Point at it |
|---|---|---|
| `/health` | the node is past replay and its WAL is intact | a **liveness** probe, a supervisor's restart policy |
| `/ready` | *additionally* serving and quorum-connected | a **load balancer**, a readiness probe |

**The split is not decoration.** Point a Kubernetes `livenessProbe` at a quorum-aware endpoint and a
network partition stops being a partition and becomes a cluster-wide crashloop -- every node killed
exactly when it could still have served stale reads and rejoined. `/health` is therefore local-only.

Both carry `"state"`, the node's position in the start model
([2. The node](02-node.md#how-to-check)). A node in `AWAITING_FLOOR` answers `/health` 200 on
purpose: it is healthy and waiting on its peers, and restarting it only starts the wait again.

A node reporting 503 on `/ready` can still serve `ReadConsistency.SERIALIZABLE` reads from its local
committed state. The response body carries `canServeStaleReads`, so stale-tolerant traffic can be
routed on it.

---

## Operator attention

A raised state becomes **due** when it has been true for its window, and only then is a line written
and a sample exposed:

```
ERROR [STORAGE/WAL_DEGRADED] the WAL cannot append: No space left on device.
      Action: clear the device under --wal-dir and restart this node; /health already answers 503. ...
```
```
discas_operator_attention{group="STORAGE",state="WAL_DEGRADED",scope=""} 1
```

**The full action is on the log line**, generated from the same text as the row in
[14. Reference](14-reference.md), so it cannot fall behind. `scope` names which peer, file or
component it is about, and is empty for a process-wide state.

Some states **latch**: nothing in the process can observe them being dealt with, so they stay raised
until it restarts. That is deliberate -- the alternative is a surface that reports a fault has gone
away because nothing mentioned it again.

### The six groups

The group answers *whose it is and what class of action*, not which subsystem produced it. Node and
agent share one taxonomy, so the agent's states are in the same family rather than a page of their
own.

| Group | Whose it is | Covered in |
|---|---|---|
| `STORAGE` | this node's durable state, or another member's -- a disk, a directory, a wipe-and-rejoin | [2](02-node.md#operator-attention-states-raised-here), [3](03-cluster.md#a-copied-directory) |
| `MEMBERSHIP` | who is in the cluster and on what terms -- the member list, `N`, identities, versions | [3](03-cluster.md#membership-states) |
| `PEER` | another member's reachability -- bring it back, or decide to run short | [3](03-cluster.md#peer-states) |
| `CONFIG` | this process's configuration and the material it loads -- a file, a certificate, NTP | [5](05-access.md#troubleshooting) |
| `CAPACITY` | the workload against what the process is configured to sustain -- a budget or a tuning decision, not a fault | [4](04-quorum.md#capacity) |
| `INTERNAL` | the software -- collect the trace and report it | [12](12-troubleshooting.md) |

### Two things deliberately raise nothing

**Blocked tombstone collection.** Every cause of it already has a state on the member that owns the
action -- `PEER_DOWN` for a `silent` blocker, `WAL_DEGRADED` on a member answering `retained` -- so a
state here would alert the node that cannot act. Its gauges are the diagnostic.

**A peer still replaying** (`NOT_REPLAYED`). Transient, self-clearing, and it fires on every rolling
restart.

---

## Metrics

Prometheus/OpenMetrics text is the **pull-based OpenTelemetry path**: the OTel Collector ingests it
natively through its `prometheus` receiver, and Prometheus or Grafana Alloy scrape it directly --
with no runtime dependency added to discas, which has none.

```
discas_node_rounds_committed_total 3
discas_node_peers_handshaked 2
discas_node_quorum_available 1
discas_node_peer_handshaked{peer="n2"} 1
discas_node_peer_state_changed_seconds{peer="n2"} 1786584979
discas_node_peer_transitions_total{peer="n2",direction="up"} 1
```

**Keys are never labels** -- per-key series would be unbounded cardinality, and key detail stays in
the log. Per-peer labels are safe because `N` is frozen at startup.
[14. Reference](14-reference.md#metrics) lists every metric.

### What to watch

| Question | Signal |
|---|---|
| Is a person owed something? | `discas_operator_attention > 0` -- the one rule |
| Can this node serve writes? | `discas_node_quorum_available`, `/ready` |
| Are members flapping? | `discas_node_peer_transitions_total{direction="down"}` rate |
| Is durability degrading? | `discas_node_wal_degradations_total`, `discas_node_snapshots_failed_total` |
| Is the store filling? | `STORE_FULL` via the attention rule; `discas_node_writes_refused_no_capacity_total` |
| Is reclamation stuck? | `discas_node_tombstone_collection_blocked_seconds` -- alert at a **day**, never at a sweep |
| Is material about to expire? | `discas_reload_material_expires_seconds` |
| Did my config edit take? | `discas_reloads_total` vs `discas_reload_failures_total` |
| Is the software misbehaving? | `discas_node_event_loop_task_failures_total` and the `INTERNAL` state |
| Did a member lose its volume? | `discas_node_state_awaiting_floor_total` rising on a member that should have had one -- [2. The node](02-node.md#on-an-orchestrated-platform) |

Host-level CPU, memory and disk are not exposed here and are your platform's job. Two of them do
have discas-specific meaning: **heap** bounds the store (`--store-heap-fraction`, see
[4. Quorum](04-quorum.md#capacity)) and **disk under `--wal-dir`** filling up is what produces
`WAL_DEGRADED`.

---

## Logs

`Log` is two levels and two streams -- **INFO -> stdout, ERROR -> stderr** -- the split container
runtimes and supervisors expect. No hierarchy, no config file, no dependency, and nothing to
configure.

```
2026-08-11 13:36:19.712 INFO  [n1] recovery complete, keys=0
```

**Every `error` line is an operator-attention state**, and therefore every one names an action.
That is the whole contract: if it is on stderr, somebody has to do something, and the line says
what. Anything that does not need a person is INFO or a counter.

Do not build alerting on log text. The states are the stable surface; the wording is not.

---

## Impact

| Action | Effect |
|---|---|
| `--observability-enabled false` | no endpoints at all. The node still runs; you are blind |
| Binding observability to a routable address | exposes peer identities and topology. Bind it to loopback and scrape locally |
| Pointing a liveness probe at `/ready` | turns a partition into a cluster-wide crashloop. Never do this |
| Alerting on `discas_node_tombstones` | noise. It is a workload shape, not a fault |

---

## Related

- [4. Quorum and durability](04-quorum.md) -- what the quorum and capacity metrics mean
- [12. Troubleshooting](12-troubleshooting.md) -- symptom to page
- [14. Reference](14-reference.md) -- every metric and every state, in full
- Runbooks: [RB-01 check cluster health](13-runbooks.md#rb-01-check-cluster-health)
