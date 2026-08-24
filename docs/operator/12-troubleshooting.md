# 12. Troubleshooting

An index, not a second copy of the explanations. Find the symptom, follow the link -- the reasoning
lives on the topic page, and the steps live in [13. Runbooks](13-runbooks.md).

**Start here in an incident.** Two commands answer most of it:

```sh
# Is a person owed something, and what?
curl -s localhost:9600/metrics | grep discas_operator_attention

# Can this node serve writes?
curl -s localhost:9600/ready
```

Every raised state names its own action on the log line it wrote. If
`discas_operator_attention` is empty and `/ready` is 200 on a majority, the cluster is not the
problem.

---

## By symptom

### Starting and stopping

| Symptom | Go to |
|---|---|
| A node will not start at all | [2. The node -- the node will not start](02-node.md#the-node-will-not-start) |
| A node enters `FAILED` immediately | [2. The node](02-node.md#the-node-will-not-start) -- corrupt WAL or unreadable marker; [RB-08](13-runbooks.md#rb-08-a-node-that-will-not-start) |
| A node is stuck in `AWAITING_FLOOR` | [4. Quorum -- a node that starts with nothing](04-quorum.md#a-node-that-starts-with-nothing) |
| A node is stuck in `REPLAYING` | normal for a large key space; watch the INFO progress line. If it never ends, [2. The node](02-node.md#how-to-check) |
| The first node of a cold start never becomes ready | [10. Upgrades -- cold start](10-upgrade.md#cold-start-of-a-whole-cluster). Expected: start the rest |
| A node did not shut down cleanly (`SHUTDOWN_INCOMPLETE`) | [2. The node](02-node.md#stopping) |
| A setting has no effect | [7. Configuration](07-configuration.md#how-to-check) -- read the effective-configuration table |

### Membership and peers

| Symptom | Go to |
|---|---|
| A node started fine and nobody will talk to it | [3. Cluster -- membership states](03-cluster.md#membership-states). Check the handshake on the *peers* |
| A member dropped out (`PEER_DOWN`) | [3. Cluster -- peer states](03-cluster.md#peer-states) |
| Members flapping | `discas_node_peer_transitions_total{direction="down"}`; then [3. Cluster](03-cluster.md#how-to-check) |
| A member-file edit was ignored | [3. Cluster -- what a reload does](03-cluster.md#what-a-reload-does). Add/remove changes `N` and is always refused |
| `CLUSTER_SIZE_MISMATCH` | [3. Cluster](03-cluster.md#membership-states). Never resize `N` to fix quorum |
| `IDENTITY_MISMATCH` / `UNKNOWN_MEMBER` / `FOREIGN_CLUSTER` | [3. Cluster](03-cluster.md#membership-states) |
| `VERSION_MISMATCH` after an upgrade | [10. Upgrades](10-upgrade.md#changing-the-version) |
| Clock skew (`CLOCK_SKEW`, `CLOCK_UNUSABLE`) | [5. Access](05-access.md#troubleshooting). Run NTP; this bounds every lock lease |

### Quorum and capacity

| Symptom | Go to |
|---|---|
| Writes fail with `NO_QUORUM_AT_COORDINATOR` | [4. Quorum -- losing quorum](04-quorum.md#losing-quorum); [RB-07](13-runbooks.md#rb-07-recover-from-a-lost-quorum) |
| `/ready` 503 on one node while others serve | [2. The node -- up but serves nothing](02-node.md#the-node-is-up-but-serves-nothing) |
| Writes refused with `STORE_FULL` | [4. Quorum -- capacity](04-quorum.md#capacity); [RB-09](13-runbooks.md#rb-09-store_full) |
| A node stops during replay with `STORE_FULL` | [4. Quorum -- capacity](04-quorum.md#capacity). Never delete anything under `--wal-dir` |
| `WAL_DEGRADED` | [2. The node](02-node.md#operator-attention-states-raised-here). Clear the device and restart. Latches |
| Tombstones climbing | [4. Quorum -- tombstones climbing](04-quorum.md#tombstones-climbing). A workload shape, not a fault |
| Tombstone collection stopped | [4. Quorum -- collection stopped](04-quorum.md#collection-stopped). Read the `answer` label |
| Progress stalls on one hot key | [4. Quorum -- same-key contention](04-quorum.md#same-key-contention) |

### Storage and data

| Symptom | Go to |
|---|---|
| `STORAGE_UNREADABLE` | [2. The node -- recovery](02-node.md#recovery). Delete what is broken; never restore a copy |
| `SNAPSHOT_FAILING` | [2. The node](02-node.md#operator-attention-states-raised-here). Not fatal; replay time grows |
| `STORAGE_CLONED` or `PEER_STORAGE_ROLLED_BACK` | [3. Cluster -- a copied directory](03-cluster.md#a-copied-directory); [RB-15](13-runbooks.md#rb-15-a-node-returned-from-a-copied-directory) |
| `UNACCOUNTED_KEYS_DROPPED` | [3. Cluster](03-cluster.md#a-copied-directory). Latches; the only signal that state left a node |
| Deleted keys came back | [3. Cluster -- a copied directory](03-cluster.md#a-copied-directory). Stop that node first |
| A dump will not read | [8. discas-admin](08-admin.md#troubleshooting). Not repairable; take a fresh one |

### Access

| Symptom | Go to |
|---|---|
| A client gets `ACCESS_DENIED` | [5. Access](05-access.md#troubleshooting) |
| A token or ACL edit did nothing | [7. Configuration](07-configuration.md#reload-vs-restart); `RELOAD_FAILED` says the file is malformed |
| `MATERIAL_EXPIRING` | [5. Access](05-access.md#rotating-a-ca). Rotation is hot |
| A peer certificate expired | it presents as `PEER_DOWN` and `/ready` 503 -- [5. Access](05-access.md#troubleshooting) |
| `RELOAD_NOT_WATCHED` | [5. Access](05-access.md#troubleshooting). Check the host's inotify limits |
| An ACL is set but ignored | [5. Access](05-access.md) -- an ACL over `allowall` is advisory. Pair it with `token` or `mtls` |

### The agent

| Symptom | Go to |
|---|---|
| Every request fails but agent health says `ok` | [11. The agent](11-http-agent.md#how-to-check). Agent health is not cluster health |
| The agent reaches nobody | [11. The agent](11-http-agent.md#troubleshooting). Usually pointed at peer ports |
| `NODE_UNREACHABLE` | [11. The agent](11-http-agent.md#troubleshooting) |

### The software itself

| Symptom | Go to |
|---|---|
| `UNHANDLED_ERROR` | the event loop caught an exception and carried on; `scope` names the component. **Collect the stack trace and report it.** One state for every such site, because they have one action between them |
| `discas_node_event_loop_task_failures_total` climbing | same. This is a defect, not a configuration problem |

---

## What your callers see

Operators get asked about error codes their applications report. These are `ClientErrorCode` values,
carried on every failure response; applications should branch on them and never on the error text.
"Hidden" means the client re-dispatched to another coordinator without telling the caller.

| Code | Applied? | What the caller sees | Usually means |
|---|---|---|---|
| `NOT_READY` | no | hidden failover | a node is replaying. Not visible as an error unless every node is |
| `NO_QUORUM_AT_COORDINATOR` | no | hidden failover | that coordinator cannot see a majority; another may |
| `BALLOT_LOST` | no | surfaced | contention on the key. A property of the key, not of the node |
| `PROPOSAL_EXPIRED` | **no** | surfaced | the write outlived `--proposal-expiry-ms` before Accept. Definitely not applied |
| `UNAVAILABLE` | **unknown** | surfaced | the one indeterminate outcome. Bounded: it may still apply within `--proposal-expiry-ms` of when it was sent, and never afterwards |
| `STORE_FULL` | no | surfaced | [4. Quorum -- capacity](04-quorum.md#capacity). Every replica gives the same answer |
| `ACCESS_DENIED` | no | surfaced | [5. Access](05-access.md) |
| `INVALID_ARGUMENT` | no | surfaced | key or value over the limits. Fails identically everywhere |
| `INTERNAL` | unknown | surfaced | a defect. Collect it |

`cas` returning `swapped == false` is **not** a failure -- it is a result, carrying the version and
value that won, so the caller recomputes and retries without a second read.

The one thing worth telling an application team: **every failure above is determinate except
`UNAVAILABLE` and a client-side deadline.** For those two, a fenced write (`cas`, or `delete` with a
version) can simply be re-sent, because a duplicate provably cannot apply.

---

## Related

- [6. Monitoring](06-monitoring.md) -- the one alert rule, and what each signal means
- [13. Runbooks](13-runbooks.md) -- the procedures
- [14. Reference](14-reference.md) -- every state, metric and flag
