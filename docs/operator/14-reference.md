# 14. Reference

Lookup, not explanation. Every table here is derived from the source; where a page in this handbook
explains a row, it is linked. **`--help` on the program itself is authoritative** for flags and
defaults -- this page lists them so they can be found, not so they can be maintained twice.

---

## Ports

discas reserves no well-known port numbers. Every one below is a value you chose; the defaults given
are the ones the software supplies.

| Port | Set by | Default | Carries | Expose to |
|---|---|---|---|---|
| Peer | `--peer-bind` | this node's entry in the member list | consensus, anti-entropy, handshakes | members only |
| Client | `--client-bind` | **required** | client and agent requests | your applications |
| Node observability | `--observability-bind` | `127.0.0.1:9600` | `/metrics`, `/health`, `/ready` | loopback |
| Agent HTTP | `--http-bind` | -- | `/v1/kv`, `/v1/lock`, `/v1/agent/health` | local callers |
| Agent observability | `--observability-bind` | `127.0.0.1:9601` | `/metrics` | loopback |

---

## Invocation

Two equivalent forms for every program. The start scripts come from the build's distributions; the
`java -cp` form needs nothing but the jars.

| Program | Start script | Main class |
|---|---|---|
| node | `discas-node` | `io.github.green4j.discas.node.starter.DisCasNodeStarter` |
| agent | `discas-agent` | `io.github.green4j.discas.agent.starter.DisCasAgentStarter` |
| admin | `discas-admin` | `io.github.green4j.discas.admin.starter.DisCasAdminStarter` |

```sh
./gradlew :discas-node:installDist :discas-agent:installDist :discas-admin:installDist
#   discas-node/build/install/discas-node/bin/discas-node
#   discas-agent/build/install/discas-agent/bin/discas-agent
#   discas-admin/build/install/discas-admin/bin/discas-admin

CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)
java -cp "$CP" io.github.green4j.discas.node.starter.DisCasNodeStarter    <args>
java -cp "$CP" io.github.green4j.discas.agent.starter.DisCasAgentStarter  <args>
java -cp "$CP" io.github.green4j.discas.admin.starter.DisCasAdminStarter  <args>
```

Arguments, `DISCAS_*` variables and behaviour are identical either way.

-> [1. discas in context](01-context.md#two-ways-to-invoke-each-of-them)

---

## Endpoints

| Endpoint | Program | Purpose |
|---|---|---|
| `GET /health` | node | **liveness** -- 200 past replay with an intact WAL. Point a restart policy here |
| `GET /ready` | node | **readiness** -- 200 while additionally serving and quorum-connected. Point a load balancer here |
| `GET /metrics` | node, agent | Prometheus/OpenMetrics text exposition |
| `GET /v1/agent/health` | agent | agent liveness only -- 200 while the process is up, even with no quorum |
| `/v1/kv`, `/v1/lock` | agent | the data plane -- see the [agent manual](../agent/README.md) |

---

## Environment variables

Every option of every program is settable by a `DISCAS_*` variable, and the name is **derived
mechanically** from the long flag: upper-snake, `-` becomes `_`.

```
--wal-dir            ->  DISCAS_WAL_DIR
--client-token-file  ->  DISCAS_CLIENT_TOKEN_FILE
--observability-bind ->  DISCAS_OBSERVABILITY_BIND
```

Precedence is `CLI > ENV > DEFAULT`, decided independently per value. There is no separate table of
variables to consult, and no variable that has no flag. **Passwords belong here**, not on a command
line.

---

## `discas-node` flags

`discas-node --help` for defaults and descriptions.

| Group | Flags |
|---|---|
| **Identity** | `--node-id` (`-n`), `--cluster-id` (`-c`), `--cluster-size` |
| **Membership** | `--members-file`, `--members` |
| **Transport** | `--peer-bind`, `--client-bind` |
| **Storage** | `--wal-dir`, `--wal-max-file-bytes`, `--snapshot-retention` |
| **Runtime** | `--repair-interval-seconds`, `--round-timeout-ms`, `--round-max-retries`, `--proposal-expiry-ms`, `--round-retry-backoff-ms`, `--round-retry-jitter-ms`, `--no-quorum-backoff-ms`, `--peer-response-timeout-ms`, `--tombstone-sweep-interval-seconds`, `--snapshot-interval-seconds`, `--promise-eviction-interval-seconds`, `--wal-force-interval-ms`, `--store-heap-fraction`, `--shutdown-await-timeout-ms` |
| **Observability** | `--observability-enabled`, `--observability-bind`, `--observability-workers`, `--peer-max-frame-bytes`, `--peer-max-queued-out-bytes`, `--peer-max-rx-buffer-bytes`, `--peer-max-inflight-bytes`, `--peer-max-connections`, `--peer-force-reconnect`, `--peer-reconnect-backoff-ms`, `--peer-reconnect-backoff-cap-ms`, `--client-max-frame-bytes`, `--client-max-queued-out-bytes`, `--client-max-rx-buffer-bytes`, `--client-max-inflight-bytes`, `--client-max-connections` |
| **Security (TLS/mTLS)** | `--tls`, `--tls-keystore`, `--tls-keystore-password`, `--tls-truststore`, `--tls-truststore-password`, `--tls-cert-rotation` |
| **Security (client access)** | `--client-auth`, `--client-token-file`, `--client-token-dir`, `--client-acl-file`, `--client-tls`, `--client-tls-keystore`, `--client-tls-keystore-password`, `--client-tls-truststore`, `--client-tls-truststore-password`, `--client-tls-cert-rotation` |

Defaults worth knowing without opening `--help`, because they appear in procedures:

| Flag | Default |
|---|---|
| `--round-timeout-ms` | 5000 |
| `--round-max-retries` | 3 |
| `--proposal-expiry-ms` | 30000 |
| `--peer-response-timeout-ms` | 10000 |
| `--no-quorum-backoff-ms` | 2000 |
| `--repair-interval-seconds` | 600 |
| `--tombstone-sweep-interval-seconds` | 10 |
| `--promise-eviction-interval-seconds` | 300 |
| `--snapshot-interval-seconds` | 3600 |
| `--snapshot-retention` | 2 |
| `--wal-force-interval-ms` | 1000 |
| `--shutdown-await-timeout-ms` | 5000 |
| `--peer-force-reconnect` | `false` |
| `--client-auth` | `allowall` |

-> [7. Configuration](07-configuration.md)

---

## `discas-agent` flags

| Group | Flags |
|---|---|
| **Cluster connection** | `--nodes` (`-N`), `--nodes-file`, `--client-id` (`-i`), `--token` |
| **HTTP front end** | `--http-bind` (`-b`), `--http-workers` |
| **Observability** | `--observability-enabled`, `--observability-bind`, `--observability-workers` |
| **Runtime** | `--request-timeout-seconds`, `--client-max-frame-bytes`, `--client-max-queued-out-bytes`, `--client-max-rx-buffer-bytes`, `--client-max-inflight-bytes`, `--client-max-connections` |
| **Security (TLS/mTLS)** | `--tls`, `--tls-keystore`, `--tls-keystore-password`, `--tls-truststore`, `--tls-truststore-password`, `--tls-cert-rotation` |

-> [11. The HTTP agent](11-http-agent.md)

---

## `discas-admin` commands

| Command | What it does |
|---|---|
| `dump` | write a cluster's live pairs to a dump file |
| `load` | write a dump's pairs into a running cluster, overwriting duplicates |
| `init` | build a new cluster's on-disk state and its instructions, optionally from a dump |
| `token` | mint a client token and print the record a token file needs |

| Command | Options |
|---|---|
| `dump` | `--nodes` (`-N`), `--client-id` (`-c`), `--token`, `--out` (`-o`), `--connect-timeout-ms`; positional arguments are key prefixes |
| `load` | `--nodes` (`-N`), `--client-id` (`-c`), `--token`, `--in` (`-i`), `--cleanup`; positional arguments are cleanup prefixes |
| `init` | `--out-dir` (`-d`), `--cluster-id` (`-c`), `--members` (`-m`), `--in` (`-i`), `--client-port`, `--data-dir`, `--config-dir`, `--client-auth`, `--client-token-file`, `--client-acl-file`, `--tls`, `--client-tls` |
| `token` | `--client-id` (`-c`), `--ttl-days`, `--token`, `--iterations`. The record goes to stdout, everything else to stderr |

Exit codes: **0** success, **1** it did not work, with one line saying what, **2** the command line
itself was wrong, with the usage printed.

-> [8. discas-admin](08-admin.md)

---

## Node states

The start model, reported in the `"state"` field of `/health` and `/ready`.

| State | Meaning | `/health` | `/ready` |
|---|---|---|---|
| `REPLAYING` | reading its own snapshot and WAL tail | 503 | 503 |
| `AWAITING_FLOOR` | started with no state; asking the cluster for its promise floor | **200** | 503 |
| `SERVING` | in the quorum | 200 | 200 with quorum |
| `CLOSING` | draining | 503 | 503 |
| `FAILED` | stopped by something it cannot continue past | 503 | 503 |

Counted as `discas_node_state_replaying_total`, `_awaiting_floor_total`, `_serving_total`,
`_closing_total`, `_failed_total`.

-> [2. The node](02-node.md#how-to-check)

---

## Operator states

Every condition that needs a person. The alert rule is `discas_operator_attention > 0` and needs no
`for:` clause -- the *normal for* column is each state's own window, below which it says nothing.

**The full action is on the log line**, generated from the same text; the column here is its first
clause.

| State | Group | Normal for | Action |
|---|---|---|---|
| `STORAGE_UNREADABLE` | `STORAGE` | -- | Delete what is broken under `--wal-dir` and restart -- the log, or the whole directory |
| `SNAPSHOT_FAILING` | `STORAGE` | -- | Check free space and permissions on `--wal-dir` |
| `WAL_DEGRADED` | `STORAGE` | -- | Clear the device under `--wal-dir` and restart this node |
| `SHUTDOWN_INCOMPLETE` | `STORAGE` | -- | Confirm the process has exited and nothing still holds `--wal-dir` before starting it again |
| `UNACCOUNTED_KEYS_DROPPED` | `STORAGE` | -- | This member came back holding state nobody could confirm, and it has been dropped rather than repaired outward |
| `STORAGE_CLONED` | `STORAGE` | -- | One member's `--wal-dir` has been copied onto another |
| `PEER_STORAGE_ROLLED_BACK` | `STORAGE` | -- | Stop that member now -- it is not behind, it is authoritatively pushing stale state, and every minute it runs spreads more of it |
| `FLOOR_UNAVAILABLE` | `MEMBERSHIP` | 2 minute | Bring members back: this node needs enough of them to bound the promises it has forgotten |
| `MEMBERS_REJECTED` | `MEMBERSHIP` | -- | Fix `--members-file`: it must define exactly `--cluster-size` members and include this node |
| `VERSION_MISMATCH` | `MEMBERSHIP` | -- | The two ends run incompatible builds |
| `FOREIGN_CLUSTER` | `MEMBERSHIP` | -- | That node belongs to another cluster |
| `IDENTITY_MISMATCH` | `MEMBERSHIP` | -- | The member list and the peer's identity disagree |
| `UNKNOWN_MEMBER` | `MEMBERSHIP` | -- | That node is not in `--members-file` here |
| `CLUSTER_SIZE_MISMATCH` | `MEMBERSHIP` | -- | Make `--cluster-size` agree on both ends |
| `CLUSTER_SIZE_INVALID` | `MEMBERSHIP` | -- | That node reports a cluster size this client cannot work with |
| `QUORUM_LOST` | `PEER` | -- | Restore members until a majority is reachable from here |
| `PEER_DOWN` | `PEER` | 1 minute | Bring that member back or replace it (`discas_node_peer_handshaked`, and its own `/health`) |
| `NODE_UNREACHABLE` | `PEER` | 1 minute | Check that node and the path to it |
| `CLOCK_SKEW` | `CONFIG` | -- | Run NTP on both nodes |
| `CLOCK_UNUSABLE` | `CONFIG` | -- | Fix NTP on that node: its clock is more than five minutes from ours, which the handshake's replay guard refuses |
| `ACCESS_DENIED` | `CONFIG` | -- | This process's credentials are not accepted by that node |
| `RELOAD_FAILED` | `CONFIG` | -- | Fix the file |
| `RELOAD_NOT_WATCHED` | `CONFIG` | -- | Check the host's filesystem watch limits (inotify on Linux) |
| `MATERIAL_EXPIRING` | `CONFIG` | -- | Rotate this material before it expires |
| `STORE_FULL` | `CAPACITY` | -- | Give this node more heap, or raise `--store-heap-fraction`, and restart it |
| `UNHANDLED_ERROR` | `INTERNAL` | -- | Collect the stack trace and report it |

-> [6. Monitoring](06-monitoring.md#operator-attention), [12. Troubleshooting](12-troubleshooting.md)

---

## Metrics

Prometheus/OpenMetrics exposition on `/metrics`. Keys are never labels; per-peer labels are safe
because `N` is frozen at startup.

`discas_operator_attention` is not in this table because it is a family rather than a single
sample: one gauge per raised-and-due state, labelled `group`, `state` and `scope`.

| Metric | Type | Meaning |
|---|---|---|
| `discas_client_cluster_size_disagreements_total` | counter | Servers reporting a cluster size different from the one already known. |
| `discas_client_cluster_size_mismatches_total` | counter | Times the configured node count differed from the cluster's reported size. |
| `discas_client_connections_lost_total` | counter | Connections to a node that dropped unexpectedly, taking requests with them. |
| `discas_client_event_loop_task_failures_total` | counter | Tasks that threw on the client event loop. |
| `discas_client_handshakes_completed_total` | counter | CLIENT_HELLO handshakes a node accepted; a connection becoming usable. |
| `discas_client_hellos_rejected_total` | counter | CLIENT_HELLO handshakes a server refused. |
| `discas_client_invalid_cluster_sizes_total` | counter | Servers that answered the handshake with a cluster size below 1. |
| `discas_client_requests_failed_over_total` | counter | Requests re-dispatched to another peer after a server error. |
| `discas_client_scans_incomplete_total` | counter | ANY_AVAILABLE scans that returned below a majority. |
| `discas_client_send_failures_total` | counter | Request dispatches that threw, leaving that peer unable to answer. |
| `discas_client_transport_accounting_underflows_total` | counter | Times the transport's estimated-bytes accounting went negative and was clamped. |
| `discas_node_accept_handler_failures_total` | counter | Accept handlers that threw. |
| `discas_node_accepts_refused_no_capacity_total` | counter | Accepts this node refused for want of room; the guard that keeps the JVM alive. |
| `discas_node_accepts_rejected_total` | counter | Accept messages rejected because a higher ballot was already promised. |
| `discas_node_ceiling_request_failures_total` | counter | Ceiling requests that could not be sent, or answers that could not be returned. |
| `discas_node_cluster_size` | gauge | N, the frozen quorum basis. |
| `discas_node_digest_request_failures_total` | counter | Anti-entropy digest requests that failed. |
| `discas_node_event_loop_task_failures_total` | counter | Tasks that threw on the node event loop. |
| `discas_node_external_ballots_observed_total` | counter | Times a peer's ballot counter exceeded ours and was adopted. |
| `discas_node_keys_repaired_total` | counter | Keys reconciled by anti-entropy. |
| `discas_node_keys_request_failures_total` | counter | Anti-entropy key-page requests that failed. |
| `discas_node_members_reloads_accepted_total` | counter | Membership reloads applied; the pair to the refusals above, and what ends one. |
| `discas_node_members_reloads_rejected_total` | counter | Membership reloads refused, e.g. one that would change the frozen cluster size. |
| `discas_node_peer_clock_skew_ms` | gauge | Peer wall clock minus ours at the last handshake, in milliseconds. |
| `discas_node_peer_disconnects_total` | counter | Handshaked peers whose connection went away. |
| `discas_node_peer_handshaked` | gauge | 1 when the peer's PEER_HELLO handshake has completed and the connection is live; TCP connectivity alone does not count. |
| `discas_node_peer_handshakes_completed_total` | counter | PEER_HELLO handshakes that completed; a peer becoming usable for consensus. |
| `discas_node_peer_handshakes_rejected_total` | counter | PEER_HELLO handshakes rejected by either side. |
| `discas_node_peer_incarnation_changes_total` | counter | Members that reappeared with replaced storage; each recovers its own floor. |
| `discas_node_peer_state_changed_seconds` | gauge | Unix seconds of the peer's last handshake/disconnect transition. |
| `discas_node_peer_transitions_total` | counter | Peer handshake/disconnect transitions, by direction. |
| `discas_node_peers_handshaked` | gauge | Peers whose PEER_HELLO handshake has completed and not since dropped. |
| `discas_node_prepare_handler_failures_total` | counter | Prepare handlers that threw. |
| `discas_node_prepares_rejected_total` | counter | Prepare messages rejected because a higher ballot was already promised. |
| `discas_node_quorum_available` | gauge | 1 when this node plus its handshaked peers can form a quorum, else 0. |
| `discas_node_repair_cycles_total` | counter | Anti-entropy repair cycles started. |
| `discas_node_requests_before_ready_total` | counter | Requests refused because the node had not finished recovery. |
| `discas_node_rounds_committed_total` | counter | CASPaxos rounds that reached a committing quorum. |
| `discas_node_rounds_failed_total` | counter | CASPaxos rounds that failed or timed out. |
| `discas_node_rounds_refused_no_majority_total` | counter | Linearizable operations refused up front because this node cannot reach a majority; no round was run. |
| `discas_node_scan_failures_total` | counter | Scan pages this node could not serve because serving one threw. |
| `discas_node_serializable_reads_total` | counter | Reads answered from local committed state with no consensus round. |
| `discas_node_snapshots_completed_total` | counter | Snapshot writes that completed. |
| `discas_node_snapshots_failed_total` | counter | Snapshot writes that failed. |
| `discas_node_snapshots_started_total` | counter | Snapshot writes started. |
| `discas_node_tombstone_collection_blocked_by` | gauge | Members that stopped the last tombstone sweep, and what they answered. |
| `discas_node_tombstone_collection_blocked_seconds` | gauge | How long tombstones have waited with none collected; zero when there are none. Alert at a day, never at a sweep. |
| `discas_node_tombstones` | gauge | Keys held as tombstones: key space that cannot shrink until they are collected. |
| `discas_node_tombstones_collected_total` | counter | Tombstones collected: every member agreed the value could no longer be resurrected. |
| `discas_node_unaccounted_keys_dropped_total` | counter | Keys dropped by a node that could not account for its own history and whose peers did not hold them. |
| `discas_node_wal_degradations_total` | counter | Times the WAL was marked degraded. |
| `discas_node_writes_refused_no_capacity_total` | counter | Client writes this coordinator refused before running a round, for want of room. |
| `discas_reload_check_failures_total` | counter | Scheduled reload checks that threw. |
| `discas_reload_failures_total` | counter | Background reloads that failed; the last good value was retained. |
| `discas_reload_material_expires_seconds` | gauge | Unix seconds at which watched material expires; 0 when nothing has warned. |
| `discas_reload_material_expiring_total` | counter | Times watched material was reported as expiring with no replacement. |
| `discas_reload_watch_unavailable_total` | counter | Times a filesystem watch could not be established, degrading to the safety poll. |
| `discas_reloads_total` | counter | Background reloads that succeeded. |

Node lifecycle counters, one per state, follow the pattern `discas_node_state_<state>_total`.

-> [6. Monitoring](06-monitoring.md#metrics)

---

## Client error codes

What an application sees on a failure. Applications branch on these, never on the error text.

| Code | Applied? | HTTP | Client behaviour |
|---|---|---|---|
| `NOT_READY` | no | 503 | hidden failover |
| `NO_QUORUM_AT_COORDINATOR` | no | 503 | hidden failover |
| `BALLOT_LOST` | no | 503 | surfaced |
| `PROPOSAL_EXPIRED` | no | 503 | surfaced |
| `UNAVAILABLE` | **unknown** | 503 | surfaced; a fenced write may simply be re-sent |
| `INVALID_ARGUMENT` | no | 400 | surfaced |
| `STORE_FULL` | no | 507 | surfaced |
| `ACCESS_DENIED` | no | 403 | surfaced |
| `INTERNAL` | unknown | 500 | fenced writes fail over |

Every one is determinate -- the operation did not happen -- **except `UNAVAILABLE`** and a client-side
deadline.

-> [12. Troubleshooting](12-troubleshooting.md#what-your-callers-see)

---

## On-disk layout

```
<--wal-dir>/
  incarnation      the marker: which run of this member's storage this is
  wal/             log segments
  snap/            snapshots
```

A `discas-admin init` output folder:

```
<--out-dir>/
  RUN.md           generated instructions for this cluster
  members.conf     the membership, in the format --members-file reads
  1/ 2/ 3/         one data directory per member, each with its own incarnation
```

---

## Glossary

| Term | Means |
|---|---|
| **anti-entropy** | the background reconciliation that brings replicas back into agreement. Restores *values*, never promises |
| **ballot** | the logical, monotonically increasing number ordering proposals on a key. Not a timestamp |
| **CASPaxos** | the consensus algorithm. Agrees on one register at a time; there is no log and no leader |
| **cluster id** | the name of the deployment. Members presenting a different one are refused |
| **coordinator** | whichever node is running a given round. Any member can be one; there is no leader |
| **fencing token** | what makes a lock safe. A lease is a deadline and can be outlived; the token orders access at the protected resource |
| **incarnation id** | names *which run of a member's storage* this is. Lives in the data directory and is copied with it |
| **member** | one of the `N` seats, named by its `node_id` |
| **`N`** | the cluster size and the quorum basis, `--cluster-size`. Frozen at startup |
| **node id** | the name of a seat. A role, not a machine -- it outlives the hardware |
| **promise ceiling** | the highest ballot a member has reserved the right to promise or issue. What it must never go back on |
| **promise floor** | the bound a member with no state adopts from the cluster before it may vote |
| **quorum** | a majority, `N/2 + 1`. Any two intersect, which is why a committed value is never lost |
| **scope** | on an operator state: which peer, file or component it is about. Empty means process-wide |
| **tombstone** | a deleted key's marker, kept until every member agrees the value cannot be resurrected |
| **witness** | a member that answers a floor request. A starting-empty node needs `N - quorum + 1` of them |

---

## Related

- [1. discas in context](01-context.md) -- the model these terms belong to
- [12. Troubleshooting](12-troubleshooting.md) -- symptom to page
- [13. Runbooks](13-runbooks.md) -- the procedures
