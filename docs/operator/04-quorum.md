# 4. Quorum and durability

This is the chapter that in a Raft handbook would be called *Raft*, and the differences matter to
you at three in the morning. **There is no leader**, so there is no election, no election timeout,
and no procedure called "recover the leader". There is **no log** -- CASPaxos agrees on one register
at a time -- so there is no global revision, no snapshot-plus-log-index to reason about, and no
follower that can be "behind". And there is **no failure detector**, so no timer's value decides
whether the cluster is healthy: a badly chosen timeout fails the request that hit it and cannot
destabilise the cluster or cost you a leader.

What remains is one number. A write completes when a **majority** -- `N/2 + 1` -- has accepted it, and
any two majorities share a member, which is the whole of why a committed value is never lost. `N` is
frozen at startup for exactly this reason: two members running different `N` could form two
majorities over one key, so they refuse to connect instead.

The one distinction to carry into every row below: **losing values is a durability problem and
quorum repairs it; losing *promises* is a safety problem and quorum cannot.** A promise was never
replicated, so no peer holds a copy. What a quorum *can* give back is the **bound** on those
promises, and the bound is the whole protection -- which is why a node that starts with nothing waits
for one instead of catching up the way a data store would.

---

## How to check

```sh
curl -s localhost:9600/metrics | grep -E 'quorum|rounds|wal|snapshot|store'
```

| Metric | Says |
|---|---|
| `discas_node_quorum_available` | 1 when this node plus its handshaked peers can form a majority |
| `discas_node_rounds_committed_total` | writes that reached agreement |
| `discas_node_rounds_refused_no_majority_total` | work shed because there was no quorum here |
| `discas_node_rounds_failed_total` | rounds that ran and did not commit |
| `discas_node_external_ballots_observed_total` | contention: other proposers duelling on a key |
| `discas_node_wal_degradations_total` | the WAL stopped being able to append |
| `discas_node_snapshots_completed_total` / `_failed_total` | whether replay time is bounded |
| `discas_node_keys_repaired_total` | anti-entropy doing its job -- a spike after a rejoin is normal |
| `discas_node_tombstones` / `_collected_total` | deleted keys awaiting reclamation, and the rate |

A cluster is healthy on this page when every member reports `discas_node_quorum_available 1` and the
`_refused_no_majority_total` counters are flat.

---

## The durability model

Three things hold a value, and only the first is authoritative:

| | What it is | If you lose it |
|---|---|---|
| `wal/` | the record. Every accepted value, appended in order | the node cannot vouch for its own history; it takes a floor from the cluster and gets values from anti-entropy |
| `snap/` | an optimisation over replaying the log | nothing but replay time |
| the quorum | the other members holding the same keys | see below -- this is the one that costs you |

**Durability rests on quorum between syncs, not on an fsync per write.** That is why
`--wal-force-interval-ms` (default 1 s) is a throughput-versus-exposure trade-off and not a
correctness switch: an unclean shutdown loses up to that much of the tail, the surviving log is
well-formed and simply short, and the promise ceiling covers the gap at a cost of one extra round
per key.

Every replica holds every key. There is no sharding, so a node is not "part of" the data -- it is a
copy of all of it. Two consequences an operator feels: a full node is a full cluster, and adding
members buys no throughput.

---

## Operations

### Losing members

| Down | What the cluster does | What you do |
|---|---|---|
| **One** (`N`=3) | writable throughout; quorum intact | restart it. It replays, rejoins, anti-entropy fills the gap. Nothing special |
| **A minority** (`N`=5, two down) | writable throughout | same |
| **A minority partitioned away** | minority: `/ready` 503, serializable reads still answered. Majority: unaffected | nothing urgent -- the guard is working. Check the majority side first |
| **A quorum or more** (`N`=3, two down) | no linearizable operation completes; survivors answer `NO_QUORUM_AT_COORDINATOR`. `?stale` reads are **still served** from local committed state | restore the members -- see below |
| **All** | nothing serves | start at least a majority, then the rest |

### Losing quorum

`QUORUM_LOST` is raised, `/ready` is already 503, and
`discas_node_peer_handshaked` says which members are missing.

1. **Check whether you are on the minority side of a partition.** If the majority is serving, this
   node's refusal is the guard working, and acting on this side is how you break it.
2. **Restore members.** Restart them, or replace the hardware under them -- the seat is what matters,
   not the machine ([3. Cluster](03-cluster.md#replacing-the-hardware-under-a-member)).
3. **Do not resize `--cluster-size` to make a quorum appear.** A mixed-`N` cluster forms two
   quorums over one key, which the handshake refuses precisely to prevent. There is no emergency in
   which this is the answer.

Reads tolerant of staleness keep working the whole time. If your callers can use
`ReadConsistency.SERIALIZABLE`, a quorum loss is a write outage rather than a total one -- the
`/ready` response body carries `canServeStaleReads` so traffic can be routed on it.

### A node that starts with nothing

It enters `AWAITING_FLOOR` and asks every member for the two bounds each reserves ahead of use.
There are two ways out and no third:

- **Enough members that remember answer** -- `N - quorum + 1` of them -- and the floor is their
  maximum. That is the smallest set guaranteed to intersect any quorum that could have taken a
  promise from this node's predecessor. It needs one member more than the cluster otherwise
  tolerates: at `N`=3, both peers.
- **Every member answers and not one reports a bound above zero** -- the cluster has no history, so
  the floor is zero. A cluster starting for the first time and one whose members all lost their
  state are the same situation.

Anything else is not an answer. Nothing times out and nothing decides a member is gone: the missing
witness may be a member with an intact disk that has not started yet. The node keeps asking at INFO,
saying how far short it is.

> **Why not just start and catch up?** Anti-entropy restores *values*. It cannot restore promises,
> because no peer ever had them. A node repaired that way holds correct data and is still unsafe to
> vote. The floor is what makes it safe, and it is why the node waits for one.

`FLOOR_UNAVAILABLE` is raised after **2 minutes** of waiting. The window is there because a member
still coming up is an ordinary moment, not a fault.

### The one case that cannot be repaired

**A quorum's worth of members lost their state at once, while some state survives.** The bound
existed nowhere else, so nothing can derive it, and the empty nodes will wait indefinitely -- which
is correct. Two ways out:

- **A member with an intact disk returns.** Nothing else is needed.
- **None is coming.** The loss is real. Clearing the rest of the directories makes it a new cluster,
  and that is the only way to say so: *removing* what is left is how an operator declares that the
  loss is accepted. Nothing infers it and no flag asserts it.

---

## Troubleshooting

### Capacity

| State | Raised when | Action |
|---|---|---|
| `STORE_FULL` | the store's estimated heap footprint reached `--store-heap-fraction` of max heap | see below |

Two shapes, one condition:

- **A serving node** refuses writes that would grow the store. Deletes still work, and that is the
  way out.
- **A node in replay does not start at all** and enters `FAILED`. There is no caller to refuse while
  it recovers state it already owns, so it stops rather than being killed by the JVM part-way
  through, at a moment nobody chose, on a node its peers were counting as a member.

The only remedy is room: more heap, or a higher `--store-heap-fraction`. **Never delete anything
under `--wal-dir`** -- the log is intact and the heap is too small for it, so deleting would destroy
good data to fix a JVM flag. And because every replica holds the same keys, a full node is a full
cluster: fix the sizing everywhere.

### Tombstones climbing

Not a fault. One key is collected per `--tombstone-sweep-interval-seconds` **per node** -- each node
sweeps the keys it owns -- so the cluster rate is `N` times the per-node rate, about 26,000 a day at
`N`=3 on the default. A workload that deletes faster than that will see `discas_node_tombstones`
climb while `_collected_total` is still rising. Lower the interval, or budget the key space at
roughly 280 bytes a tombstone.

### Collection stopped

`discas_node_tombstone_collection_blocked_seconds` climbs and
`discas_node_tombstone_collection_blocked_by{peer,answer}` names the member. Reads, writes and
`/ready` are all unaffected. Read the `answer` label:

- **`silent`** -- that member is down. Collection waits for it deliberately: a member that cannot
  answer might be the replica still holding the deleted value. Its own `PEER_DOWN` is the state to
  act on.
- **`retained`** from a member that is otherwise healthy -- a replica that is merely behind. Nothing
  to do; anti-entropy ends it. `retained` outliving several repair intervals is a defect to report,
  not an operator's job.
- **`retained` from a member with `WAL_DEGRADED`** -- a log that cannot say a write reached the disk
  cannot make a claim about the disk, so it refuses to permit a collection. Restart that member.

Alert on the blocked gauge at a **day**, never at a sweep.

### Same-key contention

Proposers duel, ballots escalate, and progress on that key stalls for a while;
`discas_node_external_ballots_observed_total` climbs. This is the workload discas handles least
well. Coordinator affinity normally serializes a key behind one node; a failover, or a client whose
node list differs in **membership or order**, breaks that -- affinity is computed from a node's
position in the client's list, so the same members listed in a different order route the same key
elsewhere ([11. The agent](11-http-agent.md#what-the-ids-in---nodes-are-and-are-not)). It is a workload shape rather than a fault, and there is no knob
that fixes it.

---

## Impact

| Action | Effect |
|---|---|
| Restart a minority | none visible. Clients fail over during replay |
| Lose a majority | writes stop and say so determinately; stale reads continue |
| Raise `--store-heap-fraction` | more of the heap is available to the store, and less to everything else. Needs a restart |
| Lower `--tombstone-sweep-interval-seconds` | faster reclamation, more background peer traffic |
| Lengthen `--wal-force-interval-ms` | more throughput, and a longer tail exposed to an unclean shutdown. Never a correctness change |

---

## Recovery

There is no "recover the cluster" operation here, because there is no single piece of state to
recover -- each member holds its own, and quorum intersection guarantees any committed value is
present in whichever majority forms. Recovery is always one of three things:

1. **Bring members back** -- the answer to a lost quorum, and almost always the whole of it.
2. **Give a member an empty directory** -- the answer to storage that is present and contradictory.
   It then proves a floor rather than asserting one.
3. **Accept the loss and start again** -- the answer when a quorum's worth of state is gone for good.
   `discas-admin init` builds the new cluster; a dump, if you have one, seeds it
   ([9. Backup](09-backup.md)).

---

## Related

- [2. The node](02-node.md) -- start states, storage repair, the two fatal cases
- [3. Cluster and members](03-cluster.md) -- `N`, membership, a copied directory
- [6. Monitoring](06-monitoring.md) -- what to alert on and at what window
- [9. Backup and recovery](09-backup.md) -- what a dump is and is not
- Runbooks: [RB-07 recover from a lost quorum](13-runbooks.md#rb-07-recover-from-a-lost-quorum),
  [RB-09 STORE_FULL](13-runbooks.md#rb-09-store_full)
