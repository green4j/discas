# 3. Cluster and members

A discas cluster is exactly `N` **seats**, each named by a `node_id`. The set of seats is the
cluster's shape, and `N` -- the quorum basis -- is **frozen when a node starts**. The membership file
does not change it: that file manages *connectivity and trust*, which is to say where each seat is
and whether this node will talk to it. Two nodes running different `N` refuse each other at the
handshake, so a mixed-`N` mesh, and with it split-brain, can never form.

**`node_id` is a role, not a machine.** It names a seat, it appears in every member file and every
certificate SAN, and it is meant to outlive the hardware under it. Replacing a failed disk therefore
is *not* a membership change: the seat stays, and only the `incarnation_id` -- which names *which run
of that seat's storage* this is -- changes. Renaming the seat instead would churn the member file and
the PKI on every disk failure.

The cluster never decides that a member is permanently gone. There is no failure detector: `/ready`
is derived from *completed handshakes*, not from probing peers, and nothing turns "not started yet"
into "gone forever". That is why bringing nodes up one at a time cannot split the cluster, and why
the judgement *"this member is not coming back"* is always yours, expressed by editing the member
file. Nothing infers it.

---

## How to check

```sh
curl -s localhost:9600/metrics | grep -E 'peer|cluster_size|quorum'
```

| Metric | Says |
|---|---|
| `discas_node_cluster_size` | `N`, the frozen quorum basis. Compare it across members -- they must agree |
| `discas_node_peers_handshaked` | how many peers completed `PEER_HELLO` and have not since dropped |
| `discas_node_peer_handshaked{peer="n2"}` | 1 or 0 per peer. **TCP connectivity alone does not count** |
| `discas_node_quorum_available` | 1 when this node plus its handshaked peers can form a quorum |
| `discas_node_peer_state_changed_seconds{peer="n2"}` | unix seconds of that peer's last transition |
| `discas_node_peer_transitions_total{peer="n2",direction="up"}` | flapping shows up here |
| `discas_node_peer_handshakes_rejected_total` | a peer that connects and is refused; the log says why |
| `discas_node_members_reloads_accepted_total` / `_rejected_total` | whether your last member-file edit took |
| `discas_node_peer_incarnation_changes_total` | a member came back on new storage |

Per-peer labels are safe because `N` is frozen at startup; keys are never labels.

**A completed handshake is not proof a peer will answer.** discas has no failure detector by design,
so nothing here notices a peer that is handshaked but wedged. This catches peers that are down,
gone, partitioned away, or failing the handshake -- not one that is merely stuck.

---

## Operations

### The membership file

Java properties, one line per member, **members only** -- no cluster id and no `N`, both of which
each node gets at startup:

```properties
node.eu-1=eu-1.discas.payments.svc.cluster.local:7001
node.eu-2=eu-2.discas.payments.svc.cluster.local:7001
node.us-1=us-1.discas.payments.svc.cluster.local:7001
```

The list must define **exactly `--cluster-size` members and include this node**. If the file at
startup does not, the node fails to start; if a *reload* does not, it is refused and the previous
list stays in force (`MEMBERS_REJECTED`).

The address may be a DNS name or a literal. It is held unresolved and resolved fresh at each
connection attempt, so a peer that moves behind a stable name -- a rescheduled pod -- is dialed at its
current endpoint with no config edit. That is why this handbook says "address" and never "IP".

`discas-admin init` generates this file for a new cluster, as `members.conf`.

### What a reload does

Edit the file in place -- nothing reads it until you ask -- then put it in force with
`curl -X POST http://127.0.0.1:9600/reload` on that member, and on every other member you copied it
to. The reply says what happened to each file
([7. Configuration](07-configuration.md#reload-vs-restart) has the full shape).

| Edit | Effect |
|---|---|
| Member re-addressed | new address stored. With `--peer-force-reconnect false` (default) a healthy connection stays and the address is used at the next reconnect; `true` drops it to reconnect now |
| Member swapped for another at the same `N` | applied -- the old peer's connections close, the new one becomes connectable |
| Member added or removed (changing `N`) | **refused and logged.** Quorum cannot change without a restart |
| Malformed | refused; the current list is kept -- and so is every other file in that reload |
| Rewritten with the same meaning | reported `unchanged`; nothing is re-applied |

### Planning a cross-region cluster

One stretched cluster across regions is a supported topology, not a workaround: no timer's value
governs cluster health, so members on a WAN link need no settings a single-rack cluster does not
([7. Configuration](07-configuration.md#tuning) says why the defaults already clear typical
inter-region round trips). What it costs is quorum -- every member votes, so a write pays two
inter-region round trips and a linearizable read one, and there is no non-voting replica to place
near distant readers.

Six decisions, all made before the first node starts:

1. **Choose a `cluster_id`.** Fixed for the life of the cluster and given to each member at startup;
   it is not in the member file.
2. **Choose `node_id`s.** Stable, human-readable, unique, and worth encoding the region for
   legibility -- `eu-1`, `eu-2`, `us-1`, `ap-1`. Pick an **odd `N`** (3 or 5) for a clean majority.
   Node id ordering also decides the single-dialer direction -- lower dials higher -- which needs no
   action but explains the logs.
3. **Plan addresses and ports.** Every member needs a peer address reachable from every region, plus
   a separate client port. Open the peer port between all regions.
4. **Design the CA hierarchy.** One root CA for the cluster, and a per-region intermediate signing
   that region's node leaves. Every member trusts the **root**, so it accepts leaves from any region.
   Keep the root offline and the intermediates with the regional issuer.
5. **Define the certificate profile.** SAN `discas://<cluster_id>/<node_id>`, client and server auth,
   and whatever lifetime your issuer supports ([15. PKI recipes](15-pki-recipes.md)).
6. **Write the membership file** and distribute it to every member, with the same `--cluster-id` in
   each member's startup environment.

Place members so that no single region holds a majority unless you intend that region to be able to
serve alone: at `N`=3 across three regions, any one region can be lost; at `N`=3 with two members in
one region, losing that region loses the cluster's writes.

### Replacing the hardware under a member

The usual case, and there is almost nothing to do:

1. **Keep the `node_id` and its certificate.** Same seat, same SAN, no member-file edit, no reload
   on the peers.
2. **Give it an empty data directory and start it.** There is no flag and nothing to declare.
3. **Watch it join.** It sits in `AWAITING_FLOOR` -- `/health` 200, `/ready` 503 -- asks the members
   for the highest ballot any of them has reserved, adopts the maximum as its promise floor, and
   only then serves. Anti-entropy fills in the values. Its peers log the change and carry on:
   `peer frankfurt-1 has new storage: incarnation ... -> ...`
4. **Leave the other members running while it recovers.** It needs `N - quorum + 1` of them to
   answer -- at `N`=3, both. It says how far short it is, once a second, at INFO.

What you must **never** do is hand the replacement a copy of another member's data directory. See
[a copied directory](#a-copied-directory) below.

### Retiring a seat in favour of another

Only when the id itself has to go. This is a membership edit and costs a PKI change:

1. Issue a cert for the new `node_id`, SAN `discas://<cluster_id>/<new_node_id>`.
2. **In one edit**, on every node: remove the old `node.<old>` line, add `node.<new>=host:port`, and
   `POST /reload`. `N` is unchanged, so the reload is applied.
3. Start the new node. It joins as above -- empty, and recovering a floor before serving.
4. Stop the old node.

### Evicting a compromised member now

A bare removal shrinks `N` and is refused, so use a **constant-`N` swap**: on every node, replace the
compromised `node.<id>` with a standby or a blackhole placeholder address in the same edit, then
`POST /reload`. The reload closes its connections and refuses its re-handshakes (`UNKNOWN_MEMBER`)
while `N`, and therefore quorum, stays fixed. Stop renewing its certificate too.

### Changing `N`

Not an online operation, by either route, and not a missing feature: `N` is a durability decision
made when the system is designed. Adding members to a CASPaxos cluster makes it *slower* rather than
faster -- every key lives on every member, so a sixth member adds a message to every round and buys
no throughput.

Two procedures, and the choice is about what you want to keep:

| | Keeps | Costs |
|---|---|---|
| **Stop-the-world restart** onto the new `--cluster-size` | the cluster id, the members' own durable state | an outage the length of a restart |
| **`dump` -> `init` -> cut over** ([9. Backup](09-backup.md)) | nothing in place; you get a new cluster with a new id | stopping writes first, and a cut-over rather than a drift |

Prefer the restart when the cluster is otherwise healthy and staying where it is. Take the dump
route when you are migrating anyway, or want a clean cluster id.

A **rolling** restart onto a new `N` is safe but not quiet: old-`N` and new-`N` nodes refuse each
other, so writes stall from the moment fewer than a majority share any single `N` until a majority
is on the new one. That is the guard working. Full steps:
[RB-06](13-runbooks.md#rb-06-change-n).

---

## Troubleshooting

### Membership states

| State | Raised when | Normal for | Action |
|---|---|---|---|
| `MEMBERS_REJECTED` | a reloaded member list was refused | -- | fix the file: exactly `--cluster-size` members, including this node. Cleared by a reload that is accepted |
| `CLUSTER_SIZE_MISMATCH` | two members disagree about `N` | -- | make `--cluster-size` agree. **Never resize `N` to make a quorum appear** |
| `CLUSTER_SIZE_INVALID` | a node reported a cluster size that cannot be true | -- | fix `--cluster-size` where that node runs |
| `FOREIGN_CLUSTER` | a peer presented a different cluster id | -- | check `--cluster-id` and the address list: something is pointed at the wrong deployment |
| `IDENTITY_MISMATCH` | the member list and the identity a peer presents disagree | -- | the member file and the certificate SANs disagree about who this is; the `cause` says which of the four |
| `UNKNOWN_MEMBER` | a peer handshaked that is not in this node's member list | -- | add it if it belongs, stop it if it does not |
| `VERSION_MISMATCH` | the two ends run incompatible peer protocol versions | -- | finish the rollout or roll it back |

### Peer states

| State | Raised when | Normal for | Action |
|---|---|---|---|
| `PEER_DOWN` | a handshaked peer's connection went away | **1 min** | bring it back or replace it. The window is why a rolling restart is quiet |
| `QUORUM_LOST` | this node's handshaked peers fell below the quorum line | -- | restore members. If this is the minority side of a partition, the guard is working -- check the other side first. See [4. Quorum](04-quorum.md#losing-quorum) |
| `NODE_UNREACHABLE` | a client or agent lost its connection to a node | **1 min** | check that node and the path to it; requests fail over meanwhile |

### A node that comes up and sees nobody

Identical behaviour whether the others are seconds away or never coming:

- `/ready` returns 503, so a load balancer sends it nothing.
- A client that reaches it anyway gets a **determinate** refusal -- the first round proves no majority
  and answers `NO_QUORUM_AT_COORDINATOR`, which the client fails over on immediately rather than
  waiting out a timeout.
- That verdict lapses after `--no-quorum-backoff-ms` (2 s), so a node that refused everything while
  alone starts serving within one slow request of its peers appearing.

Nothing distinguishes the two cases from inside. The difference is entirely in what you do next.

### A copied directory

The one damage nothing in this store can see on its own, and therefore the one that needs a
procedure. A restored or copied directory replays as a perfectly good log -- the records are
CRC-framed and unbroken, so the node trusts its own bounds and rejoins as a healthy member that
happens to be holding an older version of the world. Deleted keys are the worst of it: once their
tombstones have been collected, nothing anywhere can out-vote the copy, and the resurrection is
silent and permanent.

Two states catch the cases that *are* visible, and both mean stop:

| State | Raised when | Action |
|---|---|---|
| `STORAGE_CLONED` | a member arrived presenting another member's `incarnation_id` | stop the clone and start it empty |
| `PEER_STORAGE_ROLLED_BACK` | a member returned with the same incarnation and a lower promise ceiling -- the same storage came back holding less than it left with | **stop that member now**, keep its directory for forensics, start it empty. Do not lift the refusal |

`UNACCOUNTED_KEYS_DROPPED` is the trace on the other side: a node took its promise floor from the
cluster, found keys at or below that floor that no member holds, and dropped them. It latches, and
it is the only signal that state left a node. Establish what happened to that storage.

---

## Impact

| Action | Effect |
|---|---|
| Edit an address in the member file | none while connections are healthy, unless `--peer-force-reconnect true` |
| Constant-`N` seat swap | the old peer's connections close at once; quorum arithmetic unchanged |
| Add or remove a member without a restart | **nothing.** The reload is refused, and the cluster keeps the list it had |
| Change `N` | a write outage, bounded by the restart, on either procedure |
| Replace a member's hardware | none, as long as its peers stay up while it recovers a floor |

---

## Recovery

- **A rejected reload** is not an outage -- the previous member list is still in force. Fix the file;
  the next accepted reload clears the state.
- **A member you have decided is gone** does not need to be removed. Removing it changes `N` and is
  refused. Either replace the hardware under the same seat, or do a constant-`N` swap to a standby.
- **A cluster below quorum** is a durability question, not a membership one:
  [4. Quorum and durability](04-quorum.md#losing-quorum).

---

## Related

- [2. The node](02-node.md) -- the process, its files, start and stop
- [4. Quorum and durability](04-quorum.md) -- what a majority buys and what it costs to lose
- [5. Access](05-access.md) -- the certificate SANs the member file has to agree with
- Runbooks: [RB-05 replace a node](13-runbooks.md#rb-05-replace-a-node),
  [RB-06 change N](13-runbooks.md#rb-06-change-n),
  [RB-15 a node returned from a copy](13-runbooks.md#rb-15-a-node-returned-from-a-copied-directory)
