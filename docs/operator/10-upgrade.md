# 10. Upgrades and restarts

Restarting one node costs nothing visible ([2. The node](02-node.md#restarting)). Restarting
*several* has one trap in it, and it is the trap people hit: **a cold start is not a rolling
restart**, and the procedure that is correct for one deadlocks the other.

- **Rolling restart of a live cluster** -- restart one node, wait for its `/ready` to return 200,
  then the next. This works because the *others* hold quorum throughout.
- **Cold start, every node down** -- the first node started will **never** become ready: readiness
  requires a quorum and it is alone. Waiting for `/ready` before starting the second deadlocks the
  procedure.

So on a cold start, bring up **at least a majority** before expecting anything to be ready, then the
rest. Order does not otherwise matter and nothing is lost by it: every node has its own durable
state, and quorum intersection guarantees any committed value is present in whichever majority forms.

The property underneath both: **the cluster never decides that a member is permanently gone.** There
is no failure detector, so a node that cannot see its peers waits, refusing linearizable work, for
as long as that lasts -- there is no timeout that turns "not started yet" into "gone forever". That
is why bringing nodes up one at a time cannot split the cluster.

---

## How to check

```sh
# Before: everyone is in.
for h in n1 n2 n3; do echo -n "$h "; curl -s $h:9600/ready; echo; done

# During a roll: this node is back before you touch the next.
curl -sf localhost:9600/ready && echo up
```

`/ready` is the gate for every step below. `/health` is not -- a node in `AWAITING_FLOOR` answers
`/health` 200 while being out of the quorum, which is exactly the case a roll must not walk past.

Watch `discas_node_peers_handshaked` on the *other* members too. A node that comes back and does not
appear there is a node whose handshake is being refused, and the roll should stop until you know why
([3. Cluster](03-cluster.md#membership-states)).

---

## Operations

### Rolling restart or upgrade

`N` stays constant, quorum is never lost, and no flag is involved -- every node has its own state.

1. Confirm every member reports `/ready` 200 before starting. A roll begun one member short is a
   roll that loses quorum at its first step.
2. Stop one node with `SIGTERM` and let it drain.
3. Replace the binary or the configuration.
4. Start it, and **wait for its `/ready` to return 200.**
5. Next node.

At `N`=3 this tolerates exactly one node being down at a time, which is what the procedure does. At
`N`=5 you could take two, and should not: the margin is what absorbs an unrelated failure arriving
mid-roll.

`PEER_DOWN` carries a **1-minute** window precisely so a rolling restart is silent. If it fires
during a roll, that node has been down longer than a restart should take -- stop and look.

### Cold start of a whole cluster

1. Start **at least a majority** of members, without waiting for readiness between them.
2. Once a majority is up, they form quorum and go `SERVING`.
3. Start the rest.
4. Confirm every member reports `/ready` 200.

One case needs **more** than a majority: a member that starts **with no state** cannot serve until
enough members answer it -- `N - quorum + 1`, which at `N`=3 is both peers. So bring up *every*
member of a brand-new cluster, and leave the peers of a replaced member running while it rejoins
([4. Quorum](04-quorum.md#a-node-that-starts-with-nothing)).

### Changing the version

discas versions three surfaces, and only one of them constrains a roll: the **peer protocol**. Two
ends running incompatible peer protocol versions refuse each other with `VERSION_MISMATCH` and the
refusing node cannot carry consensus traffic.

Before rolling a version that changes it:

- **Check the release notes for a peer-protocol change.** If there is none, a roll is ordinary.
- **If there is one**, a rolling upgrade will partition the cluster by version the same way a
  `--cluster-size` change partitions it by `N`: safe, but with a write outage from the moment fewer
  than a majority share a version until a majority is on the new one. Plan it as a stop-the-world
  restart unless a brief write outage is acceptable.
- **Roll one direction at a time.** A half-rolled cluster you then start rolling back spends longer
  in the split than either roll alone.

### Changing configuration

Most configuration changes do not need a restart at all -- see
[7. Configuration](07-configuration.md#reload-vs-restart) for the four reloadable sources. For
those that do, the rolling procedure above applies unchanged, with one exception: **a change every
member must agree on cannot be rolled.** `--cluster-size` is the clear case
([3. Cluster](03-cluster.md#changing-n)); turning `--tls` on or off is another, since a member with
TLS and one without will not handshake.

For those, stop the world:

1. Confirm the cluster is healthy and quiesce writes if the change is one you want a clean line
   under.
2. Stop every member.
3. Apply the change everywhere.
4. Start at least a majority, then the rest.

Consensus state is durable in each member's WAL, so nothing is lost; the outage is the restart time.

---

## Troubleshooting

| Symptom | Cause and action |
|---|---|
| The first node of a cold start never becomes ready | expected. Readiness needs a quorum and it is alone. Start the rest -- do not wait |
| A restarted node sits in `AWAITING_FLOOR` | it came back with no state and needs `N - quorum + 1` members to answer. Leave its peers running; it says how far short it is, once a second |
| Writes stall part-way through a roll | fewer than a majority share a single `N` or peer-protocol version. Finish the roll in the direction you started |
| `VERSION_MISMATCH` after an upgrade | finish the rollout or roll it back. A mixed mesh is refused, not tolerated |
| `PEER_DOWN` fires during a roll | that node has been down over a minute. Stop the roll and find out why before touching another member |
| A member does not rejoin after its restart | check its handshake on the *peers*, not on itself: `CLUSTER_SIZE_MISMATCH`, `FOREIGN_CLUSTER`, `IDENTITY_MISMATCH` and `UNKNOWN_MEMBER` all present as "it started fine and nobody will talk to it" |

---

## Impact

| Action | Effect |
|---|---|
| Rolling restart at constant `N` | none on availability. Clients fail over during each replay, so `NOT_READY` never reaches a caller |
| Cold start | nothing serves until a majority is up |
| Rolling a peer-protocol change | a write outage bounded by how long the cluster is split by version |
| Any restart | the first write to each key may cost one extra round afterwards. That is the whole visible cost |

---

## Rollback

Every step above is reversible, and the reversal is the same procedure in the other direction --
which is the reason to roll one node at a time even when you are confident.

- **A rolling upgrade** rolls back node by node the same way. Nothing on disk has changed shape.
- **A stop-the-world change** rolls back by stopping the world again and putting the old value back.
  The WAL is unchanged either way.
- **A `--cluster-size` change** is the exception that is not cheap to reverse: it is a full
  stop-the-world in both directions. Decide it once -- [3. Cluster](03-cluster.md#changing-n).

There is no state migration to undo. A node's durable state is its WAL and its `incarnation` marker,
and neither is touched by a version or a configuration change.

---

## Related

- [2. The node](02-node.md) -- starting and stopping one node
- [3. Cluster and members](03-cluster.md) -- handshake refusals, and changing `N`
- [4. Quorum and durability](04-quorum.md) -- what a majority buys
- [7. Configuration](07-configuration.md) -- what reloads and what does not
- Runbooks: [RB-03 rolling restart](13-runbooks.md#rb-03-rolling-restart-or-upgrade),
  [RB-04 cold start](13-runbooks.md#rb-04-cold-start-a-whole-cluster)
