# 13. Runbooks

Procedures, in one shape: **When, Preconditions, Steps, Verify, Rollback**. No explanations -- the
argument lives on the topic page each runbook links to, so that a page read under pressure is steps
and nothing else.

Addresses in the examples: `9600` is the node's observability port, `7001` a peer port, `7002` a
client port. All three are yours to choose.

Commands are written in their short form. Without the distribution start scripts, substitute the
main class throughout -- same arguments, same `DISCAS_*` variables
([1. Context](01-context.md#two-ways-to-invoke-each-of-them)):

| Short form | From the jars |
|---|---|
| `discas-node` | `java -cp "$CP" io.github.green4j.discas.node.starter.DisCasNodeStarter` |
| `discas-agent` | `java -cp "$CP" io.github.green4j.discas.agent.starter.DisCasAgentStarter` |
| `discas-admin` | `java -cp "$CP" io.github.green4j.discas.admin.starter.DisCasAdminStarter` |

| | |
|---|---|
| [RB-01](#rb-01-check-cluster-health) check cluster health | [RB-09](#rb-09-store_full) `STORE_FULL` |
| [RB-02](#rb-02-restart-a-node) restart a node | [RB-10](#rb-10-take-a-backup-and-verify-it) take a backup |
| [RB-03](#rb-03-rolling-restart-or-upgrade) rolling restart | [RB-11](#rb-11-load-a-dump-into-a-live-cluster) load a dump |
| [RB-04](#rb-04-cold-start-a-whole-cluster) cold start | [RB-12](#rb-12-stand-up-a-new-cluster-from-a-dump) stand up a cluster |
| [RB-05](#rb-05-replace-a-node) replace a node | [RB-13](#rb-13-rotate-the-peer-ca) rotate the peer CA |
| [RB-06](#rb-06-change-n) change `N` | [RB-14](#rb-14-rotate-client-credentials) rotate client credentials |
| [RB-07](#rb-07-recover-from-a-lost-quorum) recover a lost quorum | [RB-15](#rb-15-a-node-returned-from-a-copied-directory) a copied directory |
| [RB-08](#rb-08-a-node-that-will-not-start) a node that will not start | |

---

## RB-01. Check cluster health

**When** -- before any other runbook, after any incident, and on a schedule.

**Preconditions** -- none.

**Steps**

```sh
# 1. Is a person owed something?  One rule covers both node and agent.
for h in n1 n2 n3; do
  echo "== $h"; curl -s $h:9600/metrics | grep 'discas_operator_attention'
done

# 2. Is each member in the quorum?
for h in n1 n2 n3; do echo -n "$h "; curl -s $h:9600/ready; echo; done

# 3. Do they agree about N, and see each other?
for h in n1 n2 n3; do
  curl -s $h:9600/metrics | grep -E 'discas_node_(cluster_size|peers_handshaked|quorum_available)'
done

# 4. Can a client actually work?  The honest end-to-end check.
discas-admin dump --nodes 1=n1:7002,2=n2:7002,3=n3:7002 --out /tmp/health.dump
```

**Verify** -- no attention samples; `/ready` 200 everywhere; `discas_node_cluster_size` identical on
every member and equal to the member count; `discas_node_quorum_available 1` everywhere; the dump
completes.

**Rollback** -- n/a; read-only. Delete `/tmp/health.dump`.

-> [6. Monitoring](06-monitoring.md)

---

## RB-02. Restart a node

**When** -- a configuration change that no reload covers, `WAL_DEGRADED` after clearing the
device, `SHUTDOWN_INCOMPLETE`, or an ordinary maintenance restart.

**Preconditions** -- every *other* member reports `/ready` 200. At `N`=3 this procedure tolerates one
node down and no more.

**Steps**

1. Confirm the rest of the cluster is healthy: RB-01, steps 2-3.
2. `SIGTERM` the node -- `systemctl stop discas-node`, or a container stop. It drains within
   `--shutdown-await-timeout-ms`.
3. Confirm the process is gone and nothing else holds `--wal-dir`.
4. Make the change.
5. Start it.

**Verify** -- `curl -sf localhost:9600/ready` returns 200, and the peers show it:
`discas_node_peers_handshaked` back to `N-1` on another member.

**Rollback** -- stop it, put the previous configuration or binary back, start it. Its durable state
is untouched by either.

-> [2. The node](02-node.md#restarting)

---

## RB-03. Rolling restart or upgrade

**When** -- a version rollout, or any configuration change every member needs that no reload covers.

**Preconditions** -- every member reports `/ready` 200. **No peer-protocol change in this version** --
if there is one, use RB-04 instead, or accept a write outage.

**Steps**

For each member, one at a time:

1. RB-02, steps 2-5.
2. **Wait for `/ready` 200 on that node before touching the next.** Not `/health`.
3. Confirm `PEER_DOWN` did not fire for it -- if it did, that node has been down over a minute; stop
   and investigate before continuing.

**Verify** -- every member `/ready` 200, `discas_node_cluster_size` unchanged, no
`VERSION_MISMATCH` anywhere.

**Rollback** -- the same procedure in the other direction, node by node. Nothing on disk changed
shape. Do not start rolling back mid-roll unless you have to: a half-rolled cluster you reverse
spends longer split than either direction alone.

-> [10. Upgrades and restarts](10-upgrade.md#rolling-restart-or-upgrade)

---

## RB-04. Cold start a whole cluster

**When** -- every node is down; or a change every member must agree on (`--cluster-size`, turning TLS
on or off).

**Preconditions** -- each member's data directory is intact and belongs to that member. If one does
not, RB-08 first.

**Steps**

1. **Do not wait for `/ready` between the first nodes.** The first node started can never become
   ready -- readiness needs a quorum and it is alone.
2. Start at least a **majority** of members.
3. Wait for those to report `/ready` 200. They now hold quorum.
4. Start the rest.

If any member is starting with **no state**, it needs `N - quorum + 1` members to answer it -- at
`N`=3, both peers. Start *every* member in that case and leave them running.

**Verify** -- every member `/ready` 200; RB-01.

**Rollback** -- stop everything and start again. Consensus state is durable in each WAL, so a
restart costs only time.

-> [10. Upgrades and restarts](10-upgrade.md#cold-start-of-a-whole-cluster)

---

## RB-05. Replace a node

**When** -- a failed disk, a lost volume, a rebuilt host. The seat stays; only the hardware changes.

**Preconditions** -- the *other* members are up and will stay up throughout. **Keep the `node_id` and
its certificate**: this is not a membership change and needs no member-file edit.

**Steps**

1. Stop the old node if it is still running.
2. Give the replacement an **empty data directory**. There is no flag and nothing to declare.
3. Start it with the same `--node-id`, `--cluster-id`, member file and certificate.
4. Leave the other members running while it recovers.

**Verify** -- the node reports `AWAITING_FLOOR`, then `SERVING`; `/ready` 200. Its peers log
`peer <id> has new storage: incarnation ... -> ...` and
`discas_node_peer_incarnation_changes_total` increments.

**Rollback** -- none needed and none possible: nothing about the cluster's shape changed. If it will
not leave `AWAITING_FLOOR`, its peers are short -- RB-07.

> **Never hand the replacement a copy of another member's directory**, or of its own. RB-15.

-> [3. Cluster](03-cluster.md#replacing-the-hardware-under-a-member)

---

## RB-06. Change `N`

**When** -- growing or shrinking the cluster. Not an online operation by either route.

**Preconditions** -- a stated write-outage window. Decide which route first:

| Keep the cluster id and the data in place | **Route A**, below |
| Migrating anyway, or want a clean cluster | **Route B**, below |

### Route A -- stop-the-world restart

1. Issue certificates for any new `node_id`s.
2. Update `members.conf` to the target set on **every** node, old and new.
3. Stop **all** nodes.
4. Start them all with the new `--cluster-size`. Follow RB-04 for the ordering.
5. New members join empty and recover a floor; anti-entropy fills their values.

### Route B -- dump, init, cut over

1. **Stop writes.** This is what makes the dump self-consistent.
2. `discas-admin dump` the cluster (RB-10).
3. `discas-admin init` a folder at the new `N` **with a new cluster id**, and stand it up (RB-12).
4. Cut traffic over -- a cut-over, not a gradual drift.

**Verify** -- every member `/ready` 200 and `discas_node_cluster_size` equal to the new `N`
everywhere; a dump returns the expected pair count.

**Rollback** -- Route A: stop the world again and restart on the old `--cluster-size` with the old
member file. Route B: the old cluster is untouched -- point traffic back at it.

> A **rolling** restart onto a new `N` is safe but not quiet: writes stall from the moment fewer
> than a majority share any single `N`. A `Version` held from the old cluster does not fence against
> a new one -- which is why Route B ends in a cut-over.

-> [3. Cluster](03-cluster.md#changing-n)

---

## RB-07. Recover from a lost quorum

**When** -- `QUORUM_LOST`; writes fail `NO_QUORUM_AT_COORDINATOR`; `/ready` 503.

**Preconditions** -- none. Stale reads are still being served throughout.

**Steps**

1. **Check the other side first.** If a majority is serving elsewhere, you are the minority of a
   partition and the guard is working. Act on the network, not on this node.
2. Identify which members are missing:
   `curl -s localhost:9600/metrics | grep discas_node_peer_handshaked`
3. Bring them back -- restart them (RB-02) or replace their hardware (RB-05).
4. If a member will not start, RB-08.
5. **Do not resize `--cluster-size` to make a quorum appear.** There is no emergency in which this
   is the answer: a mixed-`N` cluster forms two quorums over one key.

**Verify** -- `discas_node_quorum_available 1`; `/ready` 200; writes succeed.

**Rollback** -- n/a.

> If a **quorum's worth of members lost their state at once** while some state survives, the empty
> ones will wait indefinitely and that is correct -- the bound existed nowhere else. Either a member
> with an intact disk returns, or you accept the loss: clearing the rest makes it a new cluster,
> which is the only way to say so.

-> [4. Quorum](04-quorum.md#losing-quorum)

---

## RB-08. A node that will not start

**When** -- `STORAGE_UNREADABLE`, a `FAILED` state at startup, a corrupt WAL segment or an unreadable
`incarnation` marker.

**Preconditions** -- the rest of the cluster is up. If it is not, this node is not your first
problem -- RB-07.

**Steps**

1. Confirm which it is from the log: replay threw (`WalException(REPLAY_FAILED)`), or the marker is
   present and unparseable.
2. **Copy the data directory aside for forensics** if you can. The node will not read it, but a torn
   segment is a fact about the device.
3. Delete what is broken:
   - the log is unreadable -> delete `<wal-dir>/wal/`. The node keeps the values in `snap/` and takes
     its ceiling from the cluster;
   - the marker is unreadable, or you would rather not decide -> delete the whole data directory.
4. Start the node.

**Verify** -- it reaches `AWAITING_FLOOR` or `REPLAYING`, then `SERVING`; `/ready` 200.

**Rollback** -- none. **Never restore the copy you kept**: a byte-continuous but older directory
replays as a clean log and rolls the promise ceiling back below promises this node already made.
That damage is silent and permanent.

> A **truncated WAL tail** is not this. It replays as complete, needs no action, and costs one extra
> round per key afterwards.

-> [2. The node](02-node.md#the-node-will-not-start)

---

## RB-09. `STORE_FULL`

**When** -- writes refused with `STORE_FULL`, or a node that stops during replay with it.

**Preconditions** -- none. **Never delete anything under `--wal-dir`**: the log is intact and the
heap is too small for it.

**Steps**

**A serving node** -- writes that would grow the store are refused; deletes are not, and that is the
way out:

1. Delete keys you no longer need, or `discas-admin load --cleanup` a dump that omits them.
2. Then fix the sizing, below -- every replica holds the same keys, so this will recur.

**A node that stopped during replay** -- only room helps:

1. Give the JVM more heap, or raise `--store-heap-fraction`.
2. Restart it.
3. **Apply the same change to every member.** A full node is a full cluster.

**Verify** -- the state clears; `discas_node_writes_refused_no_capacity_total` stops rising; the node
reaches `SERVING`.

**Rollback** -- lowering `--store-heap-fraction` again re-raises the state. Deleting data is not
reversible without a dump.

-> [4. Quorum](04-quorum.md#capacity)

---

## RB-10. Take a backup and verify it

**When** -- on a schedule, and before any migration or `N` change.

**Preconditions** -- a majority is serving. For a migration, **stop writes first** -- a dump is not a
point-in-time snapshot.

**Steps**

```sh
DISCAS_TOKEN=... discas-admin dump \
  --nodes 1=n1:7002,2=n2:7002,3=n3:7002 \
  --out /backups/prod-$(date +%F).dump
```

Give the **whole membership**, on **client** ports. Positional arguments are key prefixes; with none,
the whole key space is carried. Keep lock keys out of the prefixes you dump.

**Verify** -- the command reports its pair count and the file exists. Then, periodically, restore it
into a scratch cluster (RB-12) and compare -- a backup nobody has restored is a hypothesis. Store the
dump where the cluster is not.

**Rollback** -- n/a; read-only, apart from the load it puts on the cluster (one round trip per key).

-> [9. Backup and recovery](09-backup.md)

---

## RB-11. Load a dump into a live cluster

**When** -- the cluster is healthy and the *data* is wrong: a bad deployment, a deleted prefix, a
migration's pairs to move across.

**Preconditions** -- a majority is serving. For `--cleanup`, **the key space is yours** -- it deletes
unfenced and cannot tell a key somebody wrote during the load from a stale one.

**Steps**

```sh
# merge: writes the dump's keys, touches nothing else
discas-admin load --nodes 1=n1:7002,2=n2:7002 --in /backups/prod.dump

# replacement, bounded by prefixes: also deletes keys under them that the dump did not carry
discas-admin load --nodes 1=n1:7002 --in /backups/prod.dump --cleanup users/ orders/
```

With `--cleanup` and no prefixes it takes the **whole key space**. The prefixes are your blast
radius.

**Verify** -- the reported written (and deleted) counts match expectation; spot-check keys.

**Rollback** -- load an earlier dump over the same prefixes. There is no transaction: an interrupted
load leaves the keys it reached written and the rest as they were, which is also why a failed load
does not try to undo itself. Keep more than one dump.

-> [8. discas-admin](08-admin.md#load)

---

## RB-12. Stand up a new cluster from a dump

**When** -- a cluster that does not exist yet: a new `N`, a new region, a disaster recovery, a
scratch copy to verify a backup.

**Preconditions** -- a dump, if you want the members seeded. The new `--cluster-id` **must differ**
from any cluster these members could reach.

**Steps**

```sh
discas-admin init \
  --out-dir ./prod-2 --cluster-id prod-2 \
  --members 1=10.0.0.1:7001,2=10.0.0.2:7001,3=10.0.0.3:7001 \
  --in /backups/prod.dump
```

`--members` is the **peer** membership, and its size is the new `N`. Then:

1. **Read `./prod-2/RUN.md`.** It is generated for this cluster and carries the `scp` and
   `discas-node` command for each member.
2. Copy each member directory to **its own** host. One directory, one member, forever.
3. Start every member before sending any traffic.

**Verify** -- the check `RUN.md` ends with: dump the new cluster on its **client** ports and compare
the pair count with the number `init` reported. Fewer means a member was started on the wrong
directory, or traffic arrived before every member was up.

**Rollback** -- the new cluster is independent; delete it and start again. `init` connects to
nothing, so nothing else is affected. If it fails part-way it removes the whole folder itself.

-> [8. discas-admin](08-admin.md#init)

---

## RB-13. Rotate the peer CA

**When** -- an annual root or intermediate roll, or a compromised CA.

**Preconditions** -- the new CA exists. Zero disruption depends on **overlap**: trust the new CA
before anyone presents leaves from it.

**Steps**

1. **Trust first.** Update **every** node's trust store to contain **both** old and new CAs, then
   `curl -X POST http://127.0.0.1:9600/reload` on each to swap it in. Everyone now accepts both;
   nobody presents a new leaf yet, so nothing changes on the wire.
2. **Re-issue leaves** from the new CA for each `node_id` -- same identity, same SAN -- write the new
   key stores, and reload. Roll node by node at your own pace; the overlap makes any mix valid. One
   reload reads a node's key store and trust store together, so the pair is never half-applied.
3. **Retire the old CA** once *every* node presents a new-CA leaf: update the trust stores to drop
   it and reload each node.

**Verify** -- after each step, `discas_node_peers_handshaked` is unchanged and
`discas_node_peer_handshakes_rejected_total` is flat. After step 3, the old CA can be destroyed.

**Rollback** -- go back to trusting both CAs. Overlap is the safe state, and every step is reversible
while both are trusted. Step 3 is the only irreversible one -- do it last, and only after step 2 is
complete everywhere.

-> [5. Access](05-access.md#rotating-a-ca), [15. PKI recipes](15-pki-recipes.md)

---

## RB-14. Rotate client credentials

**When** -- scheduled token rotation, a compromised credential, or a client CA roll.

**Preconditions** -- none. Every step here is a file edit plus a reload; no restart, no dropped
connections.

### Tokens

1. Mint the new one and keep the secret it prints:
   ```sh
   discas-admin token --client-id web-1 --ttl-days 90
   ```
2. Append its record to that client's line after ` ; `, or add it to the client's `<id>.token` file.
3. `POST /reload` on **every** node.
4. Give the new secret to the client and let it switch over. Both are valid during the overlap.
5. Drop the old record, or let its `notAfter` retire it, and reload again.

### Client certificates

Re-issue over the client's key store, same CN. Nothing to reload node-side. Established sessions
ride on; new handshakes use the new material.

### Client CA

The same trust-first procedure as RB-13, but the trust store to update is the **node's** client-CA
trust store: add the new CA there first, re-issue client leaves, then drop the old CA.

### Adding a client

Same command, no rotation: `discas-admin token -c <id> >> <token file>`, then grant it in the ACL
file and reload. One reload covers both files, so the token and its grant arrive together.

### Cutting a client off now

Policy, not PKI: drop its token record or its ACL grant and reload. It applies at the client's next
request. For pure mTLS, remove it from the issuing pipeline and let a short leaf lapse, or roll the
client CA.

**Verify** -- the reload answers `200`, `discas_reloads_total` increments and
`discas_reload_failures_total` does not; the client can still connect (or, for a revocation, cannot).

**Rollback** -- put the old record back and reload. A malformed file is not an outage: it is refused
and the last good version stays in force.

-> [5. Access](05-access.md#rotating-a-token)

---

## RB-15. A node returned from a copied directory

**When** -- `STORAGE_CLONED`, `PEER_STORAGE_ROLLED_BACK`, or deleted keys reappearing.

**Preconditions** -- **urgency.** A node in this state is not behind; it is authoritatively pushing
stale state, and every minute it runs spreads more of it.

**Steps**

1. **Stop that node now.** Before diagnosing anything else.
2. Keep its data directory for forensics. Move it somewhere the node cannot start on it.
3. Establish how it happened -- a restored backup, a cloned volume, a snapshot-based image, a
   directory copied to a replacement host.
4. Bring the seat back **empty**: RB-05.
5. Assess the damage. Deleted keys that came back are the worst of it: once tombstones have been
   collected, nothing anywhere out-votes the copy. Re-apply the deletions from the application's own
   record.

**Verify** -- the state is cleared after the restart; `discas_node_keys_repaired_total` settles;
`discas_node_unaccounted_keys_dropped_total` on the returning node tells you whether it dropped keys
no member holds.

**Rollback** -- none. **Do not lift the refusal** and do not restart the node on that directory.

> `PEER_STORAGE_ROLLED_BACK` is a **detector, not a repair**, and it is per process: peers remember
> what they have seen, so a rollback that coincides with a restart of every peer arrives
> unannounced. What is visible then is second-order -- a `discas_node_keys_repaired_total` spike on
> every member just after it rejoins while `discas_node_peer_incarnation_changes_total` stays flat.

-> [3. Cluster](03-cluster.md#a-copied-directory)
