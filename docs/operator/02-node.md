# 2. The node

`discas-node` is the process that is a member of the cluster. Everything else in this handbook is
about several of them at once; this page is about one -- where its files are, how you start and stop
it, how you tell what it is doing, and what to do when it will not come up.

Two facts shape every procedure here. First, **a node's data directory is its identity**: it holds
an `incarnation` marker that says *which run of this member's storage this is*, and giving one
directory to two processes is the one damage the cluster cannot detect. Second, **nothing tells a
node it is starting empty, and nothing needs to.** An empty directory is a legitimate start -- the
node notices, asks the cluster for a bound it can prove, and serves once it has one. That is why
there is no "force start" flag on this page, and why deleting a broken directory is an ordinary
repair rather than a last resort.

What goes wrong here is almost always one of three things: the node cannot read its own storage, it
is up but not in the quorum, or two nodes were pointed at one directory. The first two are visible
on `/health` and `/ready`; the third is covered in [3. Cluster and members](03-cluster.md).

---

## How to check

```sh
curl -s localhost:9600/health   # liveness: past replay, WAL intact
curl -s localhost:9600/ready    # readiness: additionally serving and quorum-connected
```

Both carry `"state"`, the node's position in the start model:

| State | What the node is doing | `/health` | `/ready` |
|---|---|---|---|
| `REPLAYING` | reading its own snapshot and WAL tail | 503 | 503 |
| `AWAITING_FLOOR` | started with no state; asking the cluster for its promise floor | **200** | 503 |
| `SERVING` | in the quorum | 200 | 200 with quorum |
| `CLOSING` | draining | 503 | 503 |
| `FAILED` | stopped by something it cannot continue past | 503 | 503 |

```
REPLAYING --has state----------------------> SERVING --> CLOSING
   |                                          ^
   +--none--> AWAITING_FLOOR --floor adopted--+
                 ^     |
                 +-----+ keeps asking

any state --> FAILED
```

**`AWAITING_FLOOR` answering `/health` 200 is deliberate.** The node is healthy and waiting on its
peers; restarting it only starts the wait again. `/ready` is what says whether it is a member of the
quorum -- see [6. Monitoring](06-monitoring.md#health-and-ready) for why a liveness probe must never be
pointed at a quorum-aware endpoint.

Both long states report progress at INFO, as the state repeating itself:

```
12:04:31.882 INFO  [milan-4] node REPLAYING (replayed 240000 of 512000 snapshot entries)
12:04:36.902 INFO  [milan-4] node REPLAYING -> SERVING (keys=512000)
```
```
12:19:02.110 INFO  [frankfurt-1] node REPLAYING -> AWAITING_FLOOR (replayed no state)
12:19:03.114 INFO  [frankfurt-1] node AWAITING_FLOOR (1 of 2 witnesses, 1 of 2 answered)
12:19:07.271 INFO  [frankfurt-1] node AWAITING_FLOOR -> SERVING (floor=5119 from 2 witnesses)
```

The node also prints its effective configuration at startup, with secrets masked -- that table, not
your deployment manifest, is what the process is actually running with.

---

## Operations

### Where the files are

A node has exactly two paths that matter, and neither has a default:

| | Set by | Contents |
|---|---|---|
| Data directory | `--wal-dir` | `incarnation` (the marker), `wal/` (segments), `snap/` (snapshots) |
| Configuration | `--members-file`, `--client-token-file`, `--client-acl-file`, the `--*-truststore`/`--*-keystore` paths | one file each, several re-readable without a restart |

**One directory, one member, forever.** The data directory belongs to the `node_id` that has been
running on it, and to no other. Nothing about the layout enforces this, and nothing can.

#### On an orchestrated platform

A node needs its **own durable volume**, and getting that wrong is quiet rather than loud, because a
node that comes up with an empty volume is a legitimate start -- it asks the cluster for a floor and
carries on. Three rules:

1. **Give every member a `volumeClaimTemplates` PVC, and never `emptyDir`.** With `emptyDir` every
   pod restart is a wiped node: it comes back correct, but it re-fetches the whole dataset by
   anti-entropy and cannot serve until the cluster answers it. Deleting a StatefulSet does not
   delete its PVCs, and scaling down and back up reattaches them, so ordinary churn keeps the state.
2. **Alert on `discas_node_state_awaiting_floor_total`.** It rises exactly when a member started
   with nothing, which for a member that *should* have had a volume is the signal that the volume
   did not arrive. The node stays out of the quorum meanwhile, so nothing is at risk while you look
   -- but a whole cluster's worth of it at once is the one case nothing can distinguish from a
   genuine first start, and the one worth paging on.
3. **Bringing up a new cluster, bring up every member.** A member that starts empty needs the
   cluster to answer before it serves, and a cluster where nobody holds anything has to hear from
   everybody to establish that. Partially starting a new cluster leaves it waiting, correctly.

A discas node is identity-bound, so on Kubernetes this is a **StatefulSet** with a headless Service
and a volume per pod -- not a Deployment behind a horizontal autoscaler
([1. Context](01-context.md)). Pod churn itself is survivable: addresses are held unresolved and
resolved fresh at every dial, so stable per-pod DNS keeps working across rescheduling.

### Starting

```sh
discas-node \
  --node-id n1 --cluster-id prod \
  --members-file /etc/discas/members.conf \
  --peer-bind 10.0.0.1:7001 \
  --client-bind 0.0.0.0:7002 \
  --wal-dir /var/lib/discas/n1
```

The same start, without the distribution scripts:

```sh
CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)

java -cp "$CP" io.github.green4j.discas.node.starter.DisCasNodeStarter \
  --node-id n1 --cluster-id prod \
  --members-file /etc/discas/members.conf \
  --peer-bind 10.0.0.1:7001 \
  --client-bind 0.0.0.0:7002 \
  --wal-dir /var/lib/discas/n1
```

Both forms are the same program; the wrapper adds nothing but the classpath. Use `java -cp` when you
are running from a build tree or need JVM options the wrapper does not expose.

Every option is settable by **either** a flag **or** its `DISCAS_*` environment variable, with
`CLI > ENV > DEFAULT` decided per value -- see [7. Configuration](07-configuration.md).
`discas-node --help` is the authoritative list of flags and defaults; this handbook explains the
decisions behind them and does not transcribe it.

`discas-admin init` writes these command lines for you, per member, into a `RUN.md` -- for a cluster
that does not exist yet, start at [8. discas-admin](08-admin.md#init) rather than here.

### Stopping

The node installs a shutdown hook, so `SIGTERM` -- `systemctl stop`, a container stop, Ctrl-C -- is
the correct way to stop it. It enters `CLOSING`, drains, and closes its storage.
`--shutdown-await-timeout-ms` (default 5 s) bounds the wait.

A shutdown that could not complete cleanly raises `SHUTDOWN_INCOMPLETE`. Before restarting, confirm
the process actually exited and nothing else holds `--wal-dir`.

### Restarting

An ordinary restart needs no preparation and no flag: same `node_id`, same directory, same
`incarnation`. The node replays, rejoins, and anti-entropy fills whatever it missed.

Restarting **several** nodes is a different operation with a trap in it -- see
[10. Upgrades and restarts](10-upgrade.md).

### Reloading without a restart

Four files are re-read when you ask this node to read them:

```
curl -X POST http://127.0.0.1:9600/reload
```

| File | Flag |
|---|---|
| Membership | `--members-file` |
| Client tokens | `--client-token-file` / `--client-token-dir` |
| Client ACL | `--client-acl-file` |
| TLS key and trust stores | `--tls-*`, `--client-tls-*`, with `--tls-cert-rotation` |

All four go in together or none does, and the last good version stays in force for every one of them
if any fails to parse. Nothing reads these files at any other time, so you can edit them in place.

Everything else -- ports, `--wal-dir`, `--cluster-size`, timeouts -- takes a restart.
[7. Configuration](07-configuration.md#reload-vs-restart) has the full split, and the shape of the
reply.

---

## Troubleshooting

### The node will not start

Read down to what the data directory holds. The pattern underneath the table: state that is
**absent** is recovered from the cluster; state that is **present and contradictory** stops the
node, because there is nothing to derive from evidence in conflict with itself.

| Storage state | How it looks | What the node does |
|---|---|---|
| Nothing there | no `incarnation` file | `AWAITING_FLOOR`, then serves. A new cluster's first start and a replaced disk are the same event |
| Marker + `wal/` + `snap/` | normal | `REPLAYING`, then serves |
| Marker, but `wal/` and `snap/` empty | state removed under the marker | `AWAITING_FLOOR` -- the marker is evidence about identity, not proof of content |
| Marker + snapshot, `wal/` gone | snapshot-only | `AWAITING_FLOOR` -- values load from the snapshot, the ceiling comes from the cluster |
| Log compacted, `snap/` gone | log starts after the beginning | `AWAITING_FLOOR` -- the same gap from the other side |
| WAL tail truncated | indistinguishable from normal | `REPLAYING`, then serves -- nothing to do |
| WAL segment corrupt | replay throws `WalException(REPLAY_FAILED)` | `FAILED` -- see below |
| Marker unreadable | present but malformed | `FAILED` -- see below |

**A truncated tail is handled and needs no action.** An unclean shutdown loses up to
`--wal-force-interval-ms` (default 1 s) of appends; the surviving log is well-formed and simply
short. The promise ceiling covers it -- the node refuses ballots at or below the highest counter it
had reserved -- so a promise lost with the tail cannot be contradicted. The cost is one extra round
per key after the restart.

### Operator-attention states raised here

| State | Raised when | Action |
|---|---|---|
| `STORAGE_UNREADABLE` | replay or a committed snapshot failed; the node is `FAILED` and the loop is down | delete what is broken under `--wal-dir` and restart -- **never** from a copy |
| `WAL_DEGRADED` | the WAL cannot append (disk full, bad device); `/health` 503 | clear the device and restart. **Latches**, and the whole cluster stops collecting tombstones until it is back |
| `SNAPSHOT_FAILING` | a snapshot write failed | free space on `--wal-dir`. Not fatal -- the WAL is the record -- but replay time grows. Cleared by a snapshot that completes |
| `SHUTDOWN_INCOMPLETE` | a resource threw while closing | confirm the process exited and nothing holds `--wal-dir` before restarting |

`STORE_FULL` and the storage states that are about *another* member -- `STORAGE_CLONED`,
`PEER_STORAGE_ROLLED_BACK`, `UNACCOUNTED_KEYS_DROPPED` -- belong to
[4. Quorum and durability](04-quorum.md) and [3. Cluster and members](03-cluster.md).

### The node is up but serves nothing

`/health` 200 and `/ready` 503 is a node that is fine and not in the quorum. Two causes, and the
logs tell them apart at once: `AWAITING_FLOOR` means it started empty and is short of witnesses
([4. Quorum](04-quorum.md#a-node-that-starts-with-nothing)); `SERVING` with `/ready` 503 means its
handshaked peers do not reach the quorum line ([3. Cluster](03-cluster.md)).

Neither is fixed by restarting the node, and restarting an `AWAITING_FLOOR` node makes it worse: it
starts the wait over.

---

## Impact

| Action | Effect on the cluster |
|---|---|
| Stop or restart **one** node, `N`=3 | none visible. Quorum holds; clients fail over to another coordinator, so `NOT_READY` never reaches a caller |
| Restart a node that had lost its state | it waits in `AWAITING_FLOOR` until enough members answer. Leave its peers running |
| Two nodes down at `N`=3 | no linearizable operation completes; `?stale` reads are still served. See [4. Quorum](04-quorum.md#losing-quorum) |
| Deleting a data directory | that member returns as one with no state. Safe, and the standard repair -- it then has to *prove* a floor rather than assert one |

After any restart, the first write to each key may cost one extra round while proposers bump past
the reported floor. That is the whole of the visible cost.

---

## Recovery

**Delete what is broken and restart.** This is a normal repair, and there is no flag attached to it:

- **The log is unreadable** -- delete `wal/`. The node keeps the values in `snap/` and gets its
  ceiling from the cluster, which is faster than starting from nothing.
- **The marker is unreadable, or you would rather not decide which part is bad** -- delete the whole
  data directory. The node comes back as a member with no state at all.

Keep a copy of what you delete if you can. The node will not read it, but a torn segment is a fact
about the device, and the device is probably still there.

> **On restoring a node from a backup: don't.** A backup restores *values*, which anti-entropy would
> have restored anyway. What it cannot restore is the guarantee that the node's promise ceiling is
> not behind where it was -- and a ceiling that went backwards is the one form of amnesia that is
> silent and permanent. Clearing the directory is strictly safer: a node with nothing recovers a
> floor it can prove, where a node with a stale ceiling recovers nothing and tells no one.
> [9. Backup and recovery](09-backup.md) says what backups *are* for.

There is no "force start", no "ignore the log", and no "start empty anyway". Each would be a switch
whose only use is the case that silently corrupts the register, and a switch that exists gets used
at three in the morning.

---

## Related

- [3. Cluster and members](03-cluster.md) -- membership, replacing a node, a copied directory
- [4. Quorum and durability](04-quorum.md) -- why an empty node waits, and what a quorum protects
- [7. Configuration](07-configuration.md) -- every flag, precedence, reload vs restart
- [10. Upgrades and restarts](10-upgrade.md) -- rolling restart, and why a cold start is different
- Runbooks: [RB-02 restart a node](13-runbooks.md#rb-02-restart-a-node),
  [RB-08 a node that will not start](13-runbooks.md#rb-08-a-node-that-will-not-start)
