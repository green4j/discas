# 9. Backup and recovery

Backups in discas are less load-bearing than in most stores, and it is worth being clear about why
before deciding a schedule. **A node is never recovered from a backup.** A member's data directory
is never restored, copied or reused -- a byte-continuous but older copy replays as a clean log, rolls
the promise ceiling back below promises that member already made, and does so silently. The repair
for damaged storage is an *empty directory*, not a restored one, and the node then proves a floor
from the cluster rather than asserting a stale one.

So a backup is not for node failure; quorum handles that. A backup is for the failures quorum does
not cover: **a whole cluster lost, an operator or application that deleted the wrong thing, and a
migration.** In all three the unit is *pairs*, not directories -- which is exactly what
`discas-admin dump` produces.

The second thing to be clear about: **there is no incremental dump and no point-in-time recovery.**
There is no global revision to diff against and no log to replay from, so every dump is a full one,
and the schedule is a function of two things you already know -- how long a full dump takes on your
key space (a round trip per key) and how much re-entered data you could live without.

---

## How to check

A backup nobody has restored is a hypothesis. The check is cheap and it is the only one that means
anything:

```sh
# Read one back into a scratch cluster and compare.
discas-admin init --out-dir /tmp/restore-check --cluster-id restore-check \
  --members 1=127.0.0.1:17001,2=127.0.0.1:17002,3=127.0.0.1:17003 \
  --in /backups/prod-2026-08-23.dump
# then follow /tmp/restore-check/RUN.md, and dump the result back
```

The generated `RUN.md` ends with exactly this check and states the pair count to expect. Fewer means
a member was started on the wrong directory, or traffic reached the cluster before every member was
up.

Short of a full restore, a dump's integrity is checkable by trying to read it: `load` and `init`
refuse anything that is not whole, so a dump that a command will open is a dump with a valid
checksum and a matching entry count.

---

## Operations

### What to back up

| | Back it up? | Why |
|---|---|---|
| The key/value pairs | **yes** -- `discas-admin dump` | the only thing quorum cannot give back if the whole cluster is gone |
| A member's data directory | **no**, ever | it is never restored. Copying one is the one damage the cluster cannot see |
| The membership file | yes, with your configuration | trivial to rebuild, annoying to rebuild under pressure |
| Token and ACL files | yes, with your configuration | same, and they are secrets -- treat them as such |
| Key and trust stores | per your PKI policy | discas holds no authority over them; your CA does |

The last three are ordinary configuration management. Only the first needs discas-specific tooling.

### Taking one

```sh
discas-admin dump \
  --nodes 1=10.0.0.1:7002,2=10.0.0.2:7002,3=10.0.0.3:7002 \
  --out /backups/prod-$(date +%F).dump
```

Without the start scripts, `java -cp "$CP" io.github.green4j.discas.admin.starter.DisCasAdminStarter`
takes the same arguments ([1. Context](01-context.md#two-ways-to-invoke-each-of-them)).

Full detail, including what a dump costs and what it is not, is in
[8. discas-admin](08-admin.md#dump). Two habits worth having:

- **Keep the dumps where the cluster is not.** A backup on the same disks as the members is a copy
  of the failure, not an answer to it.
- **Read one back occasionally**, as above.

**Stop writes first if the pairs have to agree with each other.** A dump is a scan and then a read
per key, with no global revision behind it, so two keys written together may not both be in it. For
a backup meant only to bound data loss this rarely matters; for a migration it always does.

### Restoring pairs into a cluster that exists

`discas-admin load` -- a merge by default, a replacement with `--cleanup` and a prefix.
[8. discas-admin](08-admin.md#load).

Use it when the cluster is healthy and the *data* is wrong: a bad deployment wrote nonsense, an
application deleted a prefix, a migration needs its pairs moved across.

### Standing a cluster back up

`discas-admin init` -- for a cluster that does not exist yet, seeded from a dump.
[8. discas-admin](08-admin.md#init).

Use it when the cluster itself is gone, or when you are deliberately building a new one: a new `N`,
a new region, a scratch copy to test a restore against.

### Choosing between them

| Situation | Command |
|---|---|
| One member's disk failed | **neither.** Empty directory, same seat, restart -- [3. Cluster](03-cluster.md#replacing-the-hardware-under-a-member) |
| A quorum is down but the members exist | **neither.** Bring them back -- [4. Quorum](04-quorum.md#losing-quorum) |
| The data is wrong, the cluster is fine | `load` |
| The cluster is gone | `init`, then cut traffic over |
| Changing `N`, or migrating | `dump` -> `init` -> cut over -- [3. Cluster](03-cluster.md#changing-n) |

---

## Troubleshooting

| Symptom | Cause and action |
|---|---|
| A dump is refused when you come to use it | it is not a whole dump, and there is no partial mode. Something interrupted the writer. Take a fresh one, and never edit a dump |
| A restored cluster has fewer pairs than expected | a member was started on the wrong directory, or traffic arrived before every member was up. `RUN.md` states the number to expect |
| A dump taken under load is internally inconsistent | not a fault, a property. Stop writes for a migration |
| Locks restored into a new cluster do nothing | correct. A lease is a deadline on the old cluster's clock and a fencing token orders only within one cluster. Keep lock keys under a prefix you do not dump |
| A `Version` from the old cluster does not fence against the new one | correct, and it is why a migration is a **cut-over** rather than a gradual drift of traffic between two clusters |

---

## Impact

| Action | Effect |
|---|---|
| `dump` on a live cluster | a consensus round per key, on top of your traffic. Schedule it like any other batch job |
| `load` on a live cluster | a write per key. Ordinary priority, ordinary contention |
| `load --cleanup` | **deletes** unfenced under the given prefixes. Not for a key space under live traffic |
| `init` | none -- it connects to nothing |

---

## What none of this gives you

Worth knowing before you plan around it:

- **Cross-key consistency in a live dump.** Stop writes if you need it.
- **A restore of a member into a running cluster.** Not a missing feature -- a member's directory is
  never brought back, and the pairs go in through `load` instead.
- **A list of what changed since a backup.** No log, no revision, nothing to diff. The blast radius
  of an incident is recovered from the application's own record, not from here.
- **Point-in-time recovery.** Every dump is full and stands alone.
- **Locks that survive a move.** See the table above.

---

## Related

- [8. discas-admin](08-admin.md) -- the three commands in full
- [2. The node](02-node.md#recovery) -- why a node is repaired with an empty directory, not a backup
- [4. Quorum and durability](04-quorum.md) -- what quorum recovers on its own
- Runbooks: [RB-10 take a backup and verify it](13-runbooks.md#rb-10-take-a-backup-and-verify-it),
  [RB-11 load a dump into a live cluster](13-runbooks.md#rb-11-load-a-dump-into-a-live-cluster),
  [RB-12 stand up a new cluster from a dump](13-runbooks.md#rb-12-stand-up-a-new-cluster-from-a-dump)
