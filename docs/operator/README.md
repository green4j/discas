# discas Operator Guide

For people **running** discas in production: how to start a cluster, keep it healthy, tell what it
is doing, and put it back when it is not. Not a tour of features -- a working handbook for the
engineer who is responsible for a cluster that is already serving traffic.

Each topic page opens with two or three paragraphs of context and then gets practical. Theory is
here only where it changes what you do, which in discas is more often than you would expect: the
store has **no leader**, **no log** and **no failure detector**, and several procedures that look
familiar from Raft systems are wrong here for that reason.

New to the system, or arriving from Consul? Start at
[1. discas in context](01-context.md) -- it is short, and it includes the naming trap
(`discas-agent` is *not* the cluster member).

---

## By question

| I want to | Page |
|---|---|
| Understand what I am running | [1. discas in context](01-context.md) |
| Start, stop, restart or repair a node | [2. The node](02-node.md) |
| Deal with a member that dropped out, or replace one | [3. Cluster and members](03-cluster.md) |
| Understand a write outage, or the storage model | [4. Quorum and durability](04-quorum.md) |
| Set up TLS, tokens or ACLs, and rotate them | [5. Access](05-access.md) |
| Decide what to alert on | [6. Monitoring and logs](06-monitoring.md) |
| Change a setting, and know whether it needs a restart | [7. Configuration](07-configuration.md) |
| Back up, restore, or stand up a cluster | [8. discas-admin](08-admin.md), [9. Backup and recovery](09-backup.md) |
| Roll out a new version | [10. Upgrades and restarts](10-upgrade.md) |
| Run the HTTP sidecar | [11. The HTTP agent](11-http-agent.md) |
| Find out what a symptom means | [12. Troubleshooting](12-troubleshooting.md) |
| Follow a procedure under pressure | [13. Runbooks](13-runbooks.md) |
| Look up a flag, metric, state or term | [14. Reference](14-reference.md) |
| Issue certificates | [15. PKI recipes](15-pki-recipes.md) |

## Contents

| | |
|---|---|
| [1. discas in context](01-context.md) | the model, the four programs, ports, and what discas does not do |
| **[2. The node](02-node.md)** | files, start and stop, the start model, reload, storage repair |
| **[3. Cluster and members](03-cluster.md)** | the membership file, replacing a member, changing `N`, a copied directory |
| **[4. Quorum and durability](04-quorum.md)** | majorities, the promise floor, losing quorum, capacity, tombstones |
| **[5. Access](05-access.md)** | the two trust domains, the four client modes, token and ACL formats, rotation |
| [6. Monitoring and logs](06-monitoring.md) | `/health` vs `/ready`, the one alert rule, metrics, logging |
| [7. Configuration](07-configuration.md) | precedence, secrets, reload vs restart, tuning |
| [8. discas-admin](08-admin.md) | `dump`, `load`, `init`, and the `RUN.md` that `init` generates |
| [9. Backup and recovery](09-backup.md) | what to back up, how often, and what none of it gives you |
| [10. Upgrades and restarts](10-upgrade.md) | rolling restart, cold start, version changes, rollback |
| [11. The HTTP agent](11-http-agent.md) | the optional sidecar, and why its health is not cluster health |
| [12. Troubleshooting](12-troubleshooting.md) | symptom -> page. Start here in an incident |
| [13. Runbooks](13-runbooks.md) | RB-01 ... RB-15, in one shape |
| [14. Reference](14-reference.md) | flags, ports, metrics, states, error codes, glossary |
| [15. PKI recipes](15-pki-recipes.md) | keytool, openssl, cert-manager, Vault, ACM PCA |

**2 to 5 are the centre of this guide.** Everything else serves them: monitoring tells you when to
open one, configuration is how you change what they describe, and the runbooks are what you do at
three in the morning without reading the argument again.

## Shape of a topic page

Pages 2 through 11 all follow the same order, so you can jump to the part you need:

> **Context** -- what this is, why it matters to you, what can go wrong
> **How to check** -- commands, and what a healthy answer looks like
> **Operations** -- what you can do, and how
> **Troubleshooting** -- symptom -> checks -> cause -> action
> **Impact** -- what your action does to the cluster
> **Recovery / rollback** -- what to do when it did not go to plan
> **Related** -- neighbouring topics and runbooks

Pages 1, 12, 13, 14 and 15 have their own shapes: an overview, an index, procedures, a lookup table,
and command recipes.

**Each explanation lives in exactly one place.** A topic page explains; troubleshooting is a one-line
index into those explanations; a runbook is steps with the argument linked, not repeated; the
reference is names without prose. If you find the same paragraph twice, one of them is a bug.

## Three things to know before reading anything else

**There is no leader.** Any member can coordinate a write. There is no election to lose, no leader
to recover, and no timer whose value decides whether the cluster stays healthy.

**`N` is frozen while the cluster runs.** Quorum is `N/2 + 1`, and two members that disagree about
`N` refuse to connect -- which is what makes split-brain impossible and resizing a planned operation.

**A member is never restored from a copy of its data directory.** The repair for damaged storage is
an *empty* directory: the node then proves a promise floor from the cluster instead of asserting a
stale one. A restored copy replays as a clean log and is the one damage nothing here can see.

---

The other documentation sets: [user/](../user/README.md) for people building on discas,
[agent/](../agent/README.md) for people calling it over HTTP, [dev/](../dev/README.md) for people
working on discas itself. The [root README](../../README.md) carries the positioning -- what discas
is for, how it compares to etcd, ZooKeeper and Consul, and its limitations.
