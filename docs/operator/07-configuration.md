# 7. Configuration

Every discas program is configured the same way, and there is no configuration file format to learn.
Each option is settable by **either** a long flag **or** its `DISCAS_*` environment variable; the
variable name is derived mechanically from the flag -- upper-snake, `-` becomes `_` -- so `--wal-dir`
pairs with `DISCAS_WAL_DIR` and `--client-token-file` with `DISCAS_CLIENT_TOKEN_FILE`. There is
nothing to look up.

Precedence is **`CLI > ENV > DEFAULT`**, decided **independently for each value**. That is the
useful part: a deployment can put the whole configuration in the environment and override one flag
on the command line for one run, without the two sources becoming a merge you have to reason about.

The authoritative list of options, defaults and variable names is `--help` on the program itself.
This page is about the decisions around them -- where secrets go, what takes effect without a
restart, and how to tell what the process is actually running with, which is not always what your
deployment manifest says.

---

## How to check

Every program prints its effective configuration at startup, with each value's source and secrets
masked:

```
discas-node effective configuration:
  PROPERTY          VALUE                    SOURCE
  node-id           n1                       CLI
  cluster-id        prod                     ENV
  client-bind       0.0.0.0:7002             CLI
  wal-dir           /var/lib/discas/n1       ENV
  round-timeout-ms  5000                     DEFAULT
  tls-keystore-password  ****                ENV
```

**That table is the truth**, not the unit file and not the Helm values. When a node behaves as
though it has a setting you did not give it, read the table before reading anything else.

```sh
discas-node  --help     # every flag, its default and its DISCAS_* variable
discas-agent --help
discas-admin <command> --help
```

All three also run from the jars with identical arguments --
`io.github.green4j.discas.node.starter.DisCasNodeStarter`,
`...agent.starter.DisCasAgentStarter` and `...admin.starter.DisCasAdminStarter`, per
[1. Context](01-context.md#two-ways-to-invoke-each-of-them).

---

## The option groups

`discas-node` groups its options, and the groups are a reasonable map of what there is to configure:

| Group | Examples | Notes |
|---|---|---|
| **Identity** | `--node-id`, `--cluster-id`, `--cluster-size` | all required in practice. `--cluster-size` is `N` and is **frozen at startup** |
| **Membership** | `--members-file`, `--members` | mutually exclusive. The file form is reloadable; the list form is not |
| **Transport** | `--peer-bind`, `--client-bind` | `--client-bind` is **required** -- discas reserves no well-known ports. `--peer-bind` defaults to this node's own entry in the member list |
| **Storage** | `--wal-dir`, `--wal-max-file-bytes`, `--snapshot-interval-seconds`, `--snapshot-retention` | `--wal-dir` is required and is one directory per node, never shared |
| **Runtime** | the timeouts, `--store-heap-fraction`, `--repair-interval-seconds`, `--tombstone-sweep-interval-seconds` | see [Tuning](#tuning) |
| **Observability** | `--observability-bind`, `--observability-workers`, `--observability-enabled`, and the `--peer-max-*` / `--client-max-*` frame and buffer limits | [6. Monitoring](06-monitoring.md) |
| **Security (TLS/mTLS)** | `--tls`, `--tls-keystore`, `--tls-truststore`, `--tls-cert-rotation` | the **peer** domain |
| **Security (client access)** | `--client-auth`, `--client-token-file`/`-dir`, `--client-acl-file`, `--client-tls-*` | the **client** domain -- [5. Access](05-access.md) |

`discas-agent` has the same shape with fewer groups: cluster connection, HTTP front end,
observability, runtime, TLS.

## Secrets

Every password option accepts a `DISCAS_*` variable, and that is where passwords belong. A password
on a command line is visible in the process table to every user on the host; a password in the
effective-configuration table is masked either way, but that only protects the log.

Generated runbooks say this rather than leaving a bracket for somebody to fill in: `discas-admin
init` writes `DISCAS_TLS_KEYSTORE_PASSWORD` and its siblings into the `RUN.md` by name.

---

## Reload vs restart

Four sources are re-read on request. Everything else takes a restart.

| Reloadable | Flag | On a bad file |
|---|---|---|
| Membership | `--members-file` | refused, previous list stays (`MEMBERS_REJECTED`) |
| Client tokens | `--client-token-file` / `--client-token-dir` | refused, previous stays (`RELOAD_FAILED`) |
| Client ACL | `--client-acl-file` | refused, previous stays (`RELOAD_FAILED`) |
| TLS key and trust stores | `--tls-*`, `--client-tls-*` with `--*-cert-rotation` | refused, previous material stays |

**A file is read when you ask for it to be read, and at no other time:**

```
curl -X POST http://127.0.0.1:9600/reload
```

That is the whole trigger, and both of its consequences are the reason for it.

**You can edit in place.** Nothing looks at a file between the moment your editor truncates it and
the moment it is saved, so an editor that saves on a timer, a half-finished ACL, or a `sed -i` that
takes a second cannot be read as a revision -- because nobody is reading. There is no window to race
and no temp-file-and-rename dance to remember.

**A set of files applies as a set.** One call re-reads *all* four sources, and it parses everything
before it publishes anything: if any of them is refused, **none** is applied and the node goes on
enforcing exactly what it was enforcing before. So a certificate and its private key, or a members
list and the ACL that names it, change together or not at all. Nothing half-applies.

The reply is the answer to "did that take effect" -- `200` when the set went in, `400` when it did
not, and a line per source either way:

```json
{"status":"applied","sources":[
  {"source":"/etc/discas/members.conf","outcome":"applied","detail":"3 nodes: 1=node-1:9001, 2=node-2:9001, 3=node-3:9001"},
  {"source":"/etc/discas/acl.conf","outcome":"unchanged","detail":"byte-identical to what is in force; not applied"}]}
```

| `outcome` | What it means |
|---|---|
| `applied` | read, parsed, differed from what was in force, and is now in force |
| `unchanged` | read and parsed, and there was nothing to apply -- see below |
| `failed` | did not parse. The last good version stays, and **nothing else in this reload was applied either** |
| `unreadable` | caught mid-write, so nothing is known about it and nothing was decided. Reload again once the writer has finished |
| `not-applied` | this file was fine and is being held back, because another source in the same reload was refused |

`unchanged` is an answer and not silence, because it is usually the interesting one: **reordered
lines, reordered grants, whitespace and comments are not changes.** They reach the parser, produce
the same configuration, and are reported as `unchanged` rather than applied. Getting it when you
expected `applied` normally means you edited a copy, or the host you edited is not the host you
reloaded.

**A refused reload is never an outage.** The last good version stays in force, on every source.

**Every application also leaves a log line**, so an edit is checkable after the fact and not only in
the reply:

```
Reloaded /etc/discas/acl.conf: applied -- 2 clients: reporter -> report/:GS; web-1 -> app/:GPCD session/:GPCDS
Reloaded /etc/discas/members.conf: applied -- 3 nodes: 1=node-1:9001, 2=node-2:9001, 3=node-3:9001
Unchanged /etc/discas/acl.conf: the file changed but says what is already in force; not applied
```

The report names what is now in force, sorted, so two of them can be compared -- and the first one
is written at startup, when the whole configuration is new.

**Reload every member.** Each node reads its own copy of these files; a change applied to some
members and not others is the state to avoid, and nothing in the cluster propagates it for you.
The agent has the same call at `POST /v1/agent/reload` for its own nodes file and TLS material.

What the line never carries is the material the file exists to protect. A token store reports which
clients are provisioned, how many records each has and when the last one expires -- never a hash,
salt or work factor. TLS material reports the leaf's subject, serial, issuer and expiry, which every
handshake shows the other side anyway -- never the private key or its password. A log is read by
more people than the file is.

**Requires a restart:** every port, `--wal-dir`, `--cluster-size`, every timeout and limit, the
authentication *mode*, and turning TLS on or off. Note the distinction in the security domain: the
**material** rotates hot, the **mode** does not.

---

## Tuning

Timeouts exist and every one is configuration rather than a constant. What distinguishes discas from
a Raft store here is *consequence*, not presence: there is no heartbeat interval, no election
timeout and no failure detector, so **no timer's value governs whether the cluster stays healthy**.
A badly chosen timeout fails the request that hit it; it cannot destabilise the cluster or cost you
a leader.

| Setting | Default | What it bounds |
|---|---|---|
| `--round-timeout-ms` | 5 s | one consensus round. Each retry restarts it |
| `--round-max-retries`, `--round-retry-backoff-ms`, `--round-retry-jitter-ms` | 3, 50 ms, 50 ms | the retry chain |
| `--proposal-expiry-ms` | 30 s | the **whole write**, from the first attempt, not reset by retries |
| `--peer-response-timeout-ms` | 10 s | one deadline for the two background exchanges that ask every peer and then act on whoever replied -- an anti-entropy page and a tombstone sweep's purge checks. Not a failure detector: expiry defers work rather than failing it |
| `--repair-interval-seconds` | 10 min | anti-entropy cadence |
| `--tombstone-sweep-interval-seconds` | 10 s | one key per sweep **per node**; the cluster rate is `N` times this |
| `--promise-eviction-interval-seconds` | 5 min | promise housekeeping |
| `--wal-force-interval-ms` | 1 s | how much of the tail an unclean shutdown can lose |
| `--no-quorum-backoff-ms` | 2 s | how long a determinate no-quorum verdict stands before the node tries again |
| `--shutdown-await-timeout-ms` | 5 s | the drain on `SIGTERM` |
| `--peer-reconnect-backoff-ms` -> `--peer-reconnect-backoff-cap-ms` | 100 ms doubling to 5 s | reconnect attempts |
| `--peer-force-reconnect` | `false` | whether a re-addressed member's healthy connection is dropped immediately |

The defaults clear typical inter-region round trips by a wide margin -- a two-round-trip write across
a 400 ms link still lands well inside 5 s -- so a cross-region cluster does not normally need any of
them changed. Raise them only for genuinely pathological links.

**Two of them interact, and the interaction is the one thing worth understanding.** `roundTimeout`
caps one attempt and each retry restarts it, so on its own it says nothing about how long a
coordinator may keep trying to commit a write the client has already given up on. `proposalExpiry`
is measured from the first attempt and is *not* reset by retries: past it, the coordinator abandons
the operation. That is what turns an indeterminate answer into a bounded one -- *this write may still
apply within `proposalExpiry` of when you sent it, and never afterwards*.

Whichever budget runs out first wins, so a `--proposal-expiry-ms` below
`round-timeout x (round-max-retries + 1)` silently shortens the retry chain rather than bounding it.
With the defaults the chain is about 20 s, so 30 s bounds it with headroom. **Raise both together**
for a slow link.

Frame and buffer limits (`--peer-max-*`, `--client-max-*`) are validated against the store's key and
value limits at startup, so a configuration that could not carry a maximum-size record is rejected
rather than failing later on a large value.

---

## Troubleshooting

| Symptom | Look at |
|---|---|
| A setting you gave has no effect | the effective-configuration table. It records the source of every value -- `DEFAULT` there means neither your flag nor your variable was seen |
| The node refuses to start with a config message | it validates combinations, not just values: `token` mode without a token store, `mtls` without a trust store, `--client-tls` without a keystore. The message names the missing option |
| An edit to a reloadable file did nothing | first, whether you called `POST /reload` on *that* host -- nothing reads the file until you do. Then the reply's `outcome` for that file: `unchanged` means it parsed to what was already running (usually a copy edited, or a cosmetic change), `failed` gives the reason it was refused, and `not-applied` means another file in the same reload was refused and took this one down with it |
| A member-file edit was refused | it must define exactly `--cluster-size` members and include this node. Adding or removing a member changes `N` and is always ignored -- [3. Cluster](03-cluster.md#what-a-reload-does) |
| Writes fail faster than your timeout | `--proposal-expiry-ms` is shorter than the retry chain, above |

---

## Impact

| Change | Effect |
|---|---|
| Any reloadable file | applies on `POST /reload`, no dropped connections |
| Any other option | a node restart, which at `N`>=3 costs nothing visible if done one at a time |
| `--cluster-size` | a cluster-wide operation with a write outage -- [3. Cluster](03-cluster.md#changing-n) |
| Lowering `--wal-force-interval-ms` | less exposure on an unclean shutdown, less throughput. Never a correctness change |
| Raising `--store-heap-fraction` | more heap for the store, less for everything else |

---

## Related

- [2. The node](02-node.md) -- where the files are, and what a restart involves
- [5. Access](05-access.md) -- the security options in full, and what each mode requires
- [6. Monitoring](06-monitoring.md) -- the observability options
- [14. Reference](14-reference.md#discas-node-flags) -- every flag in one table
