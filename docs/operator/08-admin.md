# 8. discas-admin

`discas-admin` is the operator's command line: one program, one command per operation, each a
process that runs once and exits.

```
discas-admin dump    a running cluster  ->  a file
discas-admin load    a file             ->  a running cluster   (ordinary writes)
discas-admin init    a file             ->  N member directories + RUN.md, for a cluster that does not exist yet
discas-admin token   a client id        ->  the line that client's token file needs
```

It is deliberately not part of `discas-agent`. The agent is a long-lived sidecar that answers HTTP,
and a backup is neither a request nor a response -- it is an operator standing in front of a cluster
with a file. Behind an endpoint, taking a backup would have required running the agent, and `init`,
which writes a data directory before any cluster exists, could not have lived there at all.

**The one distinction to carry into everything below.** Putting *pairs* into a cluster is ordinary
work -- that is `load`, and it is a write per key like any other. Writing a *data directory* is not,
and is only ever done by `init`, for a member that has never run. A directory that has been a member
is never restored, copied or reused ([3. Cluster](03-cluster.md#a-copied-directory) says why), and
nothing here softens that.

---

## How to check

```sh
discas-admin --help              # the three commands
discas-admin dump --help         # a command's options, defaults and DISCAS_* variables
```

Without the distribution scripts, every `discas-admin <args>` below is equally:

```sh
CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)
java -cp "$CP" io.github.green4j.discas.admin.starter.DisCasAdminStarter <args>
```

The arguments and the `DISCAS_*` variables are identical -- see
[1. Context](01-context.md#two-ways-to-invoke-each-of-them).

Every command takes its options from flags **or** `DISCAS_*` variables, and `--token` belongs in the
environment: a token on a command line is a token in the process table.

Exit codes are the usual two: **2** means you asked wrongly and the usage is printed; **1** means it
did not work, with one line saying what. A message that says "the cluster did not answer within
30000ms" is kept as written rather than reduced to an exception class name.

---

## `dump`

```sh
discas-admin dump \
  --nodes 1=10.0.0.1:7002,2=10.0.0.2:7002,3=10.0.0.3:7002 \
  --out /backups/prod-$(date +%F).dump \
  users/ orders/
```

The addresses are **client** ports -- a dump is a client and talks where clients talk. Give the
**whole membership**: a scan needs a majority to answer, and a short list can fail to reach one.

Every positional argument is a key prefix to carry; with none, the whole key space is carried.
**The prefix set is your only filter and the whole of it.** Nothing in the dump path reads a value,
so a prefix that holds locks dumps locks -- keep lock keys under a prefix you do not dump, since a
lease is a deadline on this cluster's clock and means nothing in another.

**What it costs.** A scan followed by a **linearizable read per key** -- one consensus round each.
The alternative would be reading whatever the answering member holds locally, which is a backup that
quietly carries a stale value. Budget a round trip per key, and set `--connect-timeout-ms`
accordingly.

**It is not a point-in-time snapshot.** `scan` pages with a per-page quorum and there is no global
revision, so the key list comes from one instant and each value from another. A write racing the
dump lands on either side of it, and two keys written together may not both be in it. If the pairs
have to agree with each other, **stop writes first**.

**A dump that is not whole is refused, not read short.** The entry count and the checksum are written
when the dump commits and at no other time, so anything that interrupted it -- a killed process, a
full disk, a dropped connection -- leaves a file every reader rejects. A partial backup must never
read as a complete one. **Never edit a dump**: the checksum is what separates a backup from a file
that looks like one.

The command removes its output file if it fails, because a tool that reports failure and leaves a
file leaves somebody believing they have a backup.

---

## `load`

```sh
discas-admin load --nodes 1=10.0.0.1:7002,2=10.0.0.2:7002 --in /backups/prod.dump
```

Every pair goes in as an ordinary `put`, overwriting whatever is at that key. Nothing about where the
pairs came from travels with them -- no ballots, no promises, no identity -- and a writer racing the
load wins or loses by ballot order the way two writers always do.

**A merge by default.** Keys the dump does not mention are left alone, so a load restores what the
dump held and removes nothing that appeared since.

**A replacement on request:**

```sh
discas-admin load --nodes 1=10.0.0.1:7002 --in /backups/prod.dump --cleanup users/ orders/
```

`--cleanup` deletes every key under the given prefixes that the dump did not carry, which is the only
way to make a key space *equal* a dump rather than merely contain it. With no prefixes it takes the
**whole key space**, so the prefixes are what bounds the damage. Three honest limits:

- it holds the dump's **keys** (never its values) in memory, to answer "was this in it?";
- it deletes **unfenced**, and cannot tell a key somebody wrote during the load from a stale one;
- it is for a key space you have taken charge of, not one under live traffic.

Prefixes without `--cleanup` are refused rather than silently ignored: a load without cleanup writes
the dump's keys and touches nothing else, so a prefix would have nothing to bound.

**Interrupted?** The keys it reached are written and the rest are as they were. There is no
transaction and nothing to roll back -- which is also why a failed load leaves the cluster alone
rather than trying to undo itself.

---

## `init`

The only command that connects to nothing.

```sh
discas-admin init \
  --out-dir ./prod-2 \
  --cluster-id prod-2 \
  --members 1=10.0.0.1:7001,2=10.0.0.2:7001,3=10.0.0.3:7001 \
  --in /backups/prod.dump
```

`--members` is the **peer** membership -- where members talk to each other -- and its size is the new
`N`. Without `--in` the members are empty, which is still a cluster somebody has to stand up.

It writes one local folder:

```
RUN.md          what to do with all of this
members.conf    the membership, in the format --members-file reads
1/ 2/ 3/        one data directory per member
```

Each member directory holds that member's WAL with the dump's pairs, all at one fixed low ballot,
and an `incarnation` file **minted for that directory**. The members are byte-identical from their
first second apart from that marker -- so anti-entropy has nothing to reconcile, and no two of them
claim one identity.

On a terminal, `init` asks the handful of things `RUN.md` cannot know: the client port, where data
and configuration will live on the hosts, how clients will authenticate, whether there is an ACL
file, and whether TLS is on. Each is also a flag (`--client-port`, `--data-dir`, `--config-dir`,
`--client-auth`, `--client-token-file`, `--client-acl-file`, `--tls`, `--client-tls`), and with no
terminal -- a script, a CI job -- every question takes its default without asking. **None of them
changes a byte in the folder.** They exist so the start commands in `RUN.md` are complete rather
than full of placeholders.

### Then read `RUN.md`

It is generated, not a template, and it carries:

- **the four rules that are not negotiable** -- one directory, one member, forever; never copy a
  directory after a member has started on it; never point these at an existing cluster; start every
  member before sending traffic;
- an `scp` and a `discas-node` command **per member**, with that member's real paths and ports;
- how clients get in, given the auth mode you chose, and the file formats it needs;
- what clients may touch, given the ACL file you named or did not;
- how to check it worked -- a `dump` of the new cluster and the pair count to expect.

Passwords are not in it and must not be: it names the `DISCAS_*` variables instead, rather than
leaving a bracket for somebody to fill in wrongly at three in the morning.

`init` removes the whole output folder if it fails. Half a cluster's directories are worse than
none, because somebody would copy one to a host.

---

## `token`

The only command that neither connects to a cluster nor writes a file. It exists because a node
under `--client-auth token` reads a grammar nothing else could produce: the file holds a **PBKDF2
hash**, not the token, so provisioning by hand means choosing a work factor, salting, deriving,
base64-ing and assembling a `$`-separated record.

```sh
discas-admin token --client-id web-1 --ttl-days 90
```
```
client.web-1 = pbkdf2$210000$LnWseUklBA4Wo/qJpzkHuA==$h7oFhKlBOKeV4RZ0rPQNWgW6xdri7n8XLzli5TtOqs0=$1798...
discas-admin token: client web-1, 210000 iterations, expires 2026-11-22T09:14:03Z (90 days)
discas-admin token: token (shown once, store it now): YWLLmWOtrH3dnqLXuxTE8zKOIRe38N7WXLkUPwnbl_I
```

**Two streams, deliberately.** The record goes to standard output alone and unprefixed; the token
and everything explaining it go to standard error. So the redirect appends exactly what belongs in
the file, and the secret still reaches your terminal instead of scrolling into a config file:

```sh
discas-admin token -c reporter >> /etc/discas/tokens.conf
```

For the directory form, write it as `<clientId>.token` under `--client-token-dir` instead. Both are
hot-reloaded, so a node picks up a new client without a restart.

**The token is shown once and kept nowhere.** There is no command to recover it, by construction:
what the cluster stores is a hash, which is the point of storing a hash. A lost token is re-issued.

| Option | Meaning |
|---|---|
| `--client-id` (`-c`) | the client the token is for -- the id it presents, and the id the ACL file grants to. Required |
| `--ttl-days` | days until the record expires [default: 90] |
| `--token` | use this secret instead of generating one, for a token that comes from a secret manager |
| `--iterations` | the PBKDF2 work factor. Carried in the record, so raising it applies to new tokens without invalidating old ones. **Cannot be set below the default** -- the only honest direction for it is up |

Rotation is two records on one line: mint a second, append it after ` ; `, let the clients move, then
drop the first or let its expiry retire it ([5. Access](05-access.md#rotating-a-token)). Revocation
is deleting the record.

---

## Troubleshooting

| Symptom | Cause and action |
|---|---|
| `dump` or `load` reports the file rather than reading it | truncated, checksum mismatch, bytes after the trailer, or an entry count that disagrees. The file is not a whole dump and there is no partial mode. Take a fresh one |
| `dump` times out | the cluster could not reach a majority, or `--connect-timeout-ms` is short for the key count. Check `/ready` on the members first |
| `dump` reports "N keys were deleted mid-dump" | not a fault. Keys vanished between the scan and their read; the dump is complete for what still existed |
| A load's pairs are not all there | an interrupted load writes what it reached. Re-run it -- every write is idempotent in the sense that it overwrites |
| `init` refuses the output folder | it must not already hold anything. Point it somewhere new rather than clearing one that may hold a member's directory |
| A client with a fresh token is still refused | check the id: the record's key is `client.<clientId>`, and the client must present that same id. `ACCESS_DENIED` on the node says which |
| `token` refuses `--iterations` | it may only be raised above the default. A lower work factor weakens every token minted from then on |
| Two members claim one identity after a stand-up | somebody copied a directory instead of giving each member its own. See [RB-15](13-runbooks.md#rb-15-a-node-returned-from-a-copied-directory) |

---

## Impact

| Command | Effect on the cluster |
|---|---|
| `dump` | a consensus round per key, on top of your traffic. On a large key space this is real load -- schedule it |
| `load` | a write per key, at ordinary priority. Racing writers behave normally |
| `load --cleanup` | **deletes**, unfenced, everything under the prefixes that the dump did not carry. The prefixes are your blast radius |
| `init` | none. It connects to nothing |
| `token` | none until you put the line in a file; then that client can authenticate at the next reload |

---

## Recovery

- **A dump that will not read** cannot be repaired -- take another. If the cluster is gone and the
  dump was your only copy, the checksum has told you something true and unwelcome.
- **A `--cleanup` that deleted too much** is recovered by loading an earlier dump over the same
  prefixes. This is the reason to keep more than one.
- **An `init` folder you are unsure about** should be deleted and rebuilt. It connects to nothing,
  so rebuilding costs only the seeding time.

---

## Related

- [9. Backup and recovery](09-backup.md) -- what to back up, how often, and what none of it gives you
- [3. Cluster and members](03-cluster.md#changing-n) -- the `dump` -> `init` route to a new `N`
- Runbooks: [RB-10 take a backup](13-runbooks.md#rb-10-take-a-backup-and-verify-it),
  [RB-11 load into a live cluster](13-runbooks.md#rb-11-load-a-dump-into-a-live-cluster),
  [RB-12 stand up a cluster from a dump](13-runbooks.md#rb-12-stand-up-a-new-cluster-from-a-dump)
