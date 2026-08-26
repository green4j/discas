# 9. Agent and admin

## Context

Two front ends over the same client library, for two audiences.

The **agent** is a long-lived local sidecar that exposes a Consul-shaped HTTP/JSON surface, so a
caller in any language gets KV and locks without a Java client. The **admin CLI** is the opposite
shape: things an operator does once, standing in front of a cluster, with a file -- back it up, load
it, or build the directories for a cluster that does not exist yet.

They are separate for that reason. Behind an endpoint, taking a backup would require running the
agent, and `init` -- which writes a data directory *before* any cluster exists -- could not live there
at all.

## The agent

One `DisCasClient` to the configured cluster, fronted by the project's own `HttpServer`. The client
and the HTTP server run on their own event loops; handlers bridge client futures back onto the
worker thread with `response.async(...)`, so a continuation may touch the response directly.

Routes, all prefix-dispatched:

| Route | Covers |
|---|---|
| `/v1/kv/...` | `GET` (`?raw`, `?keys`, `?version=` blocking query), `PUT` (`?cas=<version>`), `DELETE` (`?cas=`) |
| `/v1/lock/...` | acquire (`?ttl=`, `?wait=`), renew, release, info -- token in `X-DisCas-Lock-Token` |
| `/v1/agent/health` | liveness plus the nodes this agent currently targets |
| `/v1/agent/reload` | `POST` only -- re-read the nodes file and TLS material, `200` applied / `400` refused |

A key's version travels as `X-DisCas-Version` and is what a blocking query passes back as
`?version=` and a fenced write passes as `?cas=`. It plays Consul's `X-Consul-Index` role but is
deliberately **not** named after it: the value is a client `Version`, and something a caller
round-trips through the HTTP surface and the Java one must not answer to two names depending on
which side of the agent it stands on. That rule also bans calling it a *cursor* anywhere: the API
already has one, `ScanPage.nextCursor()`, and it is a key to resume paging from. Where the two vocabularies do agree on a concept -- `Key`,
`Value`, `Flags`, `?cas=0` for create-if-absent -- Consul's spelling is kept verbatim. A bare `?cas` with no
version is a `400` rather than a silent unconditional put -- the caller asked for a precondition, and
there is no value-compared form to fall back on.

**The rule the HTTP surface follows: whatever the client API answers, the agent answers too.** Any
question the Java result type settles, a header settles here, rather than being left for the caller
to derive:

| Client API | Over HTTP |
|---|---|
| `GetResult.version()`, `Version` from `put`/`delete`, `CasResult.version()` | `X-DisCas-Version` on **every** read and write, fenced or not, including a `404` |
| `WatchResult.changed()` | `X-DisCas-Changed` |
| `ScanPage.quorumReached()` / `respondedNodes()` / `clusterSize()` | `X-DisCas-Complete` / `X-DisCas-Responded` / `X-DisCas-Cluster-Size` |
| `LockWriteResult.status()` | `status` in the release/renew body, beside the `released`/`renewed` boolean |

`update` has no HTTP form, and cannot: its transform is caller code, and the loop only makes sense
where that code runs. Over HTTP the loop is the caller's, built from a read and a `?cas=` write --
which is why every read hands back a version.

The node list is re-read on `POST /v1/agent/reload` and at no other time. Since a client captures its
peer set at construction, a membership change is applied by building a **new** `DisCasClient` and
swapping it in atomically (`ReloadableClient`), retiring the old one off-thread. The publish happens
on the HTTP worker that served the reload, so the swap is complete before the response is written and
the reply describes state that is already in force.

The HTTP server itself is documented in its own class javadoc, which is a full manual -- see the
class-level javadoc of
`discas-common/src/main/java/io/github/green4j/discas/common/http/server/HttpServer.java`.

## The admin CLI

`discas-admin <command>`, each command declaring its own options:

```bash
discas-admin dump  --nodes 1=host:7001,... --out prod.dump  users/ orders/
discas-admin load  --nodes 1=host:7001,... --in prod.dump --cleanup users/
discas-admin init  --out-dir ./prod-2 --cluster-id prod-2 --members 1=host:7001,... --in prod.dump
discas-admin token --client-id web-1 --ttl-days 90
```

The command list is a plain hand-written registry -- nothing is scanned for, and a command not in the
list does not exist. What every command shares (help, option parsing, output prefix, progress line,
undoing what a failed run created) is inherited from `AbstractCommand`; the *options* are not shared,
because a dump needs a cluster and prefixes while an init needs a membership and a folder.

`token` is the odd one and worth knowing about: it neither connects to a cluster nor writes a file.
It is what makes the token grammar producible -- a token file holds a PBKDF2 hash, so provisioning
without it means choosing a work factor and assembling a `$`-separated record by hand.
`TokenSpecs` owns that grammar in both directions, parser and formatter together, so the two cannot
drift; the command splits its output -- the record on stdout, the secret on stderr -- so
`>> tokens.conf` appends the line without swallowing the token.

### The dump format

A file of its own, deliberately neither a WAL segment nor a snapshot -- so that no procedure ever
reads *copy this file into the data directory*, one short step from copying a whole directory.

```
magic "DCDUMP" | formatVersion | headerLength | header{createdAt, prefixes}
entries: repeated { int keyLength, key, int valueLength, value }
end sentinel (-1) | entryCount | CRC32 of everything above
```

The count and CRC are in the **trailer**, so an unfinished dump is not a dump: a run abandoned
halfway never gets one and the reader refuses it outright rather than reading it short. The reader
proves the file end to end before handing out a single pair, and streams both passes, so a dump
larger than the heap still reads.

A dump carries **no identity** -- no cluster id, no node id, no ballots. A backup here is a portable
set of pairs, and every id it could carry is a name from the old cluster that means nothing in the
new one.

### The three operations

- **`dump`** -- scan the prefixes, then a *linearizable read per key*. Values are carried, never
  inspected: a lock record is a value like any other, so asking for a prefix that holds locks dumps
  the locks.
- **`load`** -- every pair goes in as an ordinary `put`, a full round like any other write. The
  cluster is told nothing about where the pairs came from. `--cleanup` turns a merge into a
  replacement of the named prefixes.
- **`init`** -- writes one data directory per member plus the membership file and a `RUN.md`, and
  connects to nothing. Each pair is written as a single `ACCEPT` at one fixed ballot identical on
  every member, so the members are byte-identical from their first second and anti-entropy has
  nothing to reconcile. It refuses a directory that already holds state.

`dump` and `load` live in `discas-client` (`ClusterDump`, `ClusterLoad`) rather than in the CLI, so
an application that already holds a client gets backups without standing up a process, and there is
one implementation of *what a dump is*.

## Source map

| Concern | File |
|---|---|
| Agent runtime | `discas-agent/src/main/java/io/github/green4j/discas/agent/DisCasAgent.java` |
| Handler base | `discas-agent/src/main/java/io/github/green4j/discas/agent/AbstractHandler.java` |
| KV / locks / health | `discas-agent/src/main/java/io/github/green4j/discas/agent/KvHandler.java`, `LockHandler.java`, `HealthHandler.java` |
| Client hot-swap | `discas-agent/src/main/java/io/github/green4j/discas/agent/ReloadableClient.java` |
| HTTP server (manual in its javadoc) | `discas-common/src/main/java/io/github/green4j/discas/common/http/server/HttpServer.java` |
| CLI entry, command registry | `discas-admin/src/main/java/io/github/green4j/discas/admin/starter/DisCasAdminStarter.java`, `AbstractCommand.java` |
| Operations | `discas-admin/src/main/java/io/github/green4j/discas/admin/DumpOperation.java`, `LoadOperation.java`, `InitOperation.java` |
| Dump/load over a client | `discas-client/src/main/java/io/github/green4j/discas/client/dump/ClusterDump.java`, `ClusterLoad.java` |
| Dump format | `discas-common/src/main/java/io/github/green4j/discas/common/dump/DumpCodec.java`, `DumpWriter.java`, `DumpReader.java` |
| Seeding a member directory | `discas-node/src/main/java/io/github/green4j/discas/node/seed/MemberSeed.java` |
| The generated runbook | `discas-admin/src/main/java/io/github/green4j/discas/admin/runbook/Runbook.java` |

## Tests

| Question | Suite |
|---|---|
| The HTTP surface end to end against a real cluster | `discas-int-test/src/test/java/io/github/green4j/discas/agent/AgentHttpIntegrationTest.java` |
| Nodes-file reload and client swap | `discas-int-test/src/test/java/io/github/green4j/discas/agent/AgentNodesFileReloadTest.java`, `ReloadableClientSwapTest.java` |
| The agent over TLS with authorization | `discas-int-test/src/test/java/io/github/green4j/discas/agent/AgentTlsIntegrationTest.java` |
| Dump framing, truncation, tampering | `discas-common/src/test/java/io/github/green4j/discas/common/dump/DumpCodecTest.java` |
| Dump a cluster of three, seed five, read it back | `discas-int-test/src/test/java/io/github/green4j/discas/admin/SeededClusterTest.java` |
| The HTTP server itself | `discas-common/src/test/java/io/github/green4j/discas/common/http/server/HttpServerTest.java` and siblings |

## How to change it safely

- **A new agent route is a handler plus a registry line**, and it must map `ClientErrorCode` to
  status rather than matching error text -- that is what makes `ACCESS_DENIED` a 403 and not a 500.
- **Keep the dump format's trailer semantics.** The count and CRC being last is what makes a
  truncated backup unreadable instead of quietly short.
- **Never inspect a value in dump or load.** Only the application knows what lives where; filtering
  by content would be this code claiming to know the key space better than whoever laid it out.
- **`init` writes; `load` talks to a cluster.** Keep that split -- the whole reason `init` exists is
  that the cluster is not running.
