# discas documentation

One page to try it, then four sets, one per audience.

## [QUICKSTART.md](QUICKSTART.md) -- for people meeting discas

A three-node cluster and an HTTP agent on your laptop, and a key written and read back with `curl`.
Ten minutes, no Docker, nothing to configure -- and everything on it has been run as written:

| Question | Where |
|---|---|
| How do I get a cluster running at all? | [3. Start three nodes](QUICKSTART.md#3-start-three-nodes) -- by hand, or with `discas-admin init` |
| How do I talk to it without writing Java? | [4. Start the agent](QUICKSTART.md#4-start-the-agent) |
| What does a read and a write look like? | [5. Write and read a key](QUICKSTART.md#5-write-and-read-a-key) |
| What happens when a node dies? | [6. Try killing a node](QUICKSTART.md#6-try-killing-a-node) |

## [user/](user) -- for people building on discas

The [**User Guide**](user/README.md): the Java APIs, from your own code. Start at
[1. Getting started](user/01-getting-started.md), or jump by question:

| Question | Where |
|---|---|
| How do I read and write, and what does a failure mean? | [2. Keys and values](user/02-key-value.md) |
| What does `scan` / `watch` actually guarantee? | [3. Scan and watch](user/03-scan-and-watch.md) |
| How do I take a lock safely? | [4. Locks](user/04-locks.md) |
| Timeouts, TLS, tokens, threading | [5. Client setup](user/05-client-setup.md) |
| How do I run a node inside my own service? | [6. Embedding a node](user/06-embedding-a-node.md) |
| What must my own launcher wire up? | [7. Writing your own starter](user/07-custom-starter.md) |

## [agent/](agent) -- for people calling discas over HTTP

The [**agent manual**](agent/README.md): the HTTP/JSON surface `discas-agent` puts in front of a
cluster, for a caller that is not on the JVM. Curl-first, and the whole surface is three route
prefixes:

| Question | Where |
|---|---|
| Read, write, compare-and-set, list | [2. Keys and values](agent/02-kv.md) |
| Follow a key without polling it myself | [3. Blocking queries](agent/03-watch.md) |
| Take a lock over HTTP | [4. Locks](agent/04-locks.md) |
| Which flags, which credentials, which port | [5. Running an agent](agent/05-running.md) |
| What does this status mean, and may I retry? | [6. Errors](agent/06-errors.md) |

## [dev/](dev) -- for people working on discas

The [**Developer Guide**](dev/README.md): where the logic lives, how it works, how to change it, how
to know a change is correct, and what it can break. Start at
[1. Introduction](dev/01-introduction.md), or jump by question:

| Question | Where |
|---|---|
| Where is the logic I need? | [12. Reference](dev/12-reference.md) |
| How does it work? | [3. CASPaxos](dev/03-caspaxos.md) through [9. Agent and admin](dev/09-agent-and-admin.md) |
| How do I build, run and debug it? | [2. Development environment](dev/02-development.md) |
| How do I know my change is correct? | [10. Testing](dev/10-testing.md) |
| What does it cost? | [13. Performance](dev/13-performance.md) |
| What will my change break? | [11. Limitations and compatibility](dev/11-limitations.md) |

## [operator/](operator) -- for people running discas

The [**Operator Guide**](operator/README.md): a working handbook for the engineer responsible for a
cluster that is already serving traffic. Start at
[1. discas in context](operator/01-context.md), or jump by question:

| Question | Where |
|---|---|
| How do I start, stop or repair a node? | [2. The node](operator/02-node.md) |
| A member dropped out, or has to be replaced | [3. Cluster and members](operator/03-cluster.md) |
| Why did writes stop, and what does durability rest on? | [4. Quorum and durability](operator/04-quorum.md) |
| TLS, tokens, ACLs, and rotating them | [5. Access](operator/05-access.md), [15. PKI recipes](operator/15-pki-recipes.md) |
| What do I alert on? | [6. Monitoring and logs](operator/06-monitoring.md) |
| Which flag, and does it need a restart? | [7. Configuration](operator/07-configuration.md) |
| Backups, restores, standing up a cluster | [8. discas-admin](operator/08-admin.md), [9. Backup and recovery](operator/09-backup.md) |
| Rolling out a new version | [10. Upgrades and restarts](operator/10-upgrade.md) |
| What does this symptom mean? | [12. Troubleshooting](operator/12-troubleshooting.md) |
| Give me the procedure, not the argument | [13. Runbooks](operator/13-runbooks.md) |
| Look up a flag, metric, state or term | [14. Reference](operator/14-reference.md) |

The [root README](../README.md) carries the positioning: what discas is for and its limitations.
