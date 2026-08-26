# Discas: CASPaxos Key-Value Store

`discas` (DIStributed CAS) is a tiny zero-dependency Java implementation of a CASPaxos-based replicated key-value
store. It holds the **shared state a set of microservices agrees on** -- locks, leadership,
configuration, ownership -- and is deliberately **not a large platform-wide, multi-tenant service**.

It runs embedded in your own JVM or as a standalone node, with no runtime dependencies.

---

## Documentation

| Set | For |
|---|---|
| [**Quickstart**](docs/QUICKSTART.md) | people **meeting** discas: three nodes and an agent on localhost, and a key read back with `curl` |
| [**User Guide**](docs/user/README.md) | people **building on** discas: the client API, locks, scan and watch, embedding a node, writing a starter |
| [**Agent manual**](docs/agent/README.md) | people **calling** discas over HTTP/JSON, from any language |
| [**Operator Guide**](docs/operator/README.md) | people **running** discas: the node, membership, quorum, access, monitoring, backup, upgrades, runbooks |
| [**Developer Guide**](docs/dev/README.md) | people **working on** discas: architecture, CASPaxos, storage, transports, testing, [performance](docs/dev/13-performance.md) |

Everything is indexed in [docs/README.md](docs/README.md).

---

## What discas is for

A **coordination store**: linearizable per-key compare-and-set, lease locks with fencing tokens,
prefix scan, and a coalescing watch -- the primitives distributed systems need in order to agree on
small pieces of shared state.

- **Distributed locks and leader election** -- critical sections, singleton guards, job or partition
  claiming, with a fencing token to guard the protected resource.
- **Configuration and feature flags** -- a value plus [`watch`](docs/user/03-scan-and-watch.md#watch),
  so readers pick up changes without polling.
- **Ownership and assignment registries** -- which instance currently holds a shard, a queue, a
  device, a tenant.
- **Counters, barriers, and state machines** -- anything a CAS loop expresses safely.
- **Coordination between cooperating services** -- the case discas started from: services
  orchestrating each other without operating a separate etcd/ZooKeeper/Consul cluster.

Consumers reach it as a **Java library** -- embedded in the participating processes or as a plain
client -- or over **HTTP/JSON** through [discas-agent](docs/agent/README.md).

Where etcd and Consul use Raft and ZooKeeper uses ZAB -- one elected leader ordering every write into
a replicated log -- discas uses **CASPaxos** ([Rystsov 2018](https://arxiv.org/abs/1802.07000)): any
node can propose, state is a per-key *register* rather than a log, and replicas converge by periodic
anti-entropy digest comparison rather than log catch-up. Because there is no leader to replace,
there is **no failure detector and no timer that has to track RTT** -- a round completes as soon as
any majority answers, and a peer that is down is simply not part of that majority. The peer protocol
is plain TCP on two operator-assigned ports: no gossip, no UDP, no keyring to distribute. What each
of those properties costs is [Boundaries](#boundaries); what it measures is
[13. Performance](docs/dev/13-performance.md).

**This makes one stretched cross-region cluster a first-class topology rather than a tuning
exercise.** Raft's heartbeat has to sit near the round-trip time and its election timeout an order of
magnitude above it, identically on every member, so stretching a leader-based cluster means retuning
all of them and accepting that a latency spike or a slow fsync can escalate into a lost leader and a
write outage. discas has no such timer to get wrong: a cluster spanning continents runs on the same
settings as one in a rack, a slow disk costs throughput and cannot trigger a leadership change, and
a client talks to the member next to it rather than hopping to a leader that may be a region away.
Read-heavy regional traffic need not cross at all -- `ReadConsistency.SERIALIZABLE` is answered
locally, three orders of magnitude cheaper than a linearizable read across regions.

The price is quorum: every member votes, so a write pays two inter-region round trips and a
linearizable read one, and there is no non-voting replica to place near distant readers.

**Deployment.** The same node runs in two shapes. *Embedded* puts it inside the
participating JVMs, and asks them to accept a fixed, identity-bound deployment shape in return.
*Standalone* runs it as its own process, with the agent in front of it, so the store outlives any
single application. On an elastic platform the recommended embedded topology pins `N` (3 or 5)
instances as members and leaves the rest of the fleet as pure clients, so only the small fixed part
is stateful. The trade in full:
[Embedding a node](docs/user/06-embedding-a-node.md#the-trade-a-fixed-identity-bound-deployment-shape).

---

## Boundaries

What discas does not do, whether by design or as the price of the shape above. Each links to where
it is argued in full.

**Data model**

- **No multi-key transactions, and no total order across keys.** One register at a time is the whole
  model. You can serialize a group of keys behind one lease lock and apply saga-style compensating
  writes, but that is cooperative mutual exclusion: a mid-sequence crash leaves partial state, and
  isolation holds only against actors that also take the lock. [Keys and values](docs/user/02-key-value.md)
- **No MVCC and no ordered change feed.** A key is a register with no history, so `watch` reports
  the *current* value when the version advances -- rapid successive writes coalesce and intermediate
  values are missed. [Scan and watch](docs/user/03-scan-and-watch.md#watch)
- **`scan` is a key enumeration, not a snapshot.** It completes once a majority answers, failing
  rather than returning a short list, so every key committed before it started appears -- but
  different keys may reflect different instants. [Scan and watch](docs/user/03-scan-and-watch.md)
- **`cas` fences on the key's version, and there is no value-compared form.** Comparing bytes is
  unsafe under ABA: a coordinator the client had given up on can apply the swap a second time after
  an intervening inverse write, and "not applied" and "applied then reverted" are the same
  observation. `update(key, transform)` is the read-transform-write loop, and it retries a **lost
  compare and nothing else**. [7. The client](docs/dev/07-client.md)
- **Reads are linearizable by default and cost a full round.** `ReadConsistency.SERIALIZABLE` skips
  it and may be stale; lock helpers always read linearizably. [Keys and values](docs/user/02-key-value.md)
- **Same-key contention duels.** Coordinator affinity normally serializes a key behind one peer, but
  a failover or a client with a different node list can put two coordinators on one key. This is the
  workload discas handles least well. [13. Performance](docs/dev/13-performance.md)
- **Not a database.** Coordination state, not bulk application data: no query language, no secondary
  index, no range scan beyond a key prefix.

**Membership and topology**

- **`N` is frozen while the cluster runs, and online resizing is not planned.** A reload covers
  member addresses, certificates, and constant-`N` replacement -- not `N` itself, since `N` determines
  quorum. It is a durability decision made when the system is designed, and adding members to a
  CASPaxos cluster makes it *slower*: every key lives on every member, so another member adds a
  message to every round and buys no throughput.
  [3. Cluster and members](docs/operator/03-cluster.md#changing-n)
- **Every member is a voter.** No observers, learners or witnesses, so quorum spans every region a
  member sits in and a member cannot be added purely to serve local reads.
  [11. Limitations](docs/dev/11-limitations.md)
- **No discovery and no self-managed clustering.** No catalog, no health-checked registry, no DNS,
  no autoscaling, no leader-managed reconfiguration. Membership is an operator-supplied file, and
  nothing joins on its own.

**Security and operations**

- **Not built for isolation between distrusting tenants.** Per-client prefix authorization limits
  blast radius between cooperating consumers; it will not hold a hostile one at bay.
- **Lease locks use wall-clock for expiry** -- the Redlock-class caveat. The CAS itself is
  consensus-safe; clock skew changes *perceived* expiry, which is why the `fencingToken`, not the
  lease, is the safety mechanism. [Locks](docs/user/04-locks.md)
- **`--proposal-expiry-ms` bounds the whole write; `--round-timeout-ms` bounds one attempt.** Set the
  first below `round-timeout x (retries + 1)` and it silently shortens the retry chain; raise both
  together for a slow link. [7. Configuration](docs/operator/07-configuration.md#tuning)
- **Performance figures are one-JVM figures**, and there is no HTTP benchmark at all.
  [13. Performance](docs/dev/13-performance.md)

---

## Security

Both the peer mesh and the client-facing port have an **opt-in** access-control layer. Defaults are
plaintext, appropriate on a trusted network and nowhere else; enabling security does not change the
protocol.

- **Peer to peer** -- mutual TLS with a shared cluster CA, membership enforced in the `PEER_HELLO`
  handshake against a reloadable member list with no coordinator, and zero-disruption cert
  rotation.
- **Client to node** -- mutual TLS (identity bound to `CN=<clientId>`) or server-authenticated TLS
  plus a token, layered with per-client, default-deny prefix authorization. Auth and policy live in
  files or secret managers, **never in the KV store**.

Operations: [5. Access](docs/operator/05-access.md) and
[15. PKI recipes](docs/operator/15-pki-recipes.md). Design:
[8. Transport and security](docs/dev/08-transport-security.md). The runnable reference assemblies
are `SecureClientAclExample` and `SecureClusterFileMembersExample` in `discas-example`.


---

## Build

`./gradlew build` compiles and tests everything; every module targets `--release 11`. The module
map, the task/tag/profile matrix, and how to run a local cluster are
[2. Development environment](docs/dev/02-development.md) and
[12. Reference](docs/dev/12-reference.md).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.