# 11. The HTTP agent

`discas-agent` is an **optional** sidecar that puts an HTTP/JSON front end in front of the cluster,
for callers that are not on the JVM. It is not a cluster member: it holds no data, takes no part in
consensus, and its loss costs you nothing but the callers routed through it. It is a client with a
port.

That is the opposite of what "agent" means in Consul, and it is the naming trap most likely to cost
you an hour. The cluster member is [`discas-node`](02-node.md); this is a convenience in front of
it.

Run one **per host**, next to the callers, and bind it locally. A remote agent adds a network hop
and a single point of failure in exchange for nothing -- the client library and the agent both talk
to the whole membership and both fail over between coordinators.

The complete HTTP surface -- routes, request and response shapes, status codes and what may be
retried -- is the [agent manual](../agent/README.md). This page is only what an operator needs.

---

## How to check

```sh
curl -s localhost:8500/v1/agent/health
```
```json
{"status":"ok","clientId":"agent","nodes":["1","2","3"]}
```

`nodes` is read live, so after a `--nodes-file` reload it shows the new list -- which makes this the
way to confirm a reload landed.

**This is agent liveness and nothing more.** It answers 200 while the process is up, including while
the cluster has lost quorum and can serve nothing. Wire a container liveness probe to it; do **not**
build a cluster-health alert on it. To know whether the *cluster* is serving, ask the cluster -- a
read that succeeds is the honest check -- or scrape the nodes' own `/ready`
([6. Monitoring](06-monitoring.md)).

```sh
curl -s localhost:9601/metrics
```

Metrics are on a separate port, defaulting to `127.0.0.1:9601` -- one past the node's 9600, so an
agent and a node on the same host are both scrapable without either being reconfigured.

Worth alerting on: **reload failures** (the agent is running on stale configuration), **request
failovers and send failures** (a coordinator is unhealthy), and **handshake rejections**
(credentials or cluster id are wrong).

---

## Operations

### Configuration

| Group | Options |
|---|---|
| **Cluster connection** | `--nodes` or `--nodes-file` (mutually exclusive; the file has the same format as a node's `--members-file`), `--client-id`, `--token` |
| **HTTP front end** | `--http-bind`, `--http-workers` |
| **Observability** | `--observability-bind`, `--observability-workers`, `--observability-enabled` |
| **Runtime** | `--request-timeout-seconds`, the `--client-max-*` frame and buffer limits |
| **Security** | `--tls`, `--tls-keystore`, `--tls-truststore`, `--tls-cert-rotation` |

Same rules as everywhere: a `DISCAS_*` variable per option, `CLI > ENV > DEFAULT` per value, an
effective-configuration table at startup ([7. Configuration](07-configuration.md)). `discas-agent
--help` is the authoritative list.

**`--nodes-file` is worth preferring** over `--nodes`: it can be re-read on request, so an agent
follows a membership change without a restart, and it is the same file the nodes already read.

```sh
curl -s -X POST localhost:8500/v1/agent/reload
```

That call re-reads the nodes file and the agent's TLS material together, applying both or neither,
and answers `200` when the set went in and `400` when it did not. Nothing reads those files at any
other time, so an edit in progress is never picked up half-written. A malformed file leaves the
previous list in force, and a file that means what is already running is reported `unchanged`.
Applying a new list retires the old client, **failing whatever was in flight on it**, so expect a
burst of request failures at the moment of a reload.

### What the ids in `--nodes` are, and are not

`--nodes 1=10.0.0.1:7002,2=10.0.0.2:7002` looks like it addresses nodes by name. It does not -- a
client reaches a node by **address only**, and the id is a label:

- **Not used to route.** A coordinator is chosen by hashing the key and indexing into the node list,
  so what decides which node answers is a node's **position in the list**, not its id.
- **Not in the handshake.** `CLIENT_HELLO` carries the protocol version, the *client* id and a
  credential; the response carries a status, the cluster size and the node's clock. No node id
  travels either way, so **nothing cross-checks the id you wrote** against the node that answered.
- **Not used to select or verify a certificate.** On the client port the connection authenticates
  the *certificate chain* against the trust store; there is no endpoint identification and no node
  identity in it. The `discas://<cluster>/<node>` SAN binding that makes an id cryptographic exists
  on the **peer mesh only** ([5. Access](05-access.md#peer-mesh)).

What the ids are for is **diagnosis**: every log line, metric label and observer callback about a
connection names one, so `connectionLost(n2)` reads better than an address. The grammar is also
deliberately the same as `--members-file`, so one file describes the cluster to both nodes and
agents.

Two practical consequences:

1. **A wrong id is never detected.** It costs you nothing at runtime and makes every log line about
   that node a lie. Keep the ids the same as the nodes' own `--node-id`.
2. **List the nodes in the same order everywhere.** Coordinator affinity follows list position, so
   two clients that list the same members in different orders pick different coordinators for the
   same key -- which turns key affinity into a ballot duel
   ([4. Quorum](04-quorum.md#same-key-contention)).

### The agent as a client

The agent authenticates to the cluster the way any client does: `--client-id` is the identity the
nodes' authorization rules see, `--token` is its credential, and its `--tls-*` options configure the
**client** side of the client port's TLS. Give it its own client id and its own ACL grants -- an
agent shared by several applications is a single identity with the union of their privileges, which
is worth deciding on purpose rather than by default ([5. Access](05-access.md)).

`--token` belongs in `DISCAS_TOKEN`, not on the command line.

### Starting and stopping

```sh
discas-agent \
  --nodes-file /etc/discas/members-client.conf \
  --client-id web-agent \
  --http-bind 127.0.0.1:8500
```

Or, without the start scripts:

```sh
CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)

java -cp "$CP" io.github.green4j.discas.agent.starter.DisCasAgentStarter \
  --nodes-file /etc/discas/members-client.conf \
  --client-id web-agent \
  --http-bind 127.0.0.1:8500
```

`SIGTERM` or `SIGINT` stops it: the HTTP server closes first, then the client, failing anything in
flight rather than leaving callers hanging. There is no drain to wait for and no durable state to
protect -- restarting an agent is free.

Note the port in `--nodes` / `--nodes-file` is the **client** port of each node, not the peer port.
An agent that will not connect at all is usually pointed at the peer port.

---

## Troubleshooting

| State | Raised when | Normal for | Action |
|---|---|---|---|
| `NODE_UNREACHABLE` | the agent lost its connection to a node and has not got it back | **1 min** | check that node and the path to it. Requests fail over meanwhile, so this is not an outage until enough of them are down |
| `ACCESS_DENIED` | a node refused the agent's `CLIENT_HELLO` | -- | check `--client-id` and `--token` against that node's `--client-auth` and ACL file |
| `FOREIGN_CLUSTER` | nodes reported a different cluster id, or two reported different cluster sizes | -- | the agent is pointed at the wrong deployment, or at two |
| `CLUSTER_SIZE_INVALID` | a node answered with a cluster size that cannot be true | -- | fix `--cluster-size` where that node runs |
| `RELOAD_FAILED` | a reload read `--nodes-file` and it did not parse | -- | fix the file and reload again; the previous list is still in force |

The agent shares the node's operator-attention taxonomy rather than having one of its own, so
`discas_operator_attention > 0` covers both processes with one rule
([6. Monitoring](06-monitoring.md#operator-attention)).

| Symptom | Cause |
|---|---|
| Every request fails, health says `ok` | the cluster lost quorum. The agent is fine; ask the nodes |
| The agent starts and reaches nobody | pointed at peer ports instead of client ports, or a firewall between them |
| Requests are slow but succeed | a coordinator is unhealthy and the agent is failing over on each request. Find it via the node metrics |
| A membership edit had no effect | `--nodes` is static -- use `--nodes-file`. With the file, nothing re-reads it until you `POST /v1/agent/reload` |

---

## Impact

| Action | Effect |
|---|---|
| Restarting the agent | its own in-flight requests fail. Nothing cluster-side; it holds no state |
| Losing the agent entirely | only the callers routed through it. Consensus, data and quorum are untouched |
| Binding `--http-bind` to a routable address | your data plane is now reachable from the network. The agent authenticates *to* the cluster, not its own callers |
| Sharing one agent across applications | one client identity with the union of their ACL grants |

**The agent does not authenticate its own callers.** Whatever reaches `--http-bind` speaks with the
agent's identity and the agent's privileges. Bind it to loopback, or put your own authentication in
front of it.

---

## Related

- [agent manual](../agent/README.md) -- the full HTTP surface, for the people calling it
- [5. Access](05-access.md) -- the client credentials the agent presents
- [6. Monitoring](06-monitoring.md) -- why agent health is not cluster health
- [7. Configuration](07-configuration.md) -- precedence, secrets, reload
