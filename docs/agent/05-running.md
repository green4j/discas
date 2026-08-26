# 5. Running an agent

Only what a caller needs to have an agent answering. Deployment topology, node configuration and
certificate lifecycle are the operator's, in [docs/operator](../operator/README.md) --
[11. The HTTP agent](../operator/11-http-agent.md) covers running this one.

```bash
java -cp "$CP" io.github.green4j.discas.agent.starter.DisCasAgentStarter \
    --nodes 1=10.0.0.1:8001,2=10.0.0.2:8001,3=10.0.0.3:8001
```

Every flag has a `DISCAS_*` environment variable; the command line wins over the environment, which
wins over the default. `--help` prints the full list -- this page is the part you are likely to
touch.

## Finding the cluster

Exactly one of these two, never both:

| | |
|---|---|
| `--nodes 1=host:port,2=host:port,...` | a fixed list |
| `--nodes-file <path>` | `node.<id> = host:port` per line, **re-read on request** |

In file mode you edit the file and then ask for it to be read, which needs no restart:

```bash
curl -s -X POST localhost:8500/v1/agent/reload
```
```json
{"status":"applied","sources":[{"source":"/etc/discas/nodes.conf","outcome":"applied","detail":"3 nodes: 1=10.0.0.1:8001, 2=10.0.0.2:8001, 3=10.0.0.4:8001"}]}
```

Nothing reads the file until you make that call, so a half-saved edit cannot be picked up. The same
call also re-reads the TLS material, and the two are applied together or not at all.

The client stack captures its peer set at construction, so a reload is applied by building a **new**
client and swapping it in atomically: the next request uses the new list, and the retired client is
shut down -- **failing whatever was in flight on it** rather than draining. Expect a burst of
failures at the moment of a reload, and retry them the same way you would any other.

`200` means the set went in, `400` means it did not and the last good list is still in force. A file
that parses to the list already running is reported `unchanged` and not re-applied -- which is why
reordering the lines costs nothing. `GET` on this path is a `405`.

`--client-id` (default `agent`) is the identity in the connection handshake, and what the nodes'
authentication and per-prefix authorization key off. Give each agent its own.

## The HTTP surface

| | Default |
|---|---|
| `--http-bind` | `127.0.0.1:8500` |
| `--http-workers` | available processors |
| `--request-timeout-seconds` | `10` |

**The bind default is loopback on purpose.** An agent is a sidecar with a client's credentials to
the cluster; anything that reaches it can act as that client. Widening it should be deliberate, and
paired with something that authenticates the caller -- the agent itself does not.

`--request-timeout-seconds` bounds one client operation before the agent gives up and returns
[`504`](06-errors.md). It also sets the [blocking-query cap](03-watch.md#what-the-wait-actually-is),
one second below it. Raise it if you want long polls.

## Credentials to the cluster

Which mode applies is decided by the **nodes**; the agent supplies what that mode needs.

**Token** -- a shared secret in the handshake:

```bash
--token "$DISCAS_TOKEN"
```

**Server-authenticated TLS, identity from the token:**

```bash
--tls --tls-truststore /etc/discas/trust.p12 --tls-truststore-password "$TP" --token "$T"
```

**mTLS** -- add a keystore, and the identity becomes the certificate's `CN`, which **must equal**
`--client-id`:

```bash
--tls --tls-truststore /etc/discas/trust.p12 --tls-truststore-password "$TP" \
      --tls-keystore  /etc/discas/agent.p12  --tls-keystore-password  "$KP" \
      --client-id orders-agent
```

Both stores are PKCS12. A truststore is required whenever `--tls` is on. With
`--tls-cert-rotation` (default on), rotated client certificates are picked up by
`POST /v1/agent/reload` instead of a restart.

**Neither** -- plaintext, no credential. Works only if the nodes run permissive; anything else
refuses the handshake and every request fails with [`403`](06-errors.md).

Authorization is separate and lives on the nodes: a per-client prefix ACL decides what this
`client-id` may touch, whatever it authenticated with.

## Health

```bash
curl -s -X GET localhost:8500/v1/agent/health
```
```json
{"status":"ok","clientId":"agent","nodes":["1","2","3"]}
```

`GET` only; anything else is a `405`. `nodes` is read live, so after a `--nodes-file` reload it shows
the new list -- which makes this the way to confirm a reload landed.

**This is agent liveness and nothing more.** It answers `200` while the process is up, including
while the cluster has lost quorum and can serve nothing. Wire a container liveness probe to it;
do **not** build a cluster-health alert on it. To know whether the cluster is serving, ask the
cluster -- a read that succeeds is the honest check -- or scrape the nodes' own `/ready`.

## Observability

Metrics live on a **separate port**, because a scrape target and a data plane have different
audiences and the exposition names every node this agent talks to.

| | Default |
|---|---|
| `--observability-bind` | `127.0.0.1:9601` |
| `--observability-enabled` | `true` |
| `--observability-workers` | `1` |

```bash
curl -s -X GET localhost:9601/metrics
```
```
# HELP discas_reloads_total Reloads that were applied.
# TYPE discas_reloads_total counter
discas_reloads_total 0
```

Prometheus exposition, `discas_` prefixed. `9601` is one past the node's `9600`, so an agent and a
node on the same host are both scrapable without either being reconfigured.

Worth alerting on: reload failures (the agent is running on stale configuration), request failovers
and send failures (a coordinator is unhealthy), and handshake rejections (credentials or cluster id
are wrong).

## Shutting down

`SIGTERM`/`SIGINT`. The agent closes the HTTP server, then the client, failing anything in flight
rather than leaving callers hanging.
