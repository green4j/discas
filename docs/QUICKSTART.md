# Quickstart

A three-node cluster and an HTTP agent on your laptop, and a key written and read back with `curl`.

You need a JDK 17 or newer to build (the artifacts themselves target Java 11) and `curl`.

> This is a **throwaway cluster on loopback with no authentication and no TLS** -- the defaults are
> open, which is the right setting for this page and the wrong one for anything else. Before running
> discas where it matters, read [5. Access](operator/05-access.md).

---

## 1. Build the commands

```bash
./gradlew :discas-node:installDist :discas-agent:installDist :discas-admin:installDist

export PATH="$PWD/discas-node/build/install/discas-node/bin:\
$PWD/discas-agent/build/install/discas-agent/bin:\
$PWD/discas-admin/build/install/discas-admin/bin:$PATH"

discas-node --help | head -3
```

Prefer not to touch `PATH`? Every command below also runs from the jars, with identical arguments:

```bash
CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)

java -cp "$CP" io.github.green4j.discas.node.starter.DisCasNodeStarter    # discas-node
java -cp "$CP" io.github.green4j.discas.agent.starter.DisCasAgentStarter  # discas-agent
java -cp "$CP" io.github.green4j.discas.admin.starter.DisCasAdminStarter  # discas-admin
```

See [1. discas in context](operator/01-context.md#two-ways-to-invoke-each-of-them).

## 2. Pick ports

discas reserves no well-known port numbers, so on one machine you assign every one of them. This
page uses:

| | Node 1 | Node 2 | Node 3 | Agent |
|---|---|---|---|---|
| Peer (members talk here) | 7101 | 7102 | 7103 | -- |
| Client (clients and the agent talk here) | 7201 | 7202 | 7203 | -- |
| Observability (`/health`, `/ready`, `/metrics`) | 9601 | 9602 | 9603 | 9604 |
| HTTP API | -- | -- | -- | 8500 |

**One trap worth knowing before you hit it.** A node's observability port defaults to
`127.0.0.1:9600` and the agent's to `127.0.0.1:9601` -- chosen so a node and an agent on the same
host do not collide. Run *three* nodes on one host and that spacing runs out, which is why the agent
gets an explicit `--observability-bind` below.

```bash
export DISCAS_HOME=/tmp/discas-quickstart
rm -rf "$DISCAS_HOME" && mkdir -p "$DISCAS_HOME"
```

## 3. Start three nodes

Two ways. **A** is faster to read; **B** is what you would actually do for a cluster that is going
to live somewhere.

### A. By hand

No files to prepare: a node creates its own data directory, and `--peer-bind` defaults to this
node's own entry in the member list, so it need not be repeated.

```bash
for i in 1 2 3; do
  discas-node \
    --node-id $i --cluster-id quickstart \
    --members 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
    --client-bind 127.0.0.1:720$i \
    --wal-dir "$DISCAS_HOME/$i" \
    --observability-bind 127.0.0.1:960$i \
    > "$DISCAS_HOME/node-$i.log" 2>&1 &
done
```

### B. With `discas-admin init`

`init` builds a folder holding one data directory per member, the membership file they share, and a
generated `RUN.md` with the start command for each one. It connects to nothing -- this is how you
prepare a cluster that does not exist yet, seeded from a dump (`--in`) or empty.

```bash
discas-admin init \
  --out-dir "$DISCAS_HOME/cluster" \
  --cluster-id quickstart \
  --members 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
  --client-port 7201 \
  --data-dir "$DISCAS_HOME" --config-dir "$DISCAS_HOME"

cat "$DISCAS_HOME/cluster/RUN.md"
```

`RUN.md` carries the four rules that are not negotiable -- one directory, one member, forever; never
copy a directory after a member has started on it; never point these at an existing cluster; start
every member before sending traffic -- then an `scp` and a `discas-node` command per member.

**On one machine, change one thing.** `init` assumes each member is on its own host, so it gives all
three the same `--client-bind` port. Give each its own, and start them where they already are
instead of `scp`-ing:

```bash
for i in 1 2 3; do
  discas-node \
    --node-id $i --cluster-id quickstart \
    --members-file "$DISCAS_HOME/cluster/members.conf" \
    --peer-bind 127.0.0.1:710$i \
    --client-bind 127.0.0.1:720$i \
    --wal-dir "$DISCAS_HOME/cluster/$i" \
    --observability-bind 127.0.0.1:960$i \
    > "$DISCAS_HOME/node-$i.log" 2>&1 &
done
```

### Check they formed a cluster

```bash
sleep 10
for i in 1 2 3; do echo -n "node $i: "; curl -s 127.0.0.1:960$i/ready; echo; done
```

```json
{"status":"ready","nodeId":"1","state":"SERVING","walDegraded":false,"clusterSize":3,
 "quorumSize":2,"peersHandshaked":2,"quorumAvailable":true,"canServeStaleReads":true, ...}
```

`"state":"SERVING"` with `"peersHandshaked":2` on every node is a formed cluster. If one says
`AWAITING_FLOOR`, it started with no state and is waiting for its peers -- leave the others running
and it will join ([4. Quorum](operator/04-quorum.md#a-node-that-starts-with-nothing)).

## 4. Start the agent

The agent is an optional HTTP/JSON front end for callers that are not on the JVM. It is **not** a
cluster member: it holds no data and takes no part in consensus. Point it at the nodes' **client**
ports.

```bash
discas-agent \
  --nodes 1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203 \
  --client-id quickstart-agent \
  --http-bind 127.0.0.1:8500 \
  --observability-bind 127.0.0.1:9604 \
  > "$DISCAS_HOME/agent.log" 2>&1 &

sleep 5
curl -s 127.0.0.1:8500/v1/agent/health
```

```json
{"status":"ok","clientId":"quickstart-agent","nodes":["1","2","3"]}
```

That is agent liveness only -- it answers `ok` while the process is up, even if the cluster has lost
quorum. To know whether the *cluster* is serving, ask the cluster, which is what the next section
does.

## 5. Write and read a key

```bash
curl -s -X PUT --data-binary 'blue' http://127.0.0.1:8500/v1/kv/app/theme
```
```json
{"ok":true}
```

```bash
curl -s http://127.0.0.1:8500/v1/kv/app/theme
```
```json
{"Key":"app/theme","Value":"Ymx1ZQ==","Flags":0}
```

`Value` is base64 -- the surface mirrors Consul's, and a value is arbitrary bytes. For the bytes
themselves, ask for them:

```bash
curl -s 'http://127.0.0.1:8500/v1/kv/app/theme?raw'      # -> blue
curl -s 'http://127.0.0.1:8500/v1/kv/?keys'              # -> ["app/theme"]
```

### Compare-and-set

Every read answers with the key's version in `X-DisCas-Version`. That version is what fences a write,
and it is the point of the whole store: a write applies only while the key is still where you read
it.

```bash
V=$(curl -s -o /dev/null -D - http://127.0.0.1:8500/v1/kv/app/theme \
    | grep -i x-discas-version | tr -d '\r' | awk '{print $2}')

curl -s -X PUT --data-binary 'green' "http://127.0.0.1:8500/v1/kv/app/theme?cas=$V"
```
```json
{"swapped":true,"value":"Z3JlZW4="}
```

Send the same, now stale, version again and it loses -- and the answer carries the value that won, so
a loser recomputes without a second read:

```bash
curl -s -X PUT --data-binary 'red' "http://127.0.0.1:8500/v1/kv/app/theme?cas=$V"
```
```json
{"swapped":false,"value":"Z3JlZW4="}
```

**A losing compare is a result, not a failure.** That distinction is most of what makes this store
usable -- [Keys and values](user/02-key-value.md).

### Delete

```bash
curl -s -X DELETE http://127.0.0.1:8500/v1/kv/app/theme     # -> {"ok":true}
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8500/v1/kv/app/theme   # -> 404
```

## 6. Try killing a node

The cluster tolerates losing a minority. With `N`=3, one node:

```bash
pkill -f -- '--node-id 3'
curl -s -X PUT --data-binary 'yes' http://127.0.0.1:8500/v1/kv/app/still-works
```
```json
{"ok":true}
```

Writes keep working: two of three is a majority, and the agent failed over to a node that could
still reach one. Nothing had to be reconfigured and nothing declared node 3 dead -- a peer that is
down is simply not part of a quorum.

Kill a second and the majority is gone:

```bash
pkill -f -- '--node-id 2'
curl -s -m 20 -X PUT --data-binary 'no' http://127.0.0.1:8500/v1/kv/app/broken
curl -s 'http://127.0.0.1:8500/v1/kv/app/still-works?raw&stale'
```
```
{"error":"CAS failed: Timeout"}
yes
```

Linearizable writes stop, and reads that tolerate staleness keep being answered from the surviving
node's local committed state. Start the two nodes again and the cluster resumes on its own -- there
is no readmission step ([4. Quorum](operator/04-quorum.md#losing-quorum)).

## 7. Tear down

```bash
pkill -f DisCasNodeStarter
pkill -f DisCasAgentStarter
rm -rf "$DISCAS_HOME"
```

---

## Where to go next

| You are | Read |
|---|---|
| Writing an application against it | [User Guide](user/README.md) -- the Java client, locks, scan and watch |
| Calling it over HTTP from any language | [Agent manual](agent/README.md) -- the full surface, curl-first |
| Going to run it somewhere real | [Operator Guide](operator/README.md) -- start with [5. Access](operator/05-access.md), because everything on this page was unauthenticated |
| Working on discas itself | [Developer Guide](dev/README.md) |

Two things this page skipped that matter as soon as it is not a laptop: **authentication and TLS**
are off, and this cluster's `N` is 3 and **frozen** -- resizing is a planned operation, not a live
one ([3. Cluster and members](operator/03-cluster.md#changing-n)).
