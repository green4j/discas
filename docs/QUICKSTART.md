# Quickstart

A three-node cluster and an HTTP agent on your laptop, and a key written and read back with `curl`.

You need Docker and `curl`. Nothing is built here -- the image comes from Docker Hub and carries all
three commands -- so no JDK, no Gradle, no clone. Building from source is a contributor's path, not
this one; it is in [2. Development](dev/02-development.md).

> This is a **throwaway cluster with no authentication and no TLS** -- the defaults are open, which
> is the right setting for this page and the wrong one for anything else. Before running discas
> where it matters, read [5. Access](operator/05-access.md).

---

## 1. Get the image

```bash
export DISCAS_IMAGE=green4j/discas:0.0.5
docker pull "$DISCAS_IMAGE"
```

`latest` and the minor tag `0.0` also exist and move on every release; a pinned version is what you
want for anything you intend to reproduce. The image is `linux/amd64` and `linux/arm64`.

**One image, three commands.** The first argument after the image selects which one runs, exactly
as the `discas-node` / `discas-agent` / `discas-admin` scripts do:

```bash
docker run --rm "$DISCAS_IMAGE" node --help | head -3
```

| Argument | Command | Main class |
|---|---|---|
| `node` (the default) | `discas-node` | `DisCasNodeStarter` |
| `agent` | `discas-agent` | `DisCasAgentStarter` |
| `admin` | `discas-admin` | `DisCasAdminStarter` |

Every option below is also an environment variable -- `--node-id` is `DISCAS_NODE_ID`,
`--http-bind` is `DISCAS_HTTP_BIND` -- which is usually the better fit for a container. Flags win
over the environment, which wins over the defaults. See
[1. discas in context](operator/01-context.md#two-ways-to-invoke-each-of-them).

## 2. Make a network

```bash
docker network create discas-quickstart
```

Containers on a user-defined network reach each other by container name, and each one has its own
address. That removes the port juggling a single host forces: **every node can use the same ports
inside the network**, and only what you publish to the host has to be unique.

| | Node 1 | Node 2 | Node 3 | Agent |
|---|---|---|---|---|
| Peer, in-network (members talk here) | 7001 | 7001 | 7001 | -- |
| Client, in-network (clients and the agent talk here) | 7002 | 7002 | 7002 | -- |
| Observability (`/health`, `/ready`, `/metrics`), published on the host | 9601 | 9602 | 9603 | 9604 |
| HTTP API, published on the host | -- | -- | -- | 8500 |

**What the image already does for you.** Its entrypoint defaults the binds to `0.0.0.0` inside the
container and the WAL to `/var/lib/discas`, because a loopback bind is unreachable from outside a
container and would be the first thing everyone hit. So `--client-bind`, `--observability-bind` and
`--wal-dir` are all left off below. They are defaults, not overrides: pass any of them and yours
wins.

`--peer-bind` is not defaulted by the image and does not need to be -- it falls back to this node's
own entry in the member list, which inside the container resolves to that container's address.

## 3. Start three nodes

Two ways. **A** is faster to read; **B** is what you would actually do for a cluster that is going
to live somewhere.

### A. By hand

Nothing to prepare: a node creates its own data directory on first start.

```bash
for i in 1 2 3; do
  docker run -d --name discas-$i --network discas-quickstart \
    -p 960$i:9600 \
    "$DISCAS_IMAGE" node \
      --node-id $i --cluster-id quickstart \
      --members 1=discas-1:7001,2=discas-2:7001,3=discas-3:7001
done
```

The WAL lives in each container's own filesystem, which is enough for this page: it survives
`docker stop` and `docker start` -- which section 6 depends on -- and is thrown away with
`docker rm`. For a cluster whose data you intend to keep, mount it, as **B** does.

### B. With `discas-admin init`

`init` builds a folder holding one data directory per member, the membership file they share, and a
generated `RUN.md` with the start command for each one. It connects to nothing -- this is how you
prepare a cluster that does not exist yet, seeded from a dump (`--in`) or empty.

```bash
mkdir -p ~/discas-quickstart

docker run --rm -v "$HOME/discas-quickstart:/out" "$DISCAS_IMAGE" admin init \
  --out-dir /out/cluster \
  --cluster-id quickstart \
  --members 1=discas-1:7001,2=discas-2:7001,3=discas-3:7001 \
  --client-port 7002 \
  --data-dir /var/lib/discas --config-dir /var/lib/discas

cat ~/discas-quickstart/cluster/RUN.md
```

> **Bind mounts and your Docker host.** A home directory is shared with the VM by every common
> setup; `/tmp` is not shared by some of them (Colima, for one), and a mount that is not shared
> silently becomes an empty root-owned directory the container cannot write -- `init` then stops
> with `permission denied`. Keep the path under your home directory and this does not arise.

`RUN.md` carries the four rules that are not negotiable -- one directory, one member, forever; never
copy a directory after a member has started on it; never point these at an existing cluster; start
every member before sending traffic -- then an `scp` and a `discas-node` command per member.

**Those commands assume one host per member, which is the case worth designing for and not the one
you are in.** Here the three members share a machine, so the step `RUN.md` writes as `scp` is a
mount, and each member's directory goes to its own container:

```bash
for i in 1 2 3; do
  docker run -d --name discas-$i --network discas-quickstart \
    -p 960$i:9600 \
    -v "$HOME/discas-quickstart/cluster/$i:/var/lib/discas/$i" \
    -v "$HOME/discas-quickstart/cluster/members.conf:/var/lib/discas/members.conf:ro" \
    "$DISCAS_IMAGE" node \
      --node-id $i --cluster-id quickstart \
      --members-file /var/lib/discas/members.conf \
      --wal-dir /var/lib/discas/$i
done
```

`--members-file` is re-read at runtime, where the inline `--members` list of **A** is fixed at
start. The mount is what makes the data outlive the container.

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
docker run -d --name discas-agent --network discas-quickstart \
  -p 8500:8500 -p 9604:9601 \
  "$DISCAS_IMAGE" agent \
    --nodes 1=discas-1:7002,2=discas-2:7002,3=discas-3:7002 \
    --client-id quickstart-agent

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
docker stop discas-3
curl -s -X PUT --data-binary 'yes' http://127.0.0.1:8500/v1/kv/app/still-works
```

One of two things comes back. If the agent was not talking to node 3, the write never noticed:

```json
{"ok":true}
```

If it was, the write was in flight to a coordinator that stopped answering, and you get this
instead:

```json
{"error":"Unfenced write dispatched to a coordinator that did not answer; not re-sent"}
```

**That is the correct answer, not a bug, and it is worth the detour.** A plain `PUT` carries no
version, so nothing about it is fenced: replayed, it would apply a second time. The agent cannot
tell "node 3 died before it applied this" from "node 3 applied it and died before replying", so it
refuses to guess and hands the ambiguity to you rather than resolving it silently. Send it again and
it goes to a live coordinator:

```bash
curl -s -X PUT --data-binary 'yes' http://127.0.0.1:8500/v1/kv/app/still-works
```
```json
{"ok":true}
```

A write that *is* fenced -- one with `?cas=` -- never needs that judgement call: replaying it is
safe, because the version it names has already moved. This is the whole argument for versioned
writes, and it is worked through in
[Keys and values: the one that needs care](user/02-key-value.md#the-one-that-needs-care).

So: two of three is a majority, and writes continue. Nothing had to be reconfigured and nothing
declared node 3 dead -- a peer that is down is simply not part of a quorum.

Kill a second and the majority is gone:

```bash
docker stop discas-2
curl -s -m 20 -X PUT --data-binary 'no' http://127.0.0.1:8500/v1/kv/app/broken
curl -s 'http://127.0.0.1:8500/v1/kv/app/still-works?raw&stale'
```
```
{"error":"CAS failed: Timeout"}
yes
```

Linearizable writes stop -- the exact error depends on how far the attempt got, so the unfenced
message above is equally likely here -- and reads that tolerate staleness keep being answered from
the surviving node's local committed state.

Start the two nodes again and the cluster resumes on its own; there is no readmission step
([4. Quorum](operator/04-quorum.md#losing-quorum)). Give the mesh a few seconds to re-handshake
before expecting a write to land:

```bash
docker start discas-2 discas-3
sleep 15
curl -s 'http://127.0.0.1:8500/v1/kv/app/still-works?raw'    # -> yes
```

## 7. Tear down

```bash
docker rm -f discas-1 discas-2 discas-3 discas-agent
docker network rm discas-quickstart
rm -rf ~/discas-quickstart          # only if you ran 3B
```

---

## Where to go next

| You are | Read |
|---|---|
| Writing an application against it | [User Guide](user/README.md) -- the Java client, locks, scan and watch |
| Calling it over HTTP from any language | [Agent manual](agent/README.md) -- the full surface, curl-first |
| Going to run it somewhere real | [Operator Guide](operator/README.md) -- start with [5. Access](operator/05-access.md), because everything on this page was unauthenticated |
| Working on discas itself | [Developer Guide](dev/README.md) -- building from source, and [the same cluster without containers](dev/02-development.md#a-local-cluster) |

Two things this page skipped that matter as soon as it is not a laptop: **authentication and TLS**
are off, and this cluster's `N` is 3 and **frozen** -- resizing is a planned operation, not a live
one ([3. Cluster and members](operator/03-cluster.md#changing-n)).
