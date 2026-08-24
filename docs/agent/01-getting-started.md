# 1. Getting started

## An agent, pointed at a cluster

The agent needs one thing: where the nodes are. Everything else has a default.

```bash
java -cp "$CP" io.github.green4j.discas.agent.starter.DisCasAgentStarter \
    --nodes 1=10.0.0.1:8001,2=10.0.0.2:8001,3=10.0.0.3:8001
```

`$CP` is the built jars -- `./gradlew build`, then
`CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)`. It listens on
`127.0.0.1:8500` and prints its effective configuration, every value tagged with where it came
from, before it starts. [5](05-running.md) is the rest of the flags.

**Give it every node.** There is no leader; the agent's client picks a coordinator per request and
moves to another when one cannot serve it. A shorter list works but limits failover, and a listing
needs a majority of the *real* cluster size to answer.

Check it is up:

```bash
curl -s -X GET localhost:8500/v1/agent/health
```
```json
{"status":"ok","clientId":"agent","nodes":["1","2","3"]}
```

**That is agent liveness, not cluster health.** It answers `200` while the process is running, even
when the cluster has lost quorum and can serve nothing. [5](05-running.md#health) says what to use
instead.

## The first write and read

```bash
curl -s -X PUT --data-binary 'node-7' localhost:8500/v1/kv/service/leader
```
```json
{"ok":true}
```

The key is the path after `/v1/kv`, so this wrote the key `service/leader`. The value is the request
body, raw bytes, whatever they are.

Reading gives you a choice of two shapes:

```bash
curl -s -X GET localhost:8500/v1/kv/service/leader
```
```json
{"Key":"service/leader","Value":"bm9kZS03","Flags":0}
```
```bash
curl -s -X GET 'localhost:8500/v1/kv/service/leader?raw'
```
```
node-7
```

The default is a JSON envelope with the value **base64-encoded**, because a value is bytes and JSON
is text. `?raw` hands back the bytes themselves under `application/octet-stream` -- what you want for
piping into anything, and what the rest of these examples use.

> Quote URLs that carry a `?`. In `zsh` an unquoted `?` is a glob and the request never leaves your
> shell.

## Where to go next

You now have the two calls that cover most reading and writing. The one thing worth learning before
you build on it is **`?cas`** -- the conditional write, and the only form that stays safe when a
coordinator stops answering mid-request. That is [2](02-kv.md).
