# 3. Blocking queries

A blocking query is how you follow a key without polling it yourself. It is the same endpoint as an
ordinary read, turned on by either `?version=` or `?wait=`.

## The version

Every read answers with an `X-DisCas-Version` header -- the key's opaque, monotonic version, and the
same `Version` the Java client returns. Hold it, pass it back, and the next call returns once the
key has moved past it.

```bash
curl -s -D - -X GET 'localhost:8500/v1/kv/watched'
```
```
HTTP/1.1 200 OK
X-DisCas-Version: 31333a32

{"Key":"watched","Value":"djA=","Flags":0}
```

`?wait=0` asks for the blocking form with a zero budget: the current state, right now. A plain
`GET` carries a version too, so this is a convenience rather than the only way to obtain one.

## Waiting for a change

```bash
curl -s -D - -X GET 'localhost:8500/v1/kv/watched?version=31333a32&wait=30'
```

This returns as soon as the key changes, with the new value and a new version:

```
HTTP/1.1 200 OK
X-DisCas-Version: 32333a32
X-DisCas-Changed: true

{"Key":"watched","Value":"djE=","Flags":0}
```

If nothing changes within the wait, it returns the current state and **the same version you sent**,
with `X-DisCas-Changed: false`. The header is the answer; comparing the versions yourself is the
same answer arrived at by hand, and you should not have to.

The loop is therefore:

```bash
ver=$(curl -s -D - -X GET -o /dev/null 'localhost:8500/v1/kv/config' \
      | tr -d '\r' | awk -F': ' '/X-DisCas-Version/{print $2}')

while true; do
  new=$(curl -s -D - -X GET -o /dev/null "localhost:8500/v1/kv/config?version=$ver&wait=30" \
        | tr -d '\r' | awk -F': ' '/X-DisCas-Version/{print $2}')
  if [ "$new" != "$ver" ]; then      # or read X-DisCas-Changed and skip the comparison
    ver=$new
    curl -s -X GET "localhost:8500/v1/kv/config?raw"    # act on the new value
  fi
done
```

## What the wait actually is

**`?wait=` is capped one second under the agent's `--request-timeout-seconds`** (10 s by default,
so the cap is 9 s). Ask for `wait=30` and you get 9. The cap exists so the query completes normally
instead of tripping the agent's own timeout and becoming a [`504`](06-errors.md) -- an unchanged
reply is a normal answer, a timeout is not. Omitting `?wait` entirely means the full capped budget.

Raise `--request-timeout-seconds` if you want longer polls; the cap follows it.

**It is a poll, not a subscription.** There is no server push. The agent's client re-reads the key
on a backoff until the version moves, and at the default consistency each of those reads is a full
consensus round. Watching a hundred keys means a hundred loops, each costing about a write per
second. Add `&stale` if slightly-behind notice is acceptable.

**It coalesces.** If the key changed several times during the wait you get the *latest* state, not
each step. A register keeps only its current value, so there is no history to replay. If you need
every transition, this is the wrong tool for that part of your design.

**A write of an identical value does not wake it.** Rewriting a key with the bytes it already holds
does not advance the version -- see [2](02-kv.md#swapped-means-it-holds-what-you-asked-for) -- so a
watcher sees nothing, correctly. A heartbeat that writes a constant will therefore never fire a
watch; put something that changes in the value if that is what you meant.

**Spurious wakes happen and are benign.** A linearizable read that repairs a lagging quorum
re-accepts the current value at a new ballot, so the version can advance although nothing was
written. Treat a wake as "look again", not "something definitely changed".

## Watching for a key that does not exist yet

A miss still carries a version, so you can long-poll for a *creation*:

```bash
curl -s -D - -X GET 'localhost:8500/v1/kv/absent?wait=0'
```
```
HTTP/1.1 404 Not Found
X-DisCas-Version: 303a

{"error":"not found"}
```

Feed `303a` back as `?version=` and the call returns when the key appears. The same applies to a key
that was deleted: it is tombstoned, not gone, so it keeps a version you can wait on.

## Bad parameters are refused, not defaulted

```bash
curl -s -X GET 'localhost:8500/v1/kv/watched?wait=abc'      # {"error":"invalid wait: not a number"}   400
curl -s -X GET 'localhost:8500/v1/kv/watched?version=zz'    # {"error":"invalid version: not hex"}     400
```

A parse error is a client error. Silently falling back to the default would turn a typo into a
watch that never fires, which is the failure you would never find.

## One key at a time

There is no prefix watch. Watching a set means one loop per key -- fine for a handful of
configuration keys, and given the cost above, wrong for thousands. If two keys must change together,
make them one key.
