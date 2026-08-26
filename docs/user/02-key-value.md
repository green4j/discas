# 2. Keys and values

Examples assume the `utf8`/`str` helpers from [1](01-getting-started.md#reading-and-writing).

## Reading

```java
GetResult current = client.get("config/timeout").join();
current.exists();     // false when absent or deleted
current.value();      // null when it does not exist
current.version();    // usable as a fence, and as a watch position
```

There is one read. The version comes back with every one of them because the wire always carried
it, and it is what any subsequent write needs; a caller that only wants the bytes reads
`current.value()`.

Reads are **linearizable** by default: the node runs a full consensus round, so the answer reflects
a state a quorum agreed on at read time. That costs the same as a write. When bounded staleness is
acceptable, opt out:

```java
import io.github.green4j.discas.common.client.ReadConsistency;

client.get("config/timeout", ReadConsistency.SERIALIZABLE).join();
```

A serializable read is answered from one node's local committed state with no round and no WAL
write. It may be stale, and under concurrent writers it may show a value accepted locally that the
cluster has not chosen. Use it for read-heavy paths that tolerate that; do **not** use it to decide
anything -- in particular not before taking a lock.

## Writing

There are three shapes, and the difference is not convenience.

```java
// Unfenced: overwrite whatever is there. Returns the version it committed at.
Version at = client.put("config/timeout", utf8("30s")).join();

// Fenced: apply only if the key is still where I last saw it.
CasResult r = client.cas("config/timeout", current.version(), "45s").join();

// Read-modify-write, with the retry loop inside: apply only to what is current.
GetResult updated = client.update("config/timeout", now -> bump(now)).join();
```

**Which one.** `update` whenever the current state decides anything -- what to write, or whether to
write at all. `cas` when the version is something you already hold or already know, so there is
nothing to read first. `put` when the decision does not depend on the state at all.

`put` and `delete` return the `Version` they committed at, so a writer can fence its next write, or
start a watch, on what it just wrote without reading it back.

`cas` is the compare-and-set, and it compares the key's **version**, never its bytes. There is one
CAS; there is no value-compared variant, because its failure mode cannot be handled by any caller
(see [why](../dev/03-caspaxos.md#version-fenced-cas-and-no-value-compared-cas)).

```java
r.swapped();   // did this call apply the change?
r.version();   // the version that won -- feed it back into a retry
r.value();     // and the value at it
```

`Version.INITIAL` means *the key has no committed value*. It is a real version, not a wildcard, so
a CAS against it is create-if-absent -- which has its own name:

```java
CasResult created = client.putIfAbsent("service/owner", "me").join();
if (!created.swapped()) {
    // somebody else got there first; created.value() is theirs
}
```

Losing here is **final**, not a reason to read again: the key exists, and no amount of retrying
will make it not exist. That is why this is a `cas` and not an `update`.

## Read-modify-write

This is the pattern the store exists for. Never `get` then `put` -- that loses the concurrent write
between the two. `update` is the loop:

```java
GetResult r = client.update("counter", current ->
        Long.toString(current == null ? 1L : Long.parseLong(current) + 1L)).join();
```

The transform sees the current value, or `null` when the key is absent or tombstoned, and returns
what to commit -- returning `null` tombstones the key. To abandon the update, throw from the
transform: nothing is written and the exception completes the future. There is no sentinel for
"leave it alone".

A lost compare is a **result, not a failure**: it says another writer committed, so `update` reads
again and re-applies the transform to what it finds. It never re-sends a version it already knows
is stale.

**Only a lost compare is retried.** A timeout, or any other failure, completes the future as it is:

```java
try {
    client.update("counter", inc).join();
} catch (CompletionException e) {
    // UpdateContendedException: the key is hot -- every attempt was answered and every one lost.
    // Anything else: unknown outcome. Only you can decide whether re-running is safe.
}
```

That asymmetry is the whole design. A `cas` whose outcome is unknown is safe to re-send, because
the duplicate carries an overtaken ballot and provably cannot apply. An `update` is two round
trips, so re-running `v -> v + 1` after a lost *response* would increment twice. When the outcome
is unknown, only the caller knows whether that is acceptable, so it is told.

The retry budget bounds the loop, not the individual round trips; running out raises
`UpdateContendedException`, which is deliberately not a timeout -- "the key is hot" and "the cluster
did not answer" call for opposite reactions.

```java
client.update("counter", inc, Duration.ofSeconds(5)).join();
```

If you would rather drive the loop yourself, `get` + `cas` is exactly what `update` does inside.

## Deleting

```java
Version at = client.delete("config/timeout").join();          // unfenced, returns the version
client.delete("config/timeout", current.version()).join();    // fenced, returns CasResult
```

The fenced form is literally `cas(key, version, null)` -- a delete is a CAS to a tombstone. The key
is tombstoned rather than removed, so it stays visible to `get` with a `null` value and an
advanced version; that is what stops a replica that missed the delete from resurrecting the old
value. Tombstones are collected later, automatically.

## What to do with a failure

A failed future carries a `DisCasOperationException` when a node answered, or a
`RequestFailedException`/`ClientLifecycleException` when no verdict was ever obtained. The first
carries the code that matters:

```java
import io.github.green4j.discas.client.DisCasOperationException;
import io.github.green4j.discas.common.client.ClientErrorCode;

client.put(key, value).handle((ok, err) -> {
    if (err == null) {
        return "applied";
    }
    Throwable cause = err instanceof CompletionException ? err.getCause() : err;
    if (cause instanceof DisCasOperationException) {
        ClientErrorCode code = ((DisCasOperationException) cause).code();
        // branch on code, never on the message text
    }
    return "failed";
});
```

**The one question is: did it happen?**

| Code | Did it happen? | What to do |
|---|---|---|
| `UNAVAILABLE` | **unknown** -- may still be applied | the only dangerous one; see below |
| `BALLOT_LOST` | no -- lost a ballot duel before anything was accepted | retry freely |
| `PROPOSAL_EXPIRED` | no -- abandoned before anything was proposed | retry freely |
| `NO_QUORUM_AT_COORDINATOR` | no | already retried elsewhere by the client |
| `NOT_READY` | no | already retried elsewhere by the client |
| `STORE_FULL` | no | every replica holds the same keys, so elsewhere is full too; retry after a delete, which is never refused for want of room |
| `ACCESS_DENIED`, `INVALID_ARGUMENT` | no | your request is wrong; retrying cannot fix it |
| `INTERNAL` | no | the node failed the operation |

Everything except `UNAVAILABLE` is **determinate**: the write did not happen, and re-issuing it is
free.

Before you see any of these, the client has already walked the coordinators wherever it safely can.
Always on `NOT_READY` and `NO_QUORUM_AT_COORDINATOR` -- both are conditions of one node, refused
cheaply without running a round. And for a **fenced** write, on `UNAVAILABLE`, `BALLOT_LOST`,
`PROPOSAL_EXPIRED` and `INTERNAL` too, because a duplicate of a fenced write provably cannot apply
twice. So an error from a `cas` means every reachable coordinator was tried; an error from a `put`
usually means one was.

The same rule holds when no answer arrives at all. A coordinator that goes silent past the
per-attempt timeout, or whose connection dies, is a failure with no phase to read, so only the
fence can justify trying elsewhere. A fenced write moves on; an unfenced one that had already
reached the wire stops there and fails with `RequestFailedException` carrying
`Cause.INDETERMINATE`. A request the client never managed to send is safe whatever it is, and
still moves on.

## The one that needs care

`UNAVAILABLE` means the round reached the accept phase and did not complete in time. Some acceptor
may hold the proposal and a later proposer may finish it -- **without you**. You cannot resolve it by
re-reading, because "not applied yet" and "will be applied in a moment" look identical.

**With a fenced write there is nothing to do.** A duplicate carries a version the register has
already overtaken, so it provably cannot apply twice. The client knows this and keeps walking
coordinators on `UNAVAILABLE` for a fenced write, so you get a definite answer or a deadline:

```java
// Safe to retry as written: a stale duplicate is rejected by the fence.
client.cas(key, expected, desired)
```

**With an unfenced `put`/`delete` there is no safe automatic recovery**, which is why the client does
not retry them and tells you instead -- as `UNAVAILABLE` when a node answered, and as
`Cause.INDETERMINATE` when none did. The cost is real: an unfenced write gives up after the first
coordinator that goes quiet, where a fenced one would have kept walking. If you need a recovery,
make the write identify its author and read it back:

```java
String marker = "writer-" + myId + ":" + payload;
client.put(key, utf8(marker)).handle((ok, err) -> {
    if (err == null) {
        return true;
    }
    // Unknown outcome: ask whether the value that landed is mine.
    return marker.equals(str(client.get(key).join().value()));
});
```

That is what `CoordinatorFailoverExample` demonstrates against a cluster with a node actually
killed underneath it. If you find yourself writing this often, the answer is usually to switch to a
fenced write.

## Limits

Keys and values are bounded (`KvLimits.MAX_KEY_BYTES`, `MAX_VALUE_BYTES`); an oversized request is
refused with `INVALID_ARGUMENT` before any round is started. discas holds coordination state -- a few
small registers per concern -- not documents or blobs.
