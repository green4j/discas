# 4. Locks

## What a lock here is

A value in one key, written with the same version-fenced CAS as anything else. Not a protocol
feature -- which means it inherits that write's failure story rather than having one of its own, and
you can read a lock's state with an ordinary `get` if you want to.

It is a **lease**: held for a bounded TTL, and a holder that stalls past it can be displaced.

## Taking one

```java
import io.github.green4j.discas.client.lock.Lock;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import java.time.Duration;

// One attempt.
LockAcquireResult r = client.tryLock("jobs/nightly", Duration.ofSeconds(30), "worker-3").join();

// Or wait for it.
LockAcquireResult w = client.lock("jobs/nightly",
        Duration.ofSeconds(30),        // lease
        Duration.ofSeconds(5),         // how long to keep trying
        "worker-3").join();

if (r.acquired()) {
    Lock lock = r.lock();
    try {
        doWork(lock);
    } finally {
        lock.release().join();
    }
}
```

Losing the race is a normal outcome, not an error -- only being unable to reach the cluster fails the
future:

| `LockAcquireStatus` | Means |
|---|---|
| `ACQUIRED` | you hold it; `lock()` is non-null and `observed()` is not |
| `HELD_BY_OTHER` | somebody else does; `observed()` says who and until when |
| `HELD_BY_SELF` | a live lease already stands under *your* owner id -- see [below](#when-the-outcome-is-unknown) |
| `NOT_LOCK_RECORD` | that key holds something that is not a lock -- you are colliding with other data |
| `TIMED_OUT` | only from `lock(...)`: your wait budget ran out while somebody else kept it |

A result carries the lock or the record that stood in its way, never both: on success the `Lock` is
the whole answer, and `observed()` is what a refusal has instead of one. A `lock(...)` with a zero
wait is `tryLock` and is routed to it, so a caller counting down a budget to nothing still gets the
precise refusal rather than a `TIMED_OUT` that says less.

### The owner id is required, and here is why

There is no overload that invents one for you. An acquire is a write, so its outcome can be
unknown -- and nothing else in the record can settle it afterwards. The token is generated inside the
call and never reaches you if the call failed; the generation is not knowable in advance either. The
owner id is the one field **you choose before the write**, which makes it the only thing a later read
can be compared against. An id you never saw cannot answer the question it exists for.

That gives it one hard requirement: **it must name exactly one holder**, unique among everything that
could hold this key at the same time -- across processes, and across concurrent attempts inside one
process. Two holders sharing an id would each see the other's lease as their own. Something like
`"worker-3"` is fine when there is genuinely one worker 3; a per-instance value is safer.

It is still not a credential. Only the `LockToken` decides who may release or renew -- knowing an
owner id gets you nothing.

## The part that actually protects you

**The lease is not the safety mechanism. The fencing token is.**

A holder paused past its lease -- a long GC, a frozen VM, a partition -- has a perfectly correct clock
and a `Lock` object that still looks valid, while a successor has already taken over. No lock
implementation anywhere can prevent that. What it can do is make the *protected resource* able to
reject the straggler:

```java
long token = lock.fencingToken();   // strictly increases with every acquire of this key

// Pass it to whatever the lock protects, and have that reject anything older:
storage.write(payload, token);      // storage remembers the highest token it has seen
```

If the resource you are guarding cannot take a token, the lock gives you an optimisation and not a
guarantee. Be honest with yourself about which one you needed.

Checking the lease first does not substitute for this. `remainingLease()` will read zero for a
straggler, but a check before a write is only a guess about the moment the write lands, and the
stall that displaced you is just as free to land in between. The token works because it travels
*with* the write. `FencingExample` runs this end to end -- the same stall against a resource that
compares tokens and one that does not -- and asserts that the guarded one keeps the successor's work
while the unguarded one loses it.

## Holding one for a while

```java
Duration left = lock.remainingLease();   // measured on the monotonic clock, anchored at acquire

// Extend before it runs out, typically from a keep-alive task:
LockWriteResult renewed = lock.renew(Duration.ofSeconds(30)).join();
if (!renewed.applied()) {
    // somebody took it; stop the work, do not just renew again
    renewed.status();     // why -- the reasons call for different reactions, see below
}
```

`release` and `renew` answer with a status rather than a boolean, because the reasons call for
opposite reactions:

| `LockWriteStatus` | Means | What to do |
|---|---|---|
| `APPLIED` | the write committed | carry on |
| `ALREADY_RELEASED` | the key holds the marker **your own** release wrote | for a release, you are done; for a renew, your keep-alive crossed your own release |
| `EXPIRED` | your own lease ran out before you renewed | acquire again -- you no longer hold it |
| `HELD_BY_OTHER` | somebody else holds the key; `observed()` says who | stop; the work is not yours |
| `NOT_HELD` | no lock of yours here -- the key is absent, or holds somebody else's marker | stop |
| `NOT_LOCK_RECORD` | the key holds something that is not a lock | you are colliding with other data |
| `CONTENDED` | the key was yours when read and had moved on by the write | read again and decide |

A lapsed lease is **not renewable**: from the moment it ran out any waiter was entitled to take
over, so extending it would hand back a lock you cannot claim to have kept. A release still applies
on a lapsed lease -- nobody took over while your token still matches, and writing the release marker
is the right cleanup either way.

**A release is safe to retry**, and it tells you which retry you are on. The marker keeps the token
that wrote it, so a second attempt that finds its own marker answers `ALREADY_RELEASED`: the first
attempt did land, and there is nothing left to do. That is why it is a separate status from
`NOT_HELD`, which now means the narrower thing -- no release of *yours* is written here.

One limit worth knowing, because the record cannot do better: it remembers only the latest holder.
If somebody else has acquired and released the key since your tenure, their marker is what you find,
and whether your own release got in first is no longer written down anywhere. You get `NOT_HELD`.

`remainingLease()` is deliberately monotonic, so an NTP step cannot lengthen or shorten your lease
under you, and it is the **only** lease reading a `Lock` gives you. The stored deadline is wall-clock
because it crosses processes -- the client corrects it against the cluster's clocks, so two clients
judge the same lease the same way even when their own disagree -- but it is the wrong instrument for
its own holder, so a holder is not handed it.

Renewing from a background thread while the work runs on another is the expected shape; the lock's
state is safe for that. Two things make it a safety property rather than decoration: the renew
period has to sit well inside the lease, and the work has to check. `KeepAliveExample` is that loop
twice over -- once renewing every third of the lease, once on a period longer than it -- and the
second run is the one worth reading: nothing throws, nothing logs an error, the key quietly changes
hands, and only the work checking between units stops it from carrying on without a lock.

```java
lock.info().join();       // a fresh remote read of the record
```

That read is worth having for a log or a decision to give up. It is **not** worth having as a
permission check: the answer is already stale by the time it arrives, so "am I still the holder?"
followed by "then write" is a race with a step in the middle. That is what `fencingToken()` is for,
and it is why there is no method here that answers the question more conveniently -- one would only
make the wrong pattern easier to write.

## When the outcome is unknown

`tryLock` is a write, so it inherits [the same failure question](02-key-value.md#what-to-do-with-a-failure):
if it fails with `UNAVAILABLE`, the acquire may well have committed with the answer lost on the way
back. A lock nobody knows they hold is held until its lease runs out.

Do not simply retry. At best you wait out your own lease; at worst you sit in `lock(...)` until the
budget expires and learn nothing.

Ask instead:

```java
LockAcquireResult r = client.recoverLock("jobs/nightly", "worker-3").join();

switch (r.status()) {
    case ACQUIRED:
        doWork(r.lock());   // it landed -- same token, same generation
        break;
    case NOT_HELD:
        retryTheAcquire();  // it did not land; nothing was written
        break;
    default:
        giveUp();           // somebody else got there; r.observed() says who
        break;
}
```

`recoverLock` rebuilds the `Lock` from the record, so what you get back releases and renews exactly
like the one the acquire would have handed you -- it is the same lease, not a new one.

The same thing surfaces on the normal path. If an acquire finds a live lease already in your name,
you get `HELD_BY_SELF` rather than `HELD_BY_OTHER`, and `lock(...)` stops waiting immediately instead
of spending its budget on a lease it cannot win. `HELD_BY_SELF` deliberately hands back **no** `Lock`:
if two things really did share an owner id, silently giving the second one a working lock is how you
would lose mutual exclusion without noticing. Call `recoverLock` to get the lock, and by calling it
you are saying the id is yours.

`CoordinatorFailoverExample` runs exactly this recovery against a killed coordinator.

## Rules of thumb

- **Short leases, renewed** beat long leases. A crashed holder blocks others for the remaining lease
  and nothing can shorten that.
- **Never take a lock on a serializable read.** A stale read is a lock two holders can believe they
  own. The lock path uses linearizable reads for this reason.
- **One lock per key, and keep locks out of your data prefixes.** `NOT_LOCK_RECORD` exists because
  someone eventually points a lock at a key holding something else.
- **Locks are values**, so a `dump` of a prefix containing locks dumps the locks. That is deliberate;
  only you know what lives where.
