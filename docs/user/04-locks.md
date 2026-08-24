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
import io.github.green4j.discas.client.lock.LockAcquireStatus;
import java.time.Duration;

// One attempt.
LockAcquireResult r = client.tryLock("jobs/nightly", Duration.ofSeconds(30)).join();

// Or wait for it.
LockAcquireResult w = client.lock("jobs/nightly",
        Duration.ofSeconds(30),      // lease
        Duration.ofSeconds(5)).join(); // how long to keep trying

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
| `ACQUIRED` | you hold it; `lock()` is non-null |
| `HELD_BY_OTHER` | somebody else does; `lockInfo()` says who and until when |
| `NOT_LOCK_RECORD` | that key holds something that is not a lock -- you are colliding with other data |
| `TIMED_OUT` | only from `lock(...)`: your wait budget ran out while somebody else kept it |

Pass an owner id when you want the holder to be identifiable by a human:
`tryLock(key, ttl, "worker-3")`. Without one a random id is generated.

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
| `EXPIRED` | your own lease ran out before you renewed | acquire again -- you no longer hold it |
| `HELD_BY_OTHER` | somebody else holds the key; `observed()` says who | stop; the work is not yours |
| `NOT_HELD` | the key holds no lock at all -- absent, or already released | stop |
| `NOT_LOCK_RECORD` | the key holds something that is not a lock | you are colliding with other data |
| `CONTENDED` | the key was yours when read and had moved on by the write | read again and decide |

A lapsed lease is **not renewable**: from the moment it ran out any waiter was entitled to take
over, so extending it would hand back a lock you cannot claim to have kept. A release still applies
on a lapsed lease -- nobody took over while your token still matches, and writing the release marker
is the right cleanup either way.

`remainingLease()` is deliberately monotonic, so an NTP step cannot lengthen or shorten your lease
under you. The *stored* deadline is wall-clock, because it crosses processes -- and the client
corrects it against the cluster's clocks, so two clients judge the same lease the same way even when
their own clocks disagree.

Renewing from a background thread while the work runs on another is the expected shape; the lock's
state is safe for that.

```java
lock.validate().join();   // still mine, according to the cluster?
lock.info().join();       // a fresh remote read of the record
lock.lockInfo();          // local view, no round trip
```

## When the outcome is unknown

`tryLock` is a write, so it inherits [the same failure question](02-key-value.md#what-to-do-with-a-failure):
if it fails with `UNAVAILABLE`, you may or may not hold the lock. Do not simply retry -- you could
end up waiting out your own lease.

Ask instead:

```java
import io.github.green4j.discas.client.lock.LockInfoStatus;

// After an acquire whose outcome you do not know:
client.getLockInfo("jobs/nightly").thenAccept(info -> {
    if (info.status() == LockInfoStatus.LOCKED
            && myOwnerId.equals(info.info().ownerId())) {
        // it is mine after all -- carry on, or release it
    }
});
```

This is why passing an explicit owner id is worth it: without one you cannot recognise your own
lock. `CoordinatorFailoverExample` runs exactly this recovery against a killed coordinator.

## Rules of thumb

- **Short leases, renewed** beat long leases. A crashed holder blocks others for the remaining lease
  and nothing can shorten that.
- **Never take a lock on a serializable read.** A stale read is a lock two holders can believe they
  own. The lock path uses linearizable reads for this reason.
- **One lock per key, and keep locks out of your data prefixes.** `NOT_LOCK_RECORD` exists because
  someone eventually points a lock at a key holding something else.
- **Locks are values**, so a `dump` of a prefix containing locks dumps the locks. That is deliberate;
  only you know what lives where.
