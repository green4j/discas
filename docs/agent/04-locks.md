# 4. Locks

Everything under `/v1/lock/{key}`. A lock is a value in one key, written with the same version-fenced
CAS as anything else -- not a protocol feature, so it inherits that write's failure story. It is a
**lease**: held for a bounded TTL, and a holder that stalls past it can be displaced.

## Taking one

```bash
curl -s -X PUT 'localhost:8500/v1/lock/jobs/nightly?ttl=30&owner=worker-3'
```
```json
{"status":"ACQUIRED","acquired":true,"token":"8ce3af6cdfc160556fe0aef424154fd2","generation":1}
```

`ttl` is required, in seconds, and must be at least 1. `owner` is **required** too, and free text.

There is no generated fallback, because an id you never saw cannot do the one job the field has.
An acquire is a write, and over HTTP a dropped connection or a gateway timeout takes away exactly
the response that would have told you the outcome. The token comes back only in that response, and
the generation is not knowable in advance -- `owner` is the one thing you choose before the request,
so it is the only way back to a lock that was in fact taken. See
[below](#when-you-do-not-know-whether-you-got-it).

That gives it one hard requirement: it must name **exactly one holder**, unique among everything
that might hold this key at the same time. Two holders sharing a name would each read the other's
lease as their own. It is not a credential, though -- only the token decides who may renew or
release.

Add `&wait=<seconds>` to block instead of trying once:

```bash
curl -s -X PUT 'localhost:8500/v1/lock/jobs/nightly?ttl=30&wait=5&owner=worker-4'
```

Losing the race is an ordinary outcome, not an error: every one of these is `200`, and only a
failure to reach the cluster fails the request.

| `status` | Means |
|---|---|
| `ACQUIRED` | you hold it; `token` and `generation` are yours |
| `HELD_BY_OTHER` | somebody else does; `info` says who and until when |
| `HELD_BY_SELF` | a live lease already stands under *your* `owner` -- see [below](#when-you-do-not-know-whether-you-got-it) |
| `TIMED_OUT` | only with `&wait=`: your budget ran out while it stayed held |
| `NOT_LOCK_RECORD` | that key holds something that is not a lock -- you are colliding with other data |

```json
{"status":"HELD_BY_OTHER","acquired":false,"info":{"owner":"worker-3","generation":1,"acquiredAtEpochMs":1787504798215,"leaseUntilEpochMs":1787504828215,"expired":false}}
```

## The token, and the part that actually protects you

The `token` is proof of holding, and every renew and release must carry it:

```bash
-H 'X-DisCas-Lock-Token: 8ce3af6cdfc160556fe0aef424154fd2'
```

or `?token=<hex>` if a header is inconvenient. Missing or malformed is a `400`.

It comes back once, in the answer to the acquire that minted it, and never again: no `info` block
carries the holder's token, neither the one above nor the `GET` further down. The token is not a
secret -- it sits in the clear inside the record, and keeping the wrong callers away from the key is
the [ACL's](../operator/05-access.md) job -- but an answer anyone may ask for is no place to hand out
the means to release a lease you do not hold. If the token you lost was your own, `?recover` gives
it back.

`generation` is something else, and it is the one that matters:

**The lease is not the safety mechanism. The fencing token is.**

A holder paused past its lease -- a long GC, a frozen VM, a partition -- has a perfectly correct clock
and a token that still looks valid, while a successor has already taken over. No lock anywhere can
prevent that. What it can do is let the *protected resource* reject the straggler. `generation`
strictly increases with every acquire of the key:

```json
{"status":"ACQUIRED","acquired":true,"token":"f02adcc3db7cb8bf8b8c5ee440b44ce2","generation":2}
```

Pass that number to whatever the lock protects, and have that resource remember the highest it has
seen and refuse anything lower. If the resource cannot take a token, the lock gives you an
optimisation and not a guarantee. Be honest with yourself about which one you needed.

## Holding, renewing, releasing

```bash
curl -s -X PUT -H "X-DisCas-Lock-Token: $TOK" 'localhost:8500/v1/lock/jobs/nightly?renew&ttl=60'
```
```json
{"renewed":true,"status":"APPLIED"}
```

```bash
curl -s -X DELETE -H "X-DisCas-Lock-Token: $TOK" localhost:8500/v1/lock/jobs/nightly
```
```json
{"released":true,"status":"APPLIED"}
```

`"renewed":false` means somebody took it. **Stop the work** -- do not simply renew again. `status`
says which of the refusals it was: `HELD_BY_OTHER` (someone displaced you), `EXPIRED` (your own
lease lapsed before you renewed -- a lapsed lease is not renewable, acquire again), `NOT_HELD` (no
lock of yours on the key), `ALREADY_RELEASED` (you released it yourself), `NOT_LOCK_RECORD` (that
key holds other data), or `CONTENDED` (the key moved between the read and the write -- read again
and decide).

A `DELETE` is safe to repeat, which matters over HTTP for the same reason an acquire does: the
answer is what a dropped connection takes away. The marker keeps the token that wrote it, so the
retry is told apart from a request against a key nobody holds:

```json
{"released":false,"status":"ALREADY_RELEASED"}
```

`"released":false` because this call wrote nothing -- but the release you were retrying did land,
and you are done. `NOT_HELD` is the other answer, and it is the narrower one: no release of yours is
recorded here. Note the record only remembers the latest holder, so if another has acquired and
released the key since, that is what you get, and whether your own release landed first is no longer
knowable from the key.

Short leases, renewed, beat long leases: a crashed holder blocks everyone for the remaining lease
and nothing can shorten that.

## Inspecting

```bash
curl -s -X GET localhost:8500/v1/lock/jobs/nightly
```
```json
{"status":"LOCKED","info":{"owner":"worker-3","generation":1,"acquiredAtEpochMs":1787504798215,"leaseUntilEpochMs":1787504828215,"expired":false}}
```

| `status` | Means |
|---|---|
| `UNLOCKED` | no lock here. `info` names the holder that released it, or is `null` if the key is simply absent |
| `LOCKED` | a lock whose lease has not run out |
| `EXPIRED` | the record is still there -- owner and generation readable -- but the lease has passed, so it can be taken over |
| `NOT_LOCK_RECORD` | the key holds something that is not a lock at all |

`EXPIRED` is kept distinct from `UNLOCKED` on purpose: the record survives so a takeover is
auditable. A release is kept auditable the same way -- the marker it leaves behind carries the owner
and the generation, so an `UNLOCKED` key can still say whose it last was and when it was let go:

```json
{"status":"UNLOCKED","info":{"owner":"worker-3","generation":1,"acquiredAtEpochMs":1787504798215,"leaseUntilEpochMs":0,"expired":true}}
```

A zero `leaseUntilEpochMs` is what marks it as a release rather than a lock; the key is free.

The timestamps are wall-clock epoch milliseconds, corrected against the cluster's clocks, so two
callers judge the same lease the same way even when their own clocks disagree.

## When you do not know whether you got it

An acquire is a write, so it inherits [the same question](06-errors.md): a `503`, a `504` or a
dropped connection may all mean you hold the lock and never heard. A lock nobody knows they hold is
held until its lease runs out. Do not simply retry -- at best you wait out your own lease.

Ask with `?recover`, which answers in exactly the same shape as an acquire:

```bash
curl -s -X PUT 'localhost:8500/v1/lock/jobs/nightly?recover&owner=worker-3'
```
```json
{"status":"ACQUIRED","acquired":true,"token":"8ce3af6cdfc160556fe0aef424154fd2","generation":1}
```

`ACQUIRED` hands back the lock the lost acquire took -- the same token and generation, so it renews
and releases like the original. `NOT_HELD` means the acquire never landed and is safe to issue
again. `HELD_BY_OTHER` means somebody else got there.

The same thing shows up on the normal path: an acquire that finds a live lease already under your
`owner` answers `HELD_BY_SELF` rather than `HELD_BY_OTHER`, and `&wait=` returns at once instead of
spending its budget on a lease it cannot win. No `token` comes with it -- if two callers really did
share a name, handing the second one a working token is how you would lose mutual exclusion without
noticing. Use `?recover` to get it, and in doing so say the name is yours.

## `?stale` is ignored here

The lock endpoints read at full consistency and quietly ignore `?stale`, unlike the KV endpoints.
That is deliberate: a lock decision taken on a possibly-stale read is a lock two holders can believe
they own. The consequence is that **locks stop working when the cluster loses quorum**, which is the
correct behaviour.

## Rules of thumb

- **One lock per key, and keep locks out of your data prefixes.** `NOT_LOCK_RECORD` exists because
  someone eventually points a lock at a key holding something else.
- **Locks are values**, so they show up in a `?keys` listing like anything else. Only you know what
  lives where.
- **Renew from a separate loop** to the one doing the work, and check the answer every time.
