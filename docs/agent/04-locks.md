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

`ttl` is required, in seconds, and must be at least 1. `owner` is optional and free text; without
one a random id is generated. **Pass one** -- it is what lets you recognise your own lock later.

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
| `TIMED_OUT` | only with `&wait=`: your budget ran out while it stayed held |
| `NOT_LOCK_RECORD` | that key holds something that is not a lock -- you are colliding with other data |

```json
{"status":"HELD_BY_OTHER","acquired":false,"info":{"owner":"worker-3","token":"8ce3af...","generation":1,"acquiredAtEpochMs":1787504798215,"leaseUntilEpochMs":1787504828215,"expired":false}}
```

## The token, and the part that actually protects you

The `token` is proof of holding, and every renew and release must carry it:

```bash
-H 'X-DisCas-Lock-Token: 8ce3af6cdfc160556fe0aef424154fd2'
```

or `?token=<hex>` if a header is inconvenient. Missing or malformed is a `400`.

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
lease lapsed before you renewed -- a lapsed lease is not renewable, acquire again), `NOT_HELD`
(nothing holds the key), `NOT_LOCK_RECORD` (that key holds other data), or `CONTENDED` (the key
moved between the read and the write -- read again and decide).

Short leases, renewed, beat long leases: a crashed holder blocks everyone for the remaining lease
and nothing can shorten that.

## Inspecting

```bash
curl -s -X GET localhost:8500/v1/lock/jobs/nightly
```
```json
{"status":"LOCKED","info":{"owner":"worker-3","token":"8ce3af...","generation":1,"acquiredAtEpochMs":1787504798215,"leaseUntilEpochMs":1787504828215,"expired":false}}
```

| `status` | Means |
|---|---|
| `UNLOCKED` | no lock here: absent, tombstoned, or a release marker. `info` is `null` |
| `LOCKED` | a lock whose lease has not run out |
| `EXPIRED` | the record is still there -- owner and generation readable -- but the lease has passed, so it can be taken over |
| `NOT_LOCK_RECORD` | the key holds something that is not a lock at all |

`EXPIRED` is kept distinct from `UNLOCKED` on purpose: the record survives so a takeover is
auditable.

The timestamps are wall-clock epoch milliseconds, corrected against the cluster's clocks, so two
callers judge the same lease the same way even when their own clocks disagree.

## When you do not know whether you got it

An acquire is a write, so it inherits [the same question](06-errors.md): a `503` may mean you hold
the lock and never heard. Do not simply retry -- you could end up waiting out your own lease. Ask
instead:

```bash
curl -s -X GET localhost:8500/v1/lock/jobs/nightly | grep -q '"owner":"worker-3"' && echo "mine after all"
```

This is why passing an explicit `owner` is worth it: without one you cannot recognise your own lock.

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
