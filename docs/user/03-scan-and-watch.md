# 3. Scan and watch

Two ways to look at more than one moment. Both are shaped by the same fact: discas agrees on **one
register at a time**, so there is no global revision -- no snapshot across keys, and no ordered
change stream. What each gives you instead is worth knowing exactly.

## Scan

```java
import io.github.green4j.discas.client.ScanPage;
import io.github.green4j.discas.client.ScanResult;

ScanPage keys = client.scan("service/").join();
for (ScanResult r : keys.results()) {
    String key = str(r.key());
    // r.version() is the key's version -- usable as a watch position, or as a CAS fence
}
```

Every `scan` returns a `ScanPage`, including this auto-paging form, which walks every page
internally and hands back one already-complete page. The page is how the listing says how far it
can be trusted (`respondedNodes`, `clusterSize`, `quorumReached()`) -- a bare list would have to
throw that away, and completeness is the one thing worth knowing about an enumeration.

`scan` enumerates **keys**, not values. Each node answers from its local state with no consensus
round, and the client merges the answers.

**What it guarantees:** every key whose commit completed before the scan started will appear. That
is quorum intersection -- a committed key had its accept quorum on a majority, and any two majorities
share a node.

**What it does not:**

- **No cross-key snapshot.** Different keys may reflect different instants. This is undefinable in
  this model, not merely unimplemented.
- **No values.** A node reports what it has *accepted*, and an accepted-but-not-chosen ballot can
  still be superseded -- so a key mid-write on a minority may appear and later vanish. Read the value
  with `get` when you need a committed one.
- **Concurrent mutations** may or may not be reflected, with no way to tell which.

Treat the result as *keys that exist or are being created*.

### Coverage

```java
import io.github.green4j.discas.client.ScanCoverage;

client.scan("service/", ScanCoverage.QUORUM).join();         // default
client.scan("service/", ScanCoverage.ANY_AVAILABLE).join();  // best effort
```

`QUORUM` completes once `N/2 + 1` nodes answered and **fails** below that -- it never silently
returns a short list. `ANY_AVAILABLE` returns whatever came back, which is right for an operator
inspecting a cluster that has lost quorum, and wrong for anything that acts on the result. It still
fails if *no* node answers.

### Paging

For a large key space, page it -- and hold the cursor yourself:

```java
ByteBuffer prefix = utf8("orders/");
ByteBuffer cursor = null;
do {
    ScanPage page = client.scan(prefix, cursor, 500).join();
    for (ScanResult r : page.results()) {
        process(r);
    }
    page.quorumReached();   // false only under ANY_AVAILABLE
    cursor = page.nextCursor();
} while (cursor != null);
```

`complete()` is true when there are no more pages -- the same moment `nextCursor()` becomes `null`.
Note that `quorumReached()` and `complete()` are different questions: a page can be the last one and
still not be trustworthy, or be trustworthy with more to come. On the auto-paging form
`complete()` is always true, and `quorumReached()` is true only if *every* page reached a quorum:
a walk is no more trustworthy than the flimsiest page in it.

Paging is resumable: the cursor is just a key, so you can stop and continue later. It is not a
snapshot -- keys written between pages may or may not appear.

## Watch

```java
import io.github.green4j.discas.client.WatchResult;
import java.time.Duration;

Version seen = client.get("config/timeout").join().version();

while (running) {
    WatchResult w = client.watch("config/timeout", seen, Duration.ofSeconds(30)).join();
    if (w.changed()) {
        apply(w.value());       // null if the key was deleted
        seen = w.version();     // advance, then watch again
    }
    // unchanged: the wait elapsed, loop and wait again
}
```

A **blocking query**, not an event stream: you pass the version you last saw, and the call returns
when the key moves past it or the wait elapses. That version is the whole protocol -- keep it and
pass it back. `Version.INITIAL` means "I have seen nothing", so an existing key returns at once and an
absent one blocks until it appears.

**It is a client-side poll, and each poll is a read.** There is no server-side subscription: the
client re-reads the key until the version moves, leaving at least `watchPollPeriod` between one poll
and the next -- one second by default, and spread up to five times that so watches started together
do not stay in step. The gap is counted from the moment a poll *answered*, so a round that took a
second is not followed straight away by another, and it is shortened only to keep the call inside
the `maxWait` the caller asked for.

At the default `LINEARIZABLE` each poll is a full consensus round, so one watch costs the cluster a
round every one to five seconds -- and that is per outstanding watch, not per key. Two consequences:
notice latency is bounded by the period rather than being immediate, and a watch you can afford to
be slightly stale should say so --

```java
client.watch(utf8("config/timeout"), seen, Duration.ofSeconds(30),
        ReadConsistency.SERIALIZABLE);
```

`maxWait` is measured on a monotonic clock, so an NTP step during the wait cannot shorten or extend
it.

**One watch can pace itself.** A key worth hearing about sooner, or a key not worth a round a second
at all, takes the period as an argument rather than making it everybody's setting:

```java
client.watch(utf8("jobs/nightly/state"), seen, Duration.ofMinutes(5),
        ReadConsistency.LINEARIZABLE,
        Duration.ofSeconds(10));      // at least ten seconds between polls
```

`DisCasClient.MIN_WATCH_POLL_PERIOD` is the floor -- 500 ms -- and a shorter period is refused rather
than quietly rounded up, because a caller that believes it polls ten times a second and does not is
worse off than one told it cannot. A whole client that means it sets `watchPollPeriod`; that is the
one deliberate, client-wide decision allowed under the floor.

**A serializable watch rotates over the members.** A serializable poll is answered from one member's
local state, so asking the same member again returns the same answer no matter how many times you
ask -- and a member that missed a commit would hide it for the whole wait. Successive polls
therefore address successive members, and the result carries the highest version any of them
returned. A committed change is held by a majority by definition, so a watch that gets a full lap of
polls in will find it; and the version you are handed back never falls below the one you passed in,
so the loop above cannot walk backwards. The limit that remains is arithmetic: a `maxWait` on the
order of a single poll does not complete a lap, and then which member answered is again the whole
story. Linearizable polling does not rotate and does not need to -- every poll is a round.

**Coalescing.** If the key changed several times during the wait, `changed()` is true and you get
the *latest* state; intermediate values are skipped. A register keeps only its current value, so
there is no history to replay. If you need every transition, discas is the wrong tool for that part
of your design. `WatchExample` in `discas-example` puts a number on it: a burst of twenty commits
against the number of times a watcher wakes.

**Spurious wakes are possible and benign.** A linearizable read that repairs a lagging quorum
re-accepts the current value at a new ballot, so the version can advance although nothing was
written. Treat a wake as "look again", not as "something definitely changed".

**A deleted key can be collected, and then it reads as one that was never written.** A delete
advances the version and wakes the watch like any other commit. Its tombstone is collectable,
though, and a watch polling across the collection finds the key gone rather than tombstoned: a
linearizable watch reports that as `changed()` with a null value at `Version.INITIAL`. It is the one
case in which the version handed back is below the one passed in, and the loop above continues
correctly on it -- watching from `INITIAL` fires again if the key is recreated. A serializable poll
cannot tell a collected key from a member that never saw it, so a serializable watch reports the
collection as unchanged.

**An unchanged answer says whether anything confirmed it.** A poll that fails does not end the
watch: a quorum outage the cluster recovers from inside the budget should not cost the caller the
rest of it, so the watch keeps polling and, at the deadline, answers from the newest state any poll
did manage to see. That answer can therefore be a whole poll window old, and `confirmed()` is how
you tell:

```java
WatchResult w = client.watch("config/timeout", seen, Duration.ofSeconds(30)).join();
if (!w.changed() && !w.confirmed()) {
    // Nothing confirmed this at the deadline; "unchanged" here is not evidence the key is unchanged.
}
```

The version is still the one to feed back in, so a loop that only wants to know when to look again
can ignore it. A caller for which "nothing changed" is itself evidence -- one counting how long it
has been since it could read the key at all -- should treat `confirmed() == false` as no evidence
and read again. If no poll succeeds for the whole budget there is no such state to report, and the
future completes exceptionally instead. A `changed()` answer is always confirmed: it is built from a
poll that just answered.

**A standing watch calls you instead.** Passing a `WatchListener` runs the loop above inside the
client, with no deadline:

```java
KeyWatch watch = client.watch("config/timeout", seen, ReadConsistency.LINEARIZABLE,
        w -> apply(w.value()));
// ...
watch.close();
```

Everything above holds -- the period, rotation, coalescing, spurious wakes and the collected key --
except that there is no unchanged answer: a failed poll is just retried. `changed` runs on the
client's event loop, so it must not block. `failed` is called once, when the watch ends by itself:
the client was closed, the arguments were refused, or `changed` threw. `close()` works from any
thread, `changed` included, and once it returns no further call starts.

**One key at a time.** There is no prefix watch. Watching a set of keys means one polling loop per
key -- which is fine for a handful of configuration keys and, given the cost above, wrong for
thousands.

## Choosing between them

| You want | Use |
|---|---|
| the current members of a set | `scan` with a prefix |
| to react when one known key changes | `watch` |
| to react when *any* key under a prefix changes | neither -- restructure so the fact lives in one key |
| a consistent view of several keys at one instant | not available; see above |

The last row is the one that catches people. If two keys must change together, make them one key.
