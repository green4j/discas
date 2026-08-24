# 13. Performance

Four kinds of figure and the conditions they were taken under. The point of this page is not the
numbers -- they will change -- but the **shape** they describe, because the shape follows from the
protocol and will not change: a read is a write that changes nothing, distance costs round trips,
more nodes cost throughput rather than latency, and one hot key does not scale.

```bash
./gradlew :discas-performance:envelope      # reproduce the table below
./gradlew :discas-performance:delayAccuracy # what the injected inter-region delay is worth
```

`discas-performance` sits outside `build`, `check` and `verify` on purpose: a benchmark has no
pass/fail to contribute, and a loaded CI runner would only measure itself.

## Conditions

Java 21.0.3, macOS 15 on Apple silicon (aarch64), 10 cores.

**All nodes and clients run in one JVM**, and that is a condition of every number rather than a
shortcut. It means the figures describe the protocol's shape on a fast link, not the hardware a
deployment would give it: absolute latency is flattered by the absence of a real network, and
throughput is penalised by nodes competing for the cores the clients are using.

The inter-region rows inject a one-way delay on every peer link with a busy-waiting hop accurate to
+/-0.35 ms per crossing.

## The envelope

| Measurement | Conditions | Result |
|---|---|---|
| Ping RTT | direct loopback, 1 byte | p50 0.021 ms, p99 0.058 ms |
| Write | N=3, local | p50 **0.29 ms**, p99 0.61 ms |
| Write, value unchanged | N=3, local | p50 0.16 ms, p99 0.35 ms |
| Read, linearizable | N=3, local | p50 **0.17 ms**, p99 0.36 ms |
| Read, serializable | N=3, local | p50 **0.08 ms**, p99 0.19 ms |
| Write | N=3, 25 ms/link | p50 **105.1 ms**, p99 125.4 ms |
| Write, value unchanged | N=3, 25 ms/link | p50 53.2 ms, p99 74.8 ms |
| Read, linearizable | N=3, 25 ms/link | p50 53.2 ms, p99 60.0 ms |
| Read, serializable | N=3, 25 ms/link | p50 0.12 ms, p99 1.14 ms |
| Throughput, distinct keys | N=3, local, 1 / 2 / 4 / 8 clients | 3 096 / 5 659 / 7 890 / **9 372** writes/s |
| Throughput, distinct keys | N=5, local, 1 / 2 / 4 / 8 clients | 2 465 / 4 030 / 4 830 / **6 534** writes/s |
| Throughput, one key | N=3, local, 1 / 2 / 4 / 8 clients | 3 167 / 2 578 / 2 643 / **2 800** writes/s |
| Idle cost | N=3 + 1 client, nothing in flight | 0.45% of 10 cores (~0.045 core) |

## What the shape says

**A read is a write that changes nothing -- and is priced accordingly.** A linearizable read runs the
same consensus round as a write, but because it leaves the register where it found it, the round
finishes after the prepare phase: 0.17 ms locally and 53.2 ms across regions, against 0.29 ms and
105.1 ms for a write that changes the value. Serializable reads skip the round altogether -- 0.08 ms
locally, 0.12 ms across regions, since they never leave the coordinator. That last gap, *three
orders of magnitude* between regions, is the price list for `ReadConsistency.SERIALIZABLE` and the
agent's `?stale`.

**Distance costs two link round trips per write, one per read.** A write that changes the value pays
for prepare and then accept -- 105.1 ms at 25 ms/link, four times the one-way delay. Anything that
leaves the register unchanged stops after prepare and pays twice the delay: a linearizable read, and
a write of bytes the quorum confirms are already there (53.2 ms each). Serializable reads are exempt
entirely. Budget two inter-region round trips per write, one per linearizable read, none per
serializable read.

**More nodes cost throughput, not latency.** N=5 delivers about 30% less than N=3 at every client
count, which is the `2(N-1)` messages a coordinator sends per round. Latency barely moves, because a
quorum still waits for the same one slow reply. Size `N` for the failures you must survive, then
read the throughput column for what it costs -- this is the measured half of why online resizing is
not planned ([11. Limitations](11-limitations.md)).

**Contention on one key does not scale, and adding clients makes it worse.** Distinct keys scale
until saturation, 3.1k to 9.4k writes/s from one client to eight. The same eight clients on a single
key manage 2 800 writes/s -- a fifth of the uncontended rate -- and the curve is flat from two clients
onward: the parallelism is spent on duelling proposers. One writer per key is the shape this store
rewards; if a key must take concurrent writers, expect a per-key ceiling of a few thousand writes/s
regardless of how many clients you point at it.

**Idleness is not free but is close to it.** The event loop polls its transports and backs off with a
doubling park capped at 1 ms, so an idle three-node cluster with a client costs about 0.045 of a
core in total.

## What is not measured

- **No HTTP benchmark.** The agent's surface has no published throughput or latency figures.
- **No real network.** Every figure is one JVM; a deployment's numbers will differ in both
  directions, as the conditions above explain.
- **No sustained or soak measurement.** The envelope is a snapshot, not a stability claim.

## Related

- [3. CASPaxos](03-caspaxos.md) -- why a read costs a round, and why proposers duel
- [11. Limitations and compatibility](11-limitations.md) -- the boundaries these numbers sit inside
- [10. Testing](10-testing.md) -- what *is* a pass/fail claim, which none of this is
