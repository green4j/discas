# 10. Testing

## Context

Most of what proves this store correct does **not** live next to the code it tests:
`discas-int-test` is a test-only module holding everything that needs more than one module -- cluster
tests, chaos suites, crash suites -- which is why `discas-node`'s own test directory looks thin.

Two independent axes control a run. **Which** suites execute, by JUnit tag. **How hard** they run,
by profile. Confusing them is the usual mistake: `testQuick` is not "fewer tests", it is *every*
test with durations scaled down.

## Run modes

```bash
./gradlew test          # everything EXCEPT @Tag("chaos")     -- the default, and what `build` runs
./gradlew testQuick     # everything INCLUDING chaos, discas.test.profile=quick   (pre-commit)
./gradlew testLong      # everything INCLUDING chaos, discas.test.profile=long    (full soak)
./gradlew check         # every verification task of every module
./gradlew verify        # check + testLong + asserting examples + coverage        (~23 min)
./gradlew coverageReport
./gradlew runAssertingExamples
```

Both axes are overridable per invocation:

```bash
./gradlew test -PincludeTags=chaos      # only the chaos suites
./gradlew test -PexcludeTags=           # everything, chaos included, standard profile
./gradlew test -Ddiscas.test.profile=long
./gradlew :discas-common:test --tests '*http.server*'
```

Any `-Ddiscas.*` system property is forwarded into the test JVM, so a suite's own knobs work the
same way.

### The profile axis

`TestProfile` -- `QUICK` (x0.5), `STANDARD` (x1.0, the default), `LONG` (x3.0) -- scales durations and
iteration counts inside a suite rather than selecting suites. A test reads it with
`TestProfile.current().scale(n)`, and some skip themselves below a threshold
(`assumeTrue(profile.atLeast(STANDARD))`). An unrecognised value falls back to `STANDARD` rather
than failing.

### Why `test` excludes chaos

The chaos suites are a third of the wall time and are soak tests, which is a different question from
"did this change break anything". They are **not optional** -- `testQuick` runs them scaled down and
`testLong` runs the full soak -- but `./gradlew build` stopped paying for them on every run.

**`./gradlew test` passing therefore does not mean the project is green**, and that has cost this
project once: the chaos suites were red for an unknown stretch while `test` stayed green, because
nothing else ran them. `verify` is the single definition of everything-checked, and CI
(`.github/workflows/verify.yml`) runs exactly that.

## Levels

| Level | Where | What it is for |
|---|---|---|
| Unit | each module's `src/test` | one class, no cluster -- codecs, config, the store, the HTTP server |
| Cross-module | `discas-int-test` | anything needing node + client together |
| Cluster | `discas-int-test` | real nodes over real transports, with a `TestCluster` harness |
| Chaos / soak | `discas-int-test`, `@Tag("chaos")` | a nemesis: partitions, drops, restarts, kills |
| Crash | `discas-int-test/.../crash` | a separate OS process killed with a real `SIGKILL` |
| Executable examples | `discas-example` | the examples that assert their claims, and fail the build when one does not hold |

The 13 chaos-tagged classes: `LinearizabilityCheckerChaosTest`, `ClusterKillNineChaosTest`,
`KillNineCrashTest`, `NetworkChaosTest`, `AntiEntropyChaosTest`, `LockChaosTest`,
`TcpClientTransportChaosTest`, `TcpFileWalMonkeyChaosTest`, `WalFaultBackpressureSoakTest`,
`FuzzySnapshotConcurrencyTest`, `TombstoneCollectionMatrixTest`, `CodecFuzzTest`,
`StepCaRotationIT`.

## `*MatrixTest` means the space is closed

A test whose name ends in `MatrixTest` **enumerates its state space**, over axes its class javadoc
names. It drives every cell, says how many there are, and names the ones it skips and why. The
mechanism that keeps it closed is part of the test -- an exhaustive `switch` with a failing
`default`, or a check that the rows cover the enum -- so a case added to the domain fails the build
until it has a row.

The six: `StorageRecoveryMatrixTest` (snapshot x compacted x removed), `CeilingRecoveryMatrixTest`
(each of three members provable / unprovable / down), `TombstoneCollectionMatrixTest` (what a member
lost x when it went away), `MembershipReloadMatrixTest` (every edit to a live member file),
`ClientAclRevisionMatrixTest` (every edit x a node that applied it and one that has not),
`PeerHandshakeRefusalMatrixTest` (every shape of `PEER_HELLO` x the answer it must get).

**The name marks a decision.** These are the places where sampled tests already missed real defects
-- two rows of `StorageRecoveryMatrixTest` were bugs that passed the tests of their time -- so the
space is closed and stays closed. Earn the name by enumerating something.

## What holds up which guarantee

| Claim | What backs it |
|---|---|
| A history under a killing nemesis is linearizable | `ClusterKillNineChaosTest` -- every node a real process, killed with `SIGKILL` |
| ...and every acknowledged write survives | the same suite's **witness keys** -- written once, never touched again, so ordinary traffic cannot repair them into looking fine |
| A partitioned cluster admits no bad history | `LinearizabilityCheckerChaosTest` + `RegisterLinearizabilityChecker` |
| A lost unforced WAL tail stays safe | `PromiseDurabilityTest`, and `KillNineCrashTest` for the real thing |
| Every directory shape recovers correctly | `StorageRecoveryMatrixTest` -- enumerated, not sampled |
| Tombstone collection is safe and sticks | `TombstoneCollectionMatrixTest` across every way a member leaves and returns |
| Lock leases do not rest on one client's clock | `LockClockSkewIntegrationTest` |
| Decoders reject malformed input | `CodecFuzzTest` -- truncation at every length, trailing garbage, seeded corruption |

## Harnesses worth knowing

| Harness | Use |
|---|---|
| `discas-int-test/src/test/java/io/github/green4j/discas/TestCluster.java` | N nodes + M clients, in-process, waits for readiness by *asking* rather than sleeping |
| `discas-int-test/src/test/java/io/github/green4j/discas/chaos/ChaosProxy.java` | frame-aware TCP proxy: isolate a direction, drop a frame type, cut live connections |
| `discas-int-test/src/test/java/io/github/green4j/discas/crash/NodeProcess.java` | a node as a real OS process, restartable on the same directory and ports |
| `discas-int-test/src/test/java/io/github/green4j/discas/node/wal/InMemoryWal.java` | a log that can lose its unforced tail, swallow records, or fail a force on demand |
| `discas-int-test/src/test/java/io/github/green4j/discas/TestAwait.java` | poll until a condition holds -- never `Thread.sleep` |
| `discas-int-test/src/test/java/io/github/green4j/discas/TestPorts.java` | ports from a range outside every common ephemeral range |

## Conventions

- **Never sleep for a condition.** A fixed sleep is wrong in both directions -- flaky on a loaded
  machine, slow on an idle one. Poll with `TestAwait`, or wait on an observer event.
- **Tests do not assert on volatile text** -- no console output, no exception messages, no operator
  log lines. Assert on codes, states and observable protocol.
- **Coverage is read as a dead-code detector first.** The report is aggregated across modules on
  purpose: a per-module view marks the core uncovered because `discas-node` is exercised from
  `discas-int-test`. A main-source class at 0% after the full suite is either unreachable or
  untested, and both are findings.
- **Docker** is needed only by the step-ca suites; the build discovers the endpoint from
  `docker context inspect`, so colima and rootless Podman work unconfigured.

## Adding a test

Put it in the module it can reach. If it needs a cluster it goes in `discas-int-test`; if it needs a
nemesis it goes there and gets `@Tag("chaos")`, plus a profile scale if its duration matters. A
suite that is not tagged runs in `test`, which means it must be fast and deterministic -- that is the
contract of the default task.
