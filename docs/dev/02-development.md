# 2. Development environment

## Context

Gradle multi-project, eight modules, no runtime dependencies at all -- the store, its client, its
HTTP server, its CLI parser and its metric registry are all written here. Test-only dependencies are
JUnit 5 and, for two suites, Testcontainers.

The compile target is **Java 11**, pinned with `options.release = 11` rather than `source`/`target`.
That validates against the bundled Java 11 API signatures, so a call to a newer API fails the build
instead of producing bytecode that will not load. Build with a newer JDK freely; just do not expect
`Java 12+` APIs to be available.

## Repository structure

```
discas-common/        no dependencies: wire types, event loop, HTTP server, CLI, metrics
discas-node/          a cluster member
discas-client/        the client library
discas-agent/         HTTP/JSON front end
discas-admin/         operator CLI (dump / load / init)
discas-example/       runnable examples; some assert their claims
discas-performance/   the performance envelope and its instruments
discas-int-test/      test-only module: cross-module, cluster, chaos and crash suites
discas-all/           no sources: the five deployable modules bundled into one jar
docs/dev/             this guide
docs/operator/        operator handbook: the node, membership, quorum, access, runbooks
buildSrc/             one Gradle task: hand a staged repository to the Sonatype Central Portal
config/checkstyle/    the style rules and their suppressions
.github/workflows/    build and release; see .github/ACTIONS.md
```

Modules have **no `build.gradle` of their own**: the whole build is the root `build.gradle`, one
`configure(...)` block per module. What varies between them is small enough that spreading it over
nine files hides it, and one file is where you can read what every module has in common. The two
values that change often live outside it: the version in `version.txt`, and the group, author,
Java baseline and tool versions in `gradle.properties`.

`discas-int-test` has no main sources. Anything that needs more than one module to exercise lives
there, which is why `discas-node`'s own test directory is small and its coverage looks thin in a
per-module report -- see [10. Testing](10-testing.md).

## Build

```bash
./gradlew compileJava          # compile main sources across every module
./gradlew build                # full build; runs `test`, so chaos suites are excluded
./gradlew :discas-all:assemble # one jar of common+node+client+agent+admin, plus sources and javadoc
./gradlew check                # every verification task of every module
./gradlew javadoc              # doclint is on: a broken {@link} fails here, not silently
./gradlew publishToMavenLocal  # the six published artifacts, into ~/.m2
```

`check` runs checkstyle over main and test sources. The rules are in `config/checkstyle/`, and a
rule that a piece of code is deliberately outside is suppressed there by name, with the reason.
Compilation is `-Xlint:all -Werror`: a warning fails the build.

For a fast inner loop on one module: `./gradlew :discas-node:compileJava`,
`:discas-common:test --tests '*http.server*'`, and so on.

## Run a node

Two ways, and they take identical arguments. From a build tree, launch the main class:

```bash
./gradlew build
CP=$(ls discas-*/build/libs/*.jar | grep -v tests | paste -sd: -)

java -cp "$CP" io.github.green4j.discas.node.starter.DisCasNodeStarter \
    --node-id 1 --cluster-id dev \
    --members 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
    --wal-dir /tmp/discas-1 --client-bind 127.0.0.1:8001
```

Or build the start scripts. `discas-node`, `discas-agent` and `discas-admin` carry the `application`
plugin, so each produces a wrapper named after its module -- which is what `discas-admin init` writes
into a generated `RUN.md`:

```bash
./gradlew :discas-node:installDist   # -> discas-node/build/install/discas-node/bin/discas-node
```

The wrapper adds nothing but the classpath. The other five modules are libraries and have no main
class.

Every option is also settable from the environment: the `DISCAS_*` name is derived mechanically
from the long option (`--wal-dir` -> `DISCAS_WAL_DIR`). `--help` prints the full reference with each
value's default and env name; the starter prints the effective configuration and where each value
came from at startup, with secrets masked. The agent and the admin CLI start the same way, from
`agent.starter.DisCasAgentStarter` and `admin.starter.DisCasAdminStarter`.

Embedded, in a test or an application: build a `NodeConfig`, hand it to `DisCasNodeFactory.create`
with a transport bootstrap and a `Wal`. `ExampleCluster` in `discas-example` is the short version of
this wiring, and `TestCluster` in `discas-int-test` is the version that also waits for readiness.

## A local cluster

The quickest three-node cluster is an example -- no ports to pick, no directories to clean up. With
`$CP` from above:

```bash
java -cp "$CP" io.github.green4j.discas.example.QuickstartExample
```

Others worth knowing: `ContentionExample` (two clients on one key), `CoordinatorFailoverExample`
(kills a node mid-flight and asserts the failover table), `WatchExample` (a burst of commits against
the number of times a watcher wakes), `LockExample`, `SecureClientAclExample`
(token auth plus a prefix ACL) and `SecureClusterFileMembersExample` (peer mTLS through a CA
rotation) -- the last provisions a demo PKI with `keytool`, which must be on `PATH`.

The asserting examples are wired into the build and fail it when a claim breaks:

```bash
./gradlew runAssertingExamples
```

## Tests

Two independent axes -- *which* suites run, and *how hard* they run. Both are covered in
[10. Testing](10-testing.md); the short form:

```bash
./gradlew test        # everything except @Tag("chaos")
./gradlew testQuick   # everything including chaos, scaled down (pre-commit)
./gradlew testLong    # everything including chaos, full soak
./gradlew verify      # check + testLong + asserting examples + coverage  (~23 min)
```

CI is two workflows, described in [.github/ACTIONS.md](../../.github/ACTIONS.md). `build.yml` runs
`./gradlew clean build` across a JDK 11/17/21/25 matrix and `./gradlew verify` once on the baseline;
`release.yml` publishes to Sonatype. The definition of "green" stays in the build rather than being
restated as workflow steps, so a workflow only ever names a Gradle task.

## Debugging

**Attach a debugger** to any Gradle-launched JVM with `--debug-jvm`; it suspends until you connect.

**Breakpoints on an event loop** are awkward: a node's loop is also its timer thread, so pausing it
stops peer traffic and heartbeat-ish timers with it, and connections time out while you look
around. For a cluster, prefer raising timeouts (`--round-timeout-ms`, `--peer-response-timeout-ms`)
over stepping.

**Observability beats printf.** A node exposes `/metrics`, `/health` and `/ready` on its
observability port, and every event goes through `NodeObserver`; a test can attach its own
implementation and assert on the events instead of reading a log. See
[4. The node](04-node.md#observability-seams).

**Docker** is needed only by the step-ca TLS suites. The build asks the Docker CLI where its
endpoint is (`docker context inspect`) and forwards it, so colima, Rancher Desktop and rootless
Podman work without setting `DOCKER_HOST` by hand. Without any Docker those suites fail on the
environment rather than being skipped.

## Common problems

| Symptom | Cause |
|---|---|
| `Could not find a valid Docker environment` | no Docker for the step-ca suites; the rest of the suite is unaffected |
| A node starts but `/ready` stays 503 | it has no promise floor yet -- it started without durable state and is waiting for a quorum to answer. Expected until enough members are up; see [5. Storage](05-storage.md) |
| Peers refuse to connect | mismatched `--cluster-id`, `--cluster-size`, or a member list that does not include this node. The refusal reason is a `PeerHelloRespStatus` -- see [8. Transport and security](08-transport-security.md) |
| A port is taken on a rerun | tests allocate from a range outside every common ephemeral range (`TestPorts`); an ordinary process rerun does not |
