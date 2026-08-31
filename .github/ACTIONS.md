# GitHub Actions in discas

This directory contains CI/CD workflows for `discas`:

- `workflows/build.yml` - **Check and Verify**: `clean build` across the JDK matrix, plus `verify`.
- `workflows/verify.yml` - **Verify**: the definition of "green", reusable so the workflow above and
  `release.yml` share one copy of it rather than each restating the steps.
- `workflows/release.yml` - publish artifacts to Sonatype (snapshots and releases).

## Check and Verify Workflow

`build.yml` (named **Check and Verify** in the Actions UI -- it runs both `check`, across the JDK
matrix, and `verify`) runs on:

- `pull_request` to `main`
- `push` to `main`
- a nightly schedule (`0 3 * * *`)
- manual trigger (`workflow_dispatch`)

It has two jobs.

**`gradle-build`** - the portability sweep. A Temurin JVM matrix of `11`, `17`, `21` and `25`, each
with its own Gradle cache, running:

```bash
./gradlew --no-daemon --stacktrace clean build
```

**`verify`** - the definition of "green", on the JDK 11 baseline:

```bash
./gradlew --no-daemon --stacktrace verify
```

The split is what `build` leaves out. It excludes `@Tag("chaos")` - the soak and fuzz suites, which
answer a different question from "did this change break anything" and cost a third of the wall time
- so running it four times over is a JDK sweep, not a weaker test run. `verify` is the whole thing:
unit, integration, chaos and soak, the asserting examples, and aggregated coverage. What it means
stays in `build.gradle`; a second copy in a list of workflow steps is a second place for it to
drift.

Locally the same axis is `./gradlew testQuick` (chaos included, scaled down) and `./gradlew
testLong` (the full soak).

## Release and Publish Workflow

`release.yml` supports two publishing modes:

- **snapshot** - publish `*-SNAPSHOT` versions to Sonatype snapshots repository.
- **release** - publish release versions and finalize through Sonatype Central Portal.

The publish workflow runs on **JDK 11** to keep release artifacts built from the minimum supported Java baseline.

### Triggers

- manual trigger (`workflow_dispatch`) with `publish_mode` input (`snapshot` or `release`)
- git tag push matching `v*` (automatically uses `release` mode)

### Commands

- Snapshot mode:

```bash
./gradlew --no-daemon --stacktrace clean publish
```

- Release mode:

```bash
./gradlew --no-daemon --stacktrace clean publish uploadArtifactsToSonatypeCentralPortal
```

## Published Artifacts

Group `io.github.green4j`:

| Artifact | What it is |
|---|---|
| `discas-common` | identity, codec, transport and observability primitives |
| `discas-node` | the CASPaxos node: acceptor, proposer, storage, anti-entropy |
| `discas-client` | the client API: compare-and-set, lease locks, scan and watch |
| `discas-agent` | the HTTP/JSON sidecar in front of a cluster |
| `discas-admin` | the operator command: dump, load and init |
| `discas-all` | the five above in one shaded jar |

Each carries a sources jar and a javadoc jar; `discas-all` carries one javadoc over all five.
`discas-example`, `discas-int-test` and `discas-performance` are not published - nobody deploys
sample code, a test module or an instrument.

## Required GitHub Secrets

Configure these secrets in repository or organization settings:

- `SONATYPE_USERNAME`
- `SONATYPE_PASSWORD`
- `SIGNING_GPG_SECRET_KEY` (ASCII-armored private key)
- `SIGNING_GPG_PASSWORD`

The Gradle build reads these from environment variables in `build.gradle`.

## Version Rules

Version is read from `version.txt`.

- Snapshot mode requires version ending with `-SNAPSHOT`.
- Release mode requires version **without** `-SNAPSHOT`.

The workflow validates this before running Gradle.

## Recommended Release Procedure

1. Ensure `build.yml` is green on `main`.
2. Update `version.txt` to a non-snapshot version (for example `0.1.0`).
3. Push the release commit and create/push tag `v0.1.0` (or run `release.yml` manually with `publish_mode=release`).
4. Wait for `release.yml` to publish and finalize artifacts.
5. After release, bump `version.txt` to next snapshot (for example `0.1.1-SNAPSHOT`).

## Troubleshooting

- **Missing secrets**: release workflow fails early with a clear message.
- **Version/mode mismatch**: verify `version.txt` suffix and selected `publish_mode`.
- **Signing failures**: ensure the key is ASCII-armored and password matches the key.
- **Sonatype upload errors**: retry after verifying credentials and Sonatype account permissions.
