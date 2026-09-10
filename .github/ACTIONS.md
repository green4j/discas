# GitHub Actions in discas

This directory contains CI/CD workflows for `discas`:

- `workflows/build.yml` - **Check and Verify**: `clean build` across the JDK matrix, plus `verify`.
- `workflows/release.yml` - publish artifacts to Sonatype (snapshots and releases), `verify` first.

## Check and Verify Workflow

`build.yml` (named **Check and Verify** in the Actions UI -- it runs both `check`, across the JDK
matrix, and `verify`) runs on:

- `pull_request` to `main`
- `push` to `main`
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
drift, which is why each job here names one Gradle task and nothing else.

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
./gradlew --no-daemon --stacktrace clean verify publish
```

- Release mode:

```bash
./gradlew --no-daemon --stacktrace clean verify publish uploadArtifactsToSonatypeCentralPortal
```

Both run `verify` first, in the same invocation as the upload - nothing is published which has not
passed the unit, integration, chaos and soak suites, the asserting examples and coverage of that
exact tree, rather than trusting an earlier run on another commit.

## An example of release

1. Set release version
```
echo 0.0.1 > version.txt
git add version.txt && git commit -m "Release 0.0.1"
git push
```
2. Initiate release pushing the tag
```
git tag v0.0.1
git push origin v0.0.1
```
3. Increment version to next snapshot
```
echo 0.0.2-SNAPSHOT > version.txt
git add version.txt && git commit -m "Back to snapshot" && git push
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
2. Update `version.txt` to a non-snapshot version (for example `0.0.1`).
3. Push the release commit and create/push tag `v0.0.1` (or run `release.yml` manually with `publish_mode=release`).
4. Wait for `release.yml` to publish and finalize artifacts.
5. After release, bump `version.txt` to next snapshot (for example `0.0.2-SNAPSHOT`).

## Troubleshooting

- **Missing secrets**: release workflow fails early with a clear message.
- **Version/mode mismatch**: verify `version.txt` suffix and selected `publish_mode`.
- **Signing failures**: ensure the key is ASCII-armored and password matches the key.
- **Sonatype upload errors**: retry after verifying credentials and Sonatype account permissions.
- **A release run takes the best part of an hour**: it carries the whole of `verify` before the
  upload, chaos and soak included. A red suite fails the release itself, before anything is staged.
