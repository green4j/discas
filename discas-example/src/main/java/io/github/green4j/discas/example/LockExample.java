/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockInfoResult;
import io.github.green4j.discas.client.lock.LockInfoStatus;
import io.github.green4j.discas.client.lock.LockToken;
import io.github.green4j.discas.client.DisCasClient;

import io.github.green4j.discas.client.lock.LockWriteResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The distributed lock, end to end: acquire, contend, expire, steal, fence, renew, release -- and
 * the recovery an acquire needs when its outcome is not known.
 * <p>
 * The lock is not a protocol feature. It is a value in one key, written with the same version-fenced
 * compare-and-set every other write uses, which is why it inherits that write's failure story rather
 * than having one of its own. Two consequences worth reading the code for:
 * <ul>
 *   <li><b>The lease is not the safety mechanism -- the fencing token is.</b> A lease expires
 *       against each client's own wall clock, so two clients can disagree about whether it has.
 *       {@code fencingToken()} is a strictly monotonic per-key generation, so a stale holder's
 *       token is recognisably old to whatever it guards. Pass the token to the protected resource;
 *       do not trust the clock.</li>
 *   <li><b>An acquire whose outcome is unknown may have succeeded.</b> A lock nobody knows they
 *       hold is held until its lease runs out, so the recovery is a read
 *       ({@code getLockInfo}) and a {@code release} with the token -- never a blind retry. See
 *       {@code CoordinatorFailoverExample} scenario 4 for that path under an actual node loss.</li>
 * </ul>
 */
public final class LockExample {
    private LockExample() {
    }

    public static void main(final String[] args) throws Exception {
        final Path baseDir = Files.createTempDirectory("caspaxos-tcp-lock-");
        try (ExampleCluster.RunningCluster cluster =
                     ExampleCluster.startTcpPeersWithFileWalCluster(2, baseDir)) {
            final DisCasClient clientA = cluster.client(0);
            final DisCasClient clientB = cluster.client(1);
            final String key = "job-runner";

            System.out.println("=== TCP peers + FileWal distributed lock example ===");
            for (int i = 0; i < cluster.walDirs.size(); i++) {
                System.out.println("Node-" + (i + 1) + " wal layout dir: " + cluster.walDirs.get(i));
            }

            // The one call in this example whose failure is dangerous rather than merely
            // inconvenient: an unknown outcome may mean we hold the lock without being told, and a
            // lock nobody knows they hold is held until its lease runs out.
            final ExampleOutcome<LockAcquireResult> acquire = ExampleOutcome.of(
                    clientA.tryLock(key, Duration.ofMillis(500), "owner-A"), 6_000L);
            if (acquire.unknown()) {
                System.out.println(acquire.describe("clientA.tryLock"));
                recoverPossiblyHeldLock(clientA, key, "owner-A");
                return;
            }
            final LockAcquireResult shortLease = acquire.require("clientA.tryLock");
            System.out.println("clientA.tryLock(" + key + ", 500ms, owner-A) -> status="
                    + shortLease.status()
                    + ", leaseUntilEpochMs=" + shortLease.lock().lockInfo().snapshot().leaseUntilEpochMs());

            final LockAcquireResult quickTimeout = clientB
                    .lock(key, Duration.ofSeconds(3), Duration.ofMillis(200), "owner-B")
                    .get(6, TimeUnit.SECONDS);
            System.out.println("clientB.lock(" + key + ", 3s lease, 200ms wait, owner-B) -> status="
                    + quickTimeout.status()
                    + ", observedOwner=" + quickTimeout.lockInfo().ownerId());

            System.out.println("Waiting for short lease to expire...");
            Thread.sleep(700L);

            final LockInfoResult expiredView = clientB.getLockInfo(key)
                    .get(6, TimeUnit.SECONDS);
            System.out.println("getLockInfo after expiry -> status=" + expiredView.status()
                    + ", expiredFlag=" + expiredView.info().expired());

            // No-owner lock(): a random UUID is generated as ownerId
            final LockAcquireResult stolen = clientB
                    .lock(key, Duration.ofSeconds(3), Duration.ofSeconds(2))
                    .get(8, TimeUnit.SECONDS);
            System.out.println("clientB.lock(" + key + ", 3s lease, 2s wait, auto-owner) -> status="
                    + stolen.status()
                    + ", autoOwner=" + stolen.lock().lockInfo().snapshot().ownerId());

            // Fencing token: strictly monotonic per-key generation captured at acquire
            System.out.println("stolen.lock().fencingToken() -> " + stolen.lock().fencingToken());

            // validate() re-reads the cluster's current generation; true while we hold
            final boolean validHolder = stolen.lock().validate().get(6, TimeUnit.SECONDS);
            System.out.println("stolen.lock().validate() while held -> " + validHolder);

            // Low-level release(key, LockToken): a wrong token cannot release the lock
            final LockToken wrongToken =
                    new LockToken(new byte[stolen.lock().token().bytes().remaining()]);
            final LockWriteResult wrongRelease = clientB.release(key, wrongToken)
                    .get(6, TimeUnit.SECONDS);
            System.out.println("clientB.release(key, wrongToken) -> " + wrongRelease.status());

            // Low-level renewLock(key, token, ttl) using the held token
            final LockWriteResult lowLevelRenew = clientB
                    .renewLock(key, stolen.lock().token(), Duration.ofSeconds(4))
                    .get(6, TimeUnit.SECONDS);
            System.out.println("clientB.renewLock(key, heldToken, 4s) -> " + lowLevelRenew.status());

            final LockWriteResult released = stolen.lock().release().get(6, TimeUnit.SECONDS);
            System.out.println("clientB.release() -> " + released.status());

            System.out.println("Final getLockInfo -> status="
                    + clientA.getLockInfo(key).get(6, TimeUnit.SECONDS).status());
        }
    }

    /**
     * The recovery for an acquire whose outcome is unknown: ask who holds the lock, and if it is
     * us, release it with the token from the record. Never a blind retry -- retrying an acquire we
     * may already hold just leaves the first one to expire on its own.
     */
    private static void recoverPossiblyHeldLock(final DisCasClient client, final String key,
                                                final String ownerId) {
        final ExampleOutcome<LockInfoResult> info =
                ExampleOutcome.of(client.getLockInfo(key), 6_000L);
        if (!info.applied()) {
            System.out.println("  " + info.describe("getLockInfo")
                    + " -- cannot tell; the lease will expire on its own");
            return;
        }
        final LockInfoResult held = info.value();
        if (held.status() != LockInfoStatus.LOCKED || !ownerId.equals(held.info().ownerId())) {
            System.out.println("  getLockInfo -> " + held.status() + ": we do not hold it");
            return;
        }
        final ExampleOutcome<LockWriteResult> released =
                ExampleOutcome.of(client.release(key, held.info().token()), 6_000L);
        System.out.println("  we did hold it; " + released.describe("release"));
    }
}
