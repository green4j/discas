/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockAcquireStatus;
import io.github.green4j.discas.client.lock.Lock;
import io.github.green4j.discas.client.lock.LockInfoResult;
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
 *       hold is held until its lease runs out, so the recovery is {@code recoverLock(key, ownerId)}
 *       -- never a blind retry, which at best waits out your own lease. This is what the owner id
 *       is for, and why there is no overload that invents one: it is the only field of the record
 *       you choose before the write, so it is the only thing a later read can match you against.
 *       See {@code CoordinatorFailoverExample} scenario 4 for that path under an actual node
 *       loss.</li>
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
            // remainingLease() and not a stored deadline: a holder measures its own lease on the
            // monotonic clock, so that an NTP step cannot quietly lengthen or shorten it.
            System.out.println("clientA.tryLock(" + key + ", 500ms, owner-A) -> status="
                    + shortLease.status()
                    + ", remainingLease=" + shortLease.lock().remainingLease().toMillis() + "ms");

            final LockAcquireResult quickTimeout = clientB
                    .lock(key, Duration.ofSeconds(3), Duration.ofMillis(200), "owner-B")
                    .get(6, TimeUnit.SECONDS);
            System.out.println("clientB.lock(" + key + ", 3s lease, 200ms wait, owner-B) -> status="
                    + quickTimeout.status()
                    + ", observedOwner=" + quickTimeout.observed().ownerId());

            System.out.println("Waiting for short lease to expire...");
            Thread.sleep(700L);

            final LockInfoResult expiredView = clientB.getLockInfo(key)
                    .get(6, TimeUnit.SECONDS);
            System.out.println("getLockInfo after expiry -> status=" + expiredView.status()
                    + ", expiredFlag=" + expiredView.info().expired());

            final LockAcquireResult stolen = clientB
                    .lock(key, Duration.ofSeconds(3), Duration.ofSeconds(2), "owner-B")
                    .get(8, TimeUnit.SECONDS);
            System.out.println("clientB.lock(" + key + ", 3s lease, 2s wait, owner-B) -> status="
                    + stolen.status()
                    + ", owner=" + stolen.lock().ownerId());

            // Not reentrant, and it says so precisely. A second acquire under a name that already
            // holds the key reports HELD_BY_SELF rather than HELD_BY_OTHER, and hands back no
            // Lock -- were two components sharing an owner id, giving the second one a working
            // lock is exactly how mutual exclusion would be lost without anyone noticing. The
            // waiting form stops here too instead of spending its budget on a lease it cannot win.
            final LockAcquireResult again = clientB
                    .lock(key, Duration.ofSeconds(3), Duration.ofSeconds(2), "owner-B")
                    .get(8, TimeUnit.SECONDS);
            System.out.println("clientB.lock(" + key + ", same owner-B) -> status=" + again.status()
                    + ", lock=" + again.lock() + " (no second lock is handed out)");

            // recoverLock is where the owner id is trusted, and asking for it is how you say the
            // name really is yours. It rebuilds the Lock from the standing record -- same token,
            // same generation -- so it releases and renews like the original.
            final LockAcquireResult recovered = clientB.recoverLock(key, "owner-B")
                    .get(8, TimeUnit.SECONDS);
            System.out.println("clientB.recoverLock(" + key + ", owner-B) -> status="
                    + recovered.status()
                    + ", sameGeneration="
                    + (recovered.lock().fencingToken() == stolen.lock().fencingToken()));

            fenceRenewAndLetGo(clientB, clientA, key, stolen.lock());
        }
    }

    /** The end of a tenancy: what the token refuses, what it still allows, and what it leaves behind. */
    private static void fenceRenewAndLetGo(final DisCasClient holder,
                                           final DisCasClient reader,
                                           final String key,
                                           final Lock lock) throws Exception {
        // Fencing token: strictly monotonic per-key generation captured at acquire
        System.out.println("lock.fencingToken() -> " + lock.fencingToken());

        // A fresh read says who holds it now -- useful for a log, useless as a permission check:
        // the answer is already stale when it arrives. That is what the fencing token is for.
        final LockInfoResult live = reader.getLockInfo(key).get(6, TimeUnit.SECONDS);
        System.out.println("getLockInfo while held -> status=" + live.status()
                + ", stillOurGeneration=" + (live.info().generation() == lock.fencingToken()));

        // Low-level release(key, LockToken): a wrong token cannot release the lock
        final LockToken wrongToken = new LockToken(new byte[lock.token().bytes().remaining()]);
        final LockWriteResult wrongRelease = holder.release(key, wrongToken).get(6, TimeUnit.SECONDS);
        System.out.println("release(key, wrongToken) -> " + wrongRelease.status());

        // Low-level renewLock(key, token, ttl) using the held token
        final LockWriteResult lowLevelRenew = holder
                .renewLock(key, lock.token(), Duration.ofSeconds(4)).get(6, TimeUnit.SECONDS);
        System.out.println("renewLock(key, heldToken, 4s) -> " + lowLevelRenew.status());

        final LockWriteResult released = lock.release().get(6, TimeUnit.SECONDS);
        System.out.println("release() -> " + released.status());

        // A release is a write too, so its answer can be lost -- and a retry has to be able to tell
        // "mine already landed" from "no lock here", which are the same key state and wildly
        // different news. The marker keeps the releasing token, so it can.
        final LockWriteResult releasedAgain = lock.release().get(6, TimeUnit.SECONDS);
        System.out.println("release() again -> " + releasedAgain.status()
                + ", applied=" + releasedAgain.applied() + " (nothing written, and nothing to do)");

        // And the key stays auditable while free: UNLOCKED, but the marker still names the tenancy
        // it ended, the same way EXPIRED keeps the record of a lease being taken over.
        final LockInfoResult finalState = reader.getLockInfo(key).get(6, TimeUnit.SECONDS);
        System.out.println("Final getLockInfo -> status=" + finalState.status()
                + ", lastHeldBy=" + finalState.info().ownerId()
                + ", generation=" + finalState.info().generation());
    }

    /**
     * The recovery for an acquire whose outcome is unknown. One call: it either hands back the
     * lock the lost acquire actually took -- the same lease, so releasing it really does release
     * it -- or says NOT_HELD, meaning nothing was written and the acquire is safe to issue again.
     * Never a blind retry, which would at best wait out our own lease.
     */
    private static void recoverPossiblyHeldLock(final DisCasClient client, final String key,
                                                final String ownerId) {
        final ExampleOutcome<LockAcquireResult> recovery =
                ExampleOutcome.of(client.recoverLock(key, ownerId), 6_000L);
        if (!recovery.applied()) {
            System.out.println("  " + recovery.describe("recoverLock")
                    + " -- still cannot tell; the lease will expire on its own");
            return;
        }
        final LockAcquireResult held = recovery.value();
        if (held.status() != LockAcquireStatus.ACQUIRED) {
            System.out.println("  recoverLock -> " + held.status() + ": we do not hold it");
            return;
        }
        final ExampleOutcome<LockWriteResult> released =
                ExampleOutcome.of(held.lock().release(), 6_000L);
        System.out.println("  we did hold it; " + released.describe("release"));
    }
}
