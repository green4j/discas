/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.lock.Lock;
import io.github.green4j.discas.client.lock.LockWriteResult;
import io.github.green4j.discas.client.lock.LockWriteStatus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Holding a lock for longer than one lease: renewing from a loop beside the work, and -- the part
 * that is actually load-bearing -- stopping the work the first time a renew is not
 * {@link LockWriteStatus#APPLIED}.
 * <p>
 * Short leases renewed beat long leases, because a crashed holder blocks everyone for whatever is
 * left of its lease and nothing can shorten that. The cost of short leases is this loop, and the
 * loop has one rule: <b>a renew that did not apply is not a retry, it is a stop</b>. Renewing again
 * from there is how two holders end up believing they own the same key -- the successor already has
 * it, and the answer said so.
 * <p>
 * Two runs, same code, one number different:
 * <ul>
 *   <li><b>Renewing well inside the lease.</b> The work runs past several lease lengths and the
 *       lock is still the caller's at the end.</li>
 *   <li><b>Renewing on a period longer than the lease.</b> A keep-alive that cannot keep anything
 *       alive -- the lease lapses in the gap, a waiting worker takes the key, and the first renew
 *       to come back says so. This is a configuration mistake rather than an exotic failure, and it
 *       is silent in any design where the work does not check.</li>
 * </ul>
 * The second run is what makes the first meaningful: the loop is only a safety property if it
 * reacts. Note also which clock the loop reads -- {@link Lock#remainingLease()} is monotonic and
 * anchored when the lease was requested, so a step in the wall clock cannot convince a holder it has
 * more time than it does.
 */
public final class KeepAliveExample {

    private static final long CALL_MS = 8_000L;
    private static final Duration LEASE = Duration.ofMillis(900);
    private static final Duration WORK = Duration.ofMillis(2_500);

    private KeepAliveExample() {
    }

    public static void main(final String[] args) throws Exception {
        final Path baseDir = Files.createTempDirectory("discas-keepalive-");
        try (ExampleCluster.RunningCluster cluster =
                     ExampleCluster.startTcpPeersWithFileWalCluster(2, baseDir)) {

            final DisCasClient worker = cluster.client(0);
            final DisCasClient contender = cluster.client(1);

            System.out.println("=== Keep-alive: renew beside the work, and stop when it fails ===");
            healthyRun(worker, "jobs/report");
            misconfiguredRun(worker, contender, "jobs/import");
            System.out.println("=== a keep-alive is a safety property only because the work checks it ===");
        } finally {
            ExampleFiles.deleteRecursively(baseDir);
        }
    }

    /** Renewing every third of a lease: the work outlives several leases and the lock stays ours. */
    private static void healthyRun(final DisCasClient client, final String key) throws Exception {
        final Duration period = LEASE.dividedBy(3);
        System.out.println("[1] " + LEASE.toMillis() + "ms lease, renewed every " + period.toMillis()
                + "ms, " + WORK.toMillis() + "ms of work");

        final Lock lock = ExampleOutcome.of(client.tryLock(key, LEASE, "worker-1"), CALL_MS)
                .require("tryLock").lock();
        final KeepAlive keepAlive = KeepAlive.start(lock, LEASE, period);

        final long unitsDone = doWork(keepAlive);
        keepAlive.stop();

        System.out.println("    work units completed: " + unitsDone
                + ", renews applied: " + keepAlive.renewsApplied()
                + ", lease still in hand: " + lock.remainingLease().toMillis() + "ms");
        require(keepAlive.healthy(), "the keep-alive must not have been refused: " + keepAlive.stoppedBecause());
        require(unitsDone == WORK.toMillis() / UNIT_MS, "the work must have run to completion");
        require(keepAlive.renewsApplied() > 0, "the lease must actually have been renewed");
        require(ExampleOutcome.of(lock.release(), CALL_MS).require("release").applied(),
                "the lock must still be ours to release");
    }

    /**
     * The same loop with the period set above the lease. Nothing throws, nothing logs an error, and
     * the lock is gone -- which is why the work has to be the thing that notices.
     */
    private static void misconfiguredRun(final DisCasClient worker, final DisCasClient contender,
                                         final String key) throws Exception {
        final Duration period = LEASE.multipliedBy(2);
        System.out.println("[2] the same " + LEASE.toMillis() + "ms lease, renewed every "
                + period.toMillis() + "ms -- a gap the lease cannot survive");

        final Lock lock = ExampleOutcome.of(worker.tryLock(key, LEASE, "worker-1"), CALL_MS)
                .require("tryLock").lock();
        final KeepAlive keepAlive = KeepAlive.start(lock, LEASE, period);

        // Someone was waiting for this key, and a lapsed lease is theirs to take. Nothing about
        // this is a fault: it is the contract, arriving on schedule.
        final Lock successor = ExampleOutcome
                .of(contender.lock(key, Duration.ofSeconds(30), Duration.ofSeconds(5), "worker-2"), CALL_MS)
                .require("worker-2 lock").lock();
        System.out.println("    worker-2 took the key at generation " + successor.fencingToken());

        final long unitsDone = doWork(keepAlive);
        keepAlive.stop();

        System.out.println("    work stopped after " + unitsDone + " units, because renew -> "
                + keepAlive.stoppedBecause());
        require(!keepAlive.healthy(), "the keep-alive must have been refused once the key changed hands");
        require(keepAlive.stoppedBecause() == LockWriteStatus.HELD_BY_OTHER,
                "and must say which refusal it was, so the caller knows not to re-acquire blindly");
        require(unitsDone < WORK.toMillis() / UNIT_MS,
                "the work must have stopped short rather than run on without a lock");

        ExampleOutcome.of(successor.release(), CALL_MS).require("worker-2 release");
    }

    private static final long UNIT_MS = 100L;

    /** Work in units, checking between them. Nothing here talks to the cluster. */
    private static long doWork(final KeepAlive keepAlive) throws InterruptedException {
        final long units = WORK.toMillis() / UNIT_MS;
        for (long done = 0; done < units; done++) {
            if (!keepAlive.healthy()) {
                return done;
            }
            Thread.sleep(UNIT_MS);
        }
        return units;
    }

    private static void require(final boolean condition, final String claim) {
        if (!condition) {
            throw new AssertionError(claim);
        }
    }

    /**
     * A renew loop on its own thread. It holds no opinion about what to do when a renew is refused
     * -- it records the status and stops renewing, and the work decides. Keeping the two apart is
     * what lets the work stop at a point of its own choosing rather than mid-write.
     */
    private static final class KeepAlive implements Runnable {
        private final Lock lock;
        private final Duration leaseTtl;
        private final Duration period;
        private final Thread thread;
        private volatile boolean shutdown;
        private volatile LockWriteStatus stoppedBecause;
        private volatile int renewsApplied;

        private KeepAlive(final Lock lock, final Duration leaseTtl, final Duration period) {
            this.lock = lock;
            this.leaseTtl = leaseTtl;
            this.period = period;
            this.thread = new Thread(this, "lock-keep-alive");
            this.thread.setDaemon(true);
        }

        static KeepAlive start(final Lock lock, final Duration leaseTtl, final Duration period) {
            final KeepAlive keepAlive = new KeepAlive(lock, leaseTtl, period);
            keepAlive.thread.start();
            return keepAlive;
        }

        @Override
        public void run() {
            while (!shutdown && stoppedBecause == null) {
                try {
                    Thread.sleep(period.toMillis());
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (shutdown) {
                    return;
                }
                final ExampleOutcome<LockWriteResult> renew =
                        ExampleOutcome.of(lock.renew(leaseTtl), CALL_MS);
                if (!renew.applied()) {
                    // Unreachable cluster, not a refusal: the lease is running out either way, so
                    // there is nothing to do but try again inside whatever is left of it.
                    System.out.println("    " + renew.describe("renew") + "; will try again");
                    continue;
                }
                final LockWriteResult result = renew.value();
                if (result.applied()) {
                    renewsApplied++;
                    continue;
                }
                // The lock is not ours any more. Publish why and stop renewing; re-acquiring here
                // would be this thread deciding something the work is entitled to decide.
                stoppedBecause = result.status();
                return;
            }
        }

        boolean healthy() {
            return stoppedBecause == null;
        }

        LockWriteStatus stoppedBecause() {
            return stoppedBecause;
        }

        int renewsApplied() {
            return renewsApplied;
        }

        void stop() throws InterruptedException {
            shutdown = true;
            thread.interrupt();
            thread.join(CALL_MS);
        }
    }
}
