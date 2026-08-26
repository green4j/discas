/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.client.lock.Lock;
import io.github.green4j.discas.client.lock.LockAcquireResult;
import io.github.green4j.discas.client.lock.LockAcquireStatus;
import io.github.green4j.discas.client.lock.LockInfoResult;
import io.github.green4j.discas.client.lock.LockInfoStatus;
import io.github.green4j.discas.client.lock.LockToken;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.lock.LockWriteResult;
import io.github.green4j.discas.client.lock.LockWriteStatus;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Chaos coverage for {@link Lock}: multi-client contention against
 * a TCP+FileWal cluster while nodes are restarted at random.
 *
 * <p>The at-most-one-holder invariant is enforced via {@link ChaosLockMonitor}:
 * every successful acquire CAS-claims an in-process tracker, every release
 * CAS-clears it. If two clients ever both believe they hold the same lock at
 * the same wall-clock moment, the tracker rejects the second acquire and
 * records a violation.
 *
 * <p>The chaos run also asserts:
 * <ul>
 *   <li>Workers progress (each worker successfully acquires at least once on
 *       any non-trivial run, except possibly under heavy LONG chaos).</li>
 *   <li>Most workload errors are transient (Node closing / timed out / shutdown
 *       categories from {@link ChaosAssertions}).</li>
 * </ul>
 */
@Tag("chaos")
@DisplayName("Distributed lock chaos -- multi-client contention under nemesis")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class LockChaosTest {
    private static final TcpTransportConfig DEFAULT_PEER_TRANSPORT_CONFIG = TcpTransportConfig.defaults();

    private static final ClientTransportConfig DEFAULT_CLIENT_TRANSPORT_CONFIG = ClientTransportConfig.defaults();

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("At-most-one-holder invariant holds under concurrent contention + random restarts")
    void atMostOneHolderUnderNemesis() throws Exception {
        assumeTrue(TestProfile.current().atLeast(TestProfile.STANDARD),
                "chaos: standard profile or above");
        final TestProfile profile = TestProfile.current();

        final Duration totalDuration = profile.scale(Duration.ofSeconds(15));
        final int workerThreads = 6;
        final int clientCount = 4;
        final int lockKeyCount = 3;
        final Duration leaseTtl = Duration.ofMillis(800);
        final Duration acquireWait = Duration.ofMillis(400);
        final Duration holdDuration = Duration.ofMillis(120);

        final long seed = 0x10CCAA5L;
        final List<String> lockKeys = new ArrayList<>();
        for (int i = 0; i < lockKeyCount; i++) {
            lockKeys.add("lock-" + i);
        }

        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                tempDir.resolve("lock-chaos"), DEFAULT_PEER_TRANSPORT_CONFIG, DEFAULT_CLIENT_TRANSPORT_CONFIG)) {
            harness.startCluster(clientCount);

            final ChaosLockMonitor monitor = new ChaosLockMonitor();
            final AtomicBoolean stop = new AtomicBoolean(false);
            final ChaosNemesis nemesis = new ChaosNemesis(harness, seed ^ 0xCAFEBABEL);
            final List<Throwable> workerErrors = new CopyOnWriteArrayList<>();

            final AtomicReference<ChaosNemesis.Result> nemesisResultRef = new AtomicReference<>();
            final Thread nemesisThread = new Thread(() -> {
                try {
                    nemesisResultRef.set(nemesis.run(
                            totalDuration,
                            Duration.ofMillis(500),
                            Duration.ofMillis(1500),
                            Duration.ofMillis(300),
                            Duration.ofMillis(800),
                            stop));
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "lock-chaos-nemesis");
            nemesisThread.setDaemon(true);
            nemesisThread.start();

            final CountDownLatch done = new CountDownLatch(workerThreads);
            final long deadlineNanos = System.nanoTime() + totalDuration.toNanos();

            for (int w = 0; w < workerThreads; w++) {
                final int workerIdx = w;
                final String ownerId = "worker-" + workerIdx;
                final Random rng = new Random(seed + workerIdx * 7919L);
                final Thread t = new Thread(() -> {
                    try {
                        while (!stop.get() && System.nanoTime() - deadlineNanos < 0) {
                            final DisCasClient client = harness.client(workerIdx % clientCount);
                            final String key = lockKeys.get(rng.nextInt(lockKeys.size()));
                            Lock lock = null;
                            try {
                                final LockAcquireResult res = client
                                        .lock(key, leaseTtl, acquireWait, ownerId)
                                        .get(acquireWait.toMillis() + 3000L, TimeUnit.MILLISECONDS);
                                if (res.status() != LockAcquireStatus.ACQUIRED) {
                                    continue;
                                }
                                lock = res.lock();
                                if (!monitor.recordAcquire(key, ownerId, lock.fencingToken())) {
                                    // Violation already recorded; release best-effort and continue.
                                    lock.release().orTimeout(2000, TimeUnit.MILLISECONDS)
                                            .exceptionally(x -> null).join();
                                    continue;
                                }
                                Thread.sleep(holdDuration.toMillis());
                            } catch (final TimeoutException
                                           | ExecutionException
                                           | InterruptedException e) {
                                workerErrors.add(e);
                                if (e instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            } catch (final Throwable e) {
                                workerErrors.add(e);
                            } finally {
                                if (lock != null) {
                                    monitor.recordRelease(key, ownerId);
                                    try {
                                        lock.release().orTimeout(3000, TimeUnit.MILLISECONDS)
                                                .exceptionally(x -> null).join();
                                    } catch (final Throwable ignored) {
                                    }
                                }
                            }
                        }
                    } finally {
                        done.countDown();
                    }
                }, "lock-chaos-worker-" + workerIdx);
                t.setDaemon(true);
                t.start();
            }

            done.await(totalDuration.toMillis() + 60_000L, TimeUnit.MILLISECONDS);
            stop.set(true);
            nemesisThread.join(120_000L);

            // No wall-clock "settle" needed: the lock-monitor invariant
            // (at-most-one holder) is captured at acquire/release time and
            // does not depend on quiescence.

            assertTrue(monitor.violations().isEmpty(),
                    "At-most-one-holder violations: " + monitor.violations());

            assertTrue(monitor.acquireCount() > 0,
                    "No locks were acquired across the chaos run; workload may be misconfigured");

            ChaosAssertions.assertOnlyTransientWorkloadErrors(workerErrors);

            final ChaosNemesis.Result nemesisResult = nemesisResultRef.get();
            if (nemesisResult != null) {
                ChaosAssertions.assertNoErrors(nemesisResult.errors, "Nemesis");
            }
        }
    }

    @Test
    @DisplayName("Lock survives full-cluster recycle; release still enforces ownership token")
    void lockSurvivesFullClusterRecycle() throws Exception {
        final TestProfile profile = TestProfile.current();
        assumeTrue(profile.atLeast(TestProfile.STANDARD),
                "Full-recycle durability scenario runs at STANDARD or LONG only");
        final Duration fullRecycleOutage = profile.scale(Duration.ofMillis(900));

        // Robust wall-clock walls -- the assertion here is about lease survival
        // across a full cluster recycle, not response latency, so the per-op
        // timeouts are generous enough to survive cold JVM start-up and
        // shared-runner scheduling pressure without ever hiding a bug.
        final Duration acquireTtl = Duration.ofSeconds(30);
        final Duration acquireWait = Duration.ofSeconds(20);
        final long getWaitSeconds = 30;

        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                tempDir.resolve("lock-durability"), DEFAULT_PEER_TRANSPORT_CONFIG, DEFAULT_CLIENT_TRANSPORT_CONFIG)) {
            harness.startCluster(2);

            final String key = "durable-lock";
            final DisCasClient client0 = harness.client(0);
            final DisCasClient client1 = harness.client(1);

            // Warm up the cluster: wait until every client can quiesce on
            // the key. Ensures TCP peer connections are established and
            // avoids first-op timeout flakes on cold-start JVMs.
            harness.awaitClusterQuiescent(List.of(key), ChaosAssertions.QUIESCENCE_WALL);

            // Acquire on client 0 with a TTL that easily survives the recycle.
            final LockAcquireResult acquired = client0.lock(
                            key, acquireTtl, acquireWait, "owner-A")
                    .get(getWaitSeconds, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.ACQUIRED, acquired.status());
            final Lock lockA = acquired.lock();

            harness.restartAllSequentially(fullRecycleOutage);
            // Await cluster quiescence on the lock key rather than sleeping --
            // guarantees the lease state has propagated to every client.
            harness.awaitClusterQuiescent(List.of(key), ChaosAssertions.QUIESCENCE_WALL);

            // After full restart, client 1 must still see the lock as held (lease valid).
            final LockInfoResult info = client1.getLockInfo(key).get(getWaitSeconds, TimeUnit.SECONDS);
            assertEquals(LockInfoStatus.LOCKED, info.status());

            // Client 1 with the wrong token cannot release.
            final LockWriteResult wrongReleased = client1.release(
                            key,
                            new LockToken(new byte[16]))
                    .get(getWaitSeconds, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.HELD_BY_OTHER, wrongReleased.status(),
                    "Release with wrong token must fail");

            // Client 0 with the correct token can.
            final LockWriteResult released = lockA.release().get(getWaitSeconds, TimeUnit.SECONDS);
            assertEquals(LockWriteStatus.APPLIED, released.status(),
                    "Release with correct token must succeed");

            // Once released, client 1 can acquire -- and the new fencing
            // token must be strictly greater than client 0's.
            final LockAcquireResult reacquired = client1.lock(
                            key, Duration.ofSeconds(5), acquireWait, "owner-B")
                    .get(getWaitSeconds, TimeUnit.SECONDS);
            assertEquals(LockAcquireStatus.ACQUIRED, reacquired.status());
            assertTrue(reacquired.lock().fencingToken() > lockA.fencingToken(),
                    "Fencing token must increase after re-acquire; was "
                            + lockA.fencingToken() + ", now " + reacquired.lock().fencingToken());
            reacquired.lock().release().get(getWaitSeconds, TimeUnit.SECONDS);
        }
    }

}
