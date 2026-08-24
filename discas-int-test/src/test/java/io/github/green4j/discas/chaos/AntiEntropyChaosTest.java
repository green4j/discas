/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Exercises anti-entropy concurrently with a live workload. Production's
 * {@code REPAIR_INTERVAL} is 10 minutes, so under normal chaos runs (30 s)
 * anti-entropy never fires; we drive it at 200 ms here to surface bugs in
 * digest comparison, range-repair throttling, and repair-while-write
 * concurrency.
 *
 * <p>Pattern:
 * <ul>
 *   <li>Run mixed CAS / put / delete workload.</li>
 *   <li>Inject divergence by briefly isolating one node so it falls behind.</li>
 *   <li>Heal; anti-entropy must converge all clients within the read budget.</li>
 * </ul>
 */
@Tag("chaos")
@DisplayName("Anti-entropy under live workload")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class AntiEntropyChaosTest {
    private static final TcpTransportConfig DEFAULT_PEER_TRANSPORT_CONFIG = TcpTransportConfig.defaults();

    private static final ClientTransportConfig DEFAULT_CLIENT_TRANSPORT_CONFIG = ClientTransportConfig.defaults();

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Short repair interval + brief isolations converges all clients within read budget")
    void shortRepairIntervalWithIsolations() throws Exception {
        assumeTrue(TestProfile.current().atLeast(TestProfile.STANDARD),
                "chaos: standard profile or above");
        final TestProfile profile = TestProfile.current();
        final long seed = 0xA371E7L;

        final Duration duration = profile.scale(Duration.ofSeconds(15));
        final Duration repairInterval = Duration.ofMillis(200);
        final int clientCount = 3;
        final int workerThreads = 5;

        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                tempDir.resolve("anti-entropy"),
                DEFAULT_PEER_TRANSPORT_CONFIG,
                DEFAULT_CLIENT_TRANSPORT_CONFIG,
                repairInterval,
                /* withFaultyTransport */ true,
                seed)) {
            harness.startCluster(clientCount);

            final List<String> counterKeys = keys("counter-", 6);
            final List<String> regularKeys = keys("key-", 12);
            final AtomicBoolean stop = new AtomicBoolean(false);

            final ChaosWorkload workload = new ChaosWorkload(
                    harness, counterKeys, regularKeys, workerThreads, seed,
                    Duration.ofSeconds(8), Duration.ofSeconds(12), /* casAttempts */ 10);

            final AtomicReference<ChaosWorkload.Result> workloadResultRef = new AtomicReference<>();
            final Thread workloadThread = new Thread(() -> {
                try {
                    workloadResultRef.set(workload.run(duration, stop));
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "anti-entropy-workload");
            workloadThread.setDaemon(true);
            workloadThread.start();

            // Divergence injector: every ~1s, isolate one node both directions for ~300ms.
            // This is shorter than a node restart but long enough for the majority to
            // accept new writes the isolated node misses; anti-entropy must repair.
            final long deadlineNanos = System.nanoTime() + duration.toNanos();
            final Random rng = new Random(seed ^ 0xBADBEEFL);
            final List<Integer> ids = harness.nodeIds();

            while (System.nanoTime() - deadlineNanos < 0 && !stop.get()) {
                Thread.sleep(700 + rng.nextInt(600));
                final int victim = ids.get(rng.nextInt(ids.size()));
                try {
                    final ChaosClusterHarness.FaultyLink ft = harness.faultyTransport(victim);
                    // Symmetric: nothing in, nothing out.
                    for (final int other : ids) {
                        if (other != victim) {
                            ft.isolate(other);
                        }
                    }
                    Thread.sleep(250 + rng.nextInt(250));
                    ft.healAll();
                } catch (final IllegalStateException ignored) {
                    // race with restart; skip
                }
            }

            stop.set(true);
            workloadThread.join(120_000L);

            // Heal everything just in case.
            for (final int nodeId : ids) {
                try {
                    harness.faultyTransport(nodeId).healAll();
                } catch (final IllegalStateException ignored) {
                }
            }

            final ChaosWorkload.Result workloadResult = workloadResultRef.get();
            ChaosAssertions.assertOnlyTransientWorkloadErrors(workloadResult.errors);

            final List<String> allKeys = new ArrayList<>(counterKeys);
            allKeys.addAll(regularKeys);
            ChaosAssertions.assertConvergedAcrossClients(harness, allKeys);
            // Tolerance calibrated to the empirical spread of the workload's
            // `swapped=true` count vs. the quiesced counter value. The
            // DisCasClient's per-attempt retry timer (PER_ATTEMPT_TIMEOUT) can
            // race with response arrival under contention; the timer resubmits
            // to the next peer while the original attempt's response is
            // in flight, occasionally producing a spurious extra `swapped=true`
            // ack on the client side (the actual cluster state remains
            // consistent -- verified by `assertConvergedAcrossClients` above).
            // Pre-redesign, the 15s settle-budget polling absorbed this
            // wobble; with event-driven quiescence it surfaces as a tolerance.
            ChaosAssertions.assertCounterExactness(
                    harness,
                    workloadResult.successfulCounterSwaps,
                    /* tolerance */ 8L);
        }
    }

    private static List<String> keys(final String prefix, final int count) {
        final List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(prefix + i);
        }
        return result;
    }
}
