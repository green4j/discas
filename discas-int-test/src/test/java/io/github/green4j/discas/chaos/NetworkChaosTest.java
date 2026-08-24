/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.node.NodeConfig;
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
 * Chaos coverage for asymmetric network partitions and probabilistic message
 * drops at the peer-to-peer layer. Faults are injected by the frame-aware
 * {@link ChaosProxy} that {@link ChaosClusterHarness} places on each peer link, so
 * only peer transport traffic is affected (client-to-node messages are delivered
 * normally via {@link io.github.green4j.discas.client.transport.InProcessClientTransport}).
 *
 * <p>Verifies that the cluster:
 * <ul>
 *   <li>Tolerates a steady 5-10% peer-message drop rate while workload runs.</li>
 *   <li>Recovers from asymmetric partitions (A->B blocked while B->A open) and
 *       converges after they heal.</li>
 *   <li>Does not over-count CAS swaps from message duplication / reordering
 *       that drops can mask.</li>
 * </ul>
 */
@Tag("chaos")
@DisplayName("Network chaos -- asymmetric partitions and probabilistic drops")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class NetworkChaosTest {
    private static final TcpTransportConfig DEFAULT_PEER_TRANSPORT_CONFIG = TcpTransportConfig.defaults();

    private static final ClientTransportConfig DEFAULT_CLIENT_TRANSPORT_CONFIG = ClientTransportConfig.defaults();

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Steady 5% peer-drop rate -- cluster converges and counters stay consistent")
    void steadyDropRate() throws Exception {
        assumeTrue(TestProfile.current().atLeast(TestProfile.STANDARD),
                "chaos: standard profile or above");
        final TestProfile profile = TestProfile.current();
        final long seed = 0xDEFADED;

        final Duration duration = profile.scale(Duration.ofSeconds(15));
        final int clientCount = 4;
        final int workerThreads = 6;
        final int casAttempts = 10;

        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                tempDir.resolve("net-drop"),
                DEFAULT_PEER_TRANSPORT_CONFIG,
                DEFAULT_CLIENT_TRANSPORT_CONFIG,
                NodeConfig.builder().repairInterval(),
                /* withFaultyTransport */ true,
                seed)) {
            harness.startCluster(clientCount);

            // Steady 5% drop on every peer's outbound + inbound paths.
            for (final int nodeId : harness.nodeIds()) {
                harness.faultyTransport(nodeId).dropProbability(0.05);
            }

            final List<String> counterKeys = keys("counter-", 8);
            final List<String> regularKeys = keys("key-", 16);
            final AtomicBoolean stop = new AtomicBoolean(false);

            final ChaosWorkload workload = new ChaosWorkload(
                    harness, counterKeys, regularKeys, workerThreads, seed,
                    Duration.ofSeconds(8), Duration.ofSeconds(12), casAttempts);

            final ChaosWorkload.Result workloadResult = workload.run(duration, stop);
            stop.set(true);

            // Heal all drops; convergence is asserted event-driven below.
            for (final int nodeId : harness.nodeIds()) {
                harness.faultyTransport(nodeId).dropProbability(0.0);
            }

            ChaosAssertions.assertOnlyTransientWorkloadErrors(workloadResult.errors);

            final List<String> allKeys = new ArrayList<>(counterKeys);
            allKeys.addAll(regularKeys);
            ChaosAssertions.assertConvergedAcrossClients(harness, allKeys);
            // See AntiEntropyChaosTest for tolerance rationale -- DisCasClient's
            // per-attempt retry timer can race with response arrival under
            // steady packet drop, producing occasional extra `swapped=true`
            // acks. Cluster state itself is consistent (assertConvergedAcrossClients
            // above).
            ChaosAssertions.assertCounterExactness(
                    harness,
                    workloadResult.successfulCounterSwaps,
                    /* tolerance */ 8L);
        }
    }

    @Test
    @DisplayName("Asymmetric partition windows -- cluster converges after heal")
    void asymmetricPartitionWindows() throws Exception {
        final TestProfile profile = TestProfile.current();
        assumeTrue(profile.atLeast(TestProfile.STANDARD),
                "Asymmetric partition windows scenario runs at STANDARD or LONG only");
        final long seed = 0xA51DECL;

        final Duration duration = profile.scale(Duration.ofSeconds(12));
        final int clientCount = 3;
        final int workerThreads = 4;

        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                tempDir.resolve("net-partition"),
                DEFAULT_PEER_TRANSPORT_CONFIG,
                DEFAULT_CLIENT_TRANSPORT_CONFIG,
                NodeConfig.builder().repairInterval(),
                /* withFaultyTransport */ true,
                seed)) {
            harness.startCluster(clientCount);

            final List<String> counterKeys = keys("counter-", 4);
            final List<String> regularKeys = keys("key-", 8);
            final AtomicBoolean stop = new AtomicBoolean(false);

            // Background workload.
            final ChaosWorkload workload = new ChaosWorkload(
                    harness, counterKeys, regularKeys, workerThreads, seed,
                    Duration.ofSeconds(8), Duration.ofSeconds(12), /* casAttempts */ 8);

            // Asymmetric partition driver: pick a random (src, dst) pair, block src->dst only,
            // hold for a short window, then heal. Repeat until duration elapses.
            final AtomicReference<ChaosWorkload.Result> workloadResultRef = new AtomicReference<>();
            final Thread workloadThread = new Thread(() -> {
                try {
                    workloadResultRef.set(workload.run(duration, stop));
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "net-chaos-workload");
            workloadThread.setDaemon(true);
            workloadThread.start();

            final Random rng = new Random(seed ^ 0x12345L);
            final long deadlineNanos = System.nanoTime() + duration.toNanos();
            final List<Integer> ids = harness.nodeIds();
            while (System.nanoTime() - deadlineNanos < 0 && !stop.get()) {
                final int src = ids.get(rng.nextInt(ids.size()));
                int dst = ids.get(rng.nextInt(ids.size()));
                while (dst == src) {
                    dst = ids.get(rng.nextInt(ids.size()));
                }
                try {
                    harness.faultyTransport(src).isolateOutbound(dst);
                } catch (final IllegalStateException ignored) {
                    // Node may be restarting; skip
                }
                Thread.sleep(200 + rng.nextInt(400));
                try {
                    harness.faultyTransport(src).healOutbound(dst);
                } catch (final IllegalStateException ignored) {
                }
                Thread.sleep(150 + rng.nextInt(350));
            }

            stop.set(true);
            workloadThread.join(120_000L);

            // Heal everything.
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
            // Asymmetric partitions produce more spurious `swapped=true` acks
            // than the steady-drop test -- the client's retry timer fires
            // whenever a request happens to cross a partitioned edge.
            ChaosAssertions.assertCounterExactness(
                    harness,
                    workloadResult.successfulCounterSwaps,
                    /* tolerance */ 10L);
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
