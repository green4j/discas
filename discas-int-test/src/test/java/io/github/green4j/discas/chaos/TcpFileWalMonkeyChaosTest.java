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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag("chaos")
@DisplayName("TCP + FileWal cluster -- random monkey chaos")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class TcpFileWalMonkeyChaosTest {
    private static final TcpTransportConfig DEFAULT_PEER_TRANSPORT_CONFIG = TcpTransportConfig.defaults();

    private static final ClientTransportConfig DEFAULT_CLIENT_TRANSPORT_CONFIG = ClientTransportConfig.defaults();

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Smoke: concurrent clients + random node restarts converge and preserve counters")
    void smokeRandomRestarts() throws Exception {
        assumeTrue(TestProfile.current().atLeast(TestProfile.STANDARD),
                "Smoke scenario assumes >=12 s runtime; QUICK profile uses lighter chaos tests instead");
        runScenario(smokeConfig());
    }

    @Test
    @DisplayName("Longer run with periodic full-cluster recycle preserves durability")
    void longerWithFullClusterRecycle() throws Exception {
        assumeTrue(TestProfile.current().atLeast(TestProfile.STANDARD),
                "Longer durability scenario assumes >=30 s runtime");
        runScenario(longerConfig());
    }

    @Test
    @DisplayName("Extra-long under max-timeout pressure recovers and converges (LONG profile only)")
    void extraLongMaxTimeoutPressure() throws Exception {
        assumeTrue(TestProfile.current().atLeast(TestProfile.LONG),
                "Set -Ddiscas.test.profile=long to run extra-long chaos test");
        runScenario(extraLongConfig());
    }

    private void runScenario(final ScenarioConfig config) throws Exception {
        final Path scenarioRoot = tempDir.resolve(config.name);

        final List<String> counterKeys = keys("counter-", 16);
        final List<String> regularKeys = keys("key-", 64);
        final List<String> allKeys = new ArrayList<>(counterKeys);
        allKeys.addAll(regularKeys);

        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                scenarioRoot, config.peerTransportConfig, config.clientTransportConfig)) {
            harness.startCluster(config.clientCount);

            final AtomicBoolean stopSignal = new AtomicBoolean(false);
            final ChaosWorkload workload = new ChaosWorkload(
                    harness, counterKeys, regularKeys, config.workerThreads, config.seed,
                    config.opAwaitTimeout, config.scanAwaitTimeout, config.casAttempts);
            final ChaosNemesis nemesis = new ChaosNemesis(harness, config.seed ^ 0xDEADBEEFL);

            final AtomicReference<ChaosNemesis.Result> nemesisResultRef = new AtomicReference<>();
            final Thread nemesisThread = new Thread(() -> {
                try {
                    final ChaosNemesis.Result r = nemesis.run(
                            config.duration,
                            config.restartIntervalMin,
                            config.restartIntervalMax,
                            config.outageMin,
                            config.outageMax,
                            stopSignal);
                    nemesisResultRef.set(r);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    final ChaosNemesis.Result interrupted = new ChaosNemesis.Result();
                    interrupted.errors.add(e);
                    nemesisResultRef.set(interrupted);
                }
            }, "chaos-nemesis-driver");
            nemesisThread.setDaemon(true);
            nemesisThread.start();

            final ChaosWorkload.Result workloadResult = workload.run(config.duration, stopSignal);
            stopSignal.set(true);
            nemesisThread.join(config.duration.toMillis() + 120_000L);
            final ChaosNemesis.Result nemesisResult = nemesisResultRef.get() == null
                    ? new ChaosNemesis.Result() : nemesisResultRef.get();

            ChaosAssertions.assertOnlyTransientWorkloadErrors(workloadResult.errors);
            ChaosAssertions.assertNoErrors(nemesisResult.errors, "Nemesis");
            ChaosAssertions.assertConvergedAcrossClients(harness, allKeys);
            ChaosAssertions.assertCounterExactness(
                    harness,
                    workloadResult.successfulCounterSwaps,
                    config.counterTolerance);

            if (config.includeFullRecycle) {
                ChaosAssertions.assertDurabilityAfterFullRestart(
                        harness, allKeys, config.fullRecycleOutage);
            }
        }
    }

    private static List<String> keys(final String prefix, final int count) {
        final List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(prefix + i);
        }
        return keys;
    }

    private ScenarioConfig smokeConfig() {
        final TestProfile profile = TestProfile.current();
        return new ScenarioConfig(
                "smoke",
                0xC0FFEE01L,
                profile.scale(Duration.ofSeconds(12)),
                Duration.ofMillis(250),
                Duration.ofMillis(700),
                Duration.ofMillis(150),
                Duration.ofMillis(500),
                6,
                8,
                false,
                Duration.ofSeconds(5),
                Duration.ofSeconds(8),
                12,
                8L,
                Duration.ofMillis(800),
                DEFAULT_PEER_TRANSPORT_CONFIG,
                DEFAULT_CLIENT_TRANSPORT_CONFIG
        );
    }

    private ScenarioConfig longerConfig() {
        final TestProfile profile = TestProfile.current();
        return new ScenarioConfig(
                "longer",
                0xC0FFEE02L,
                profile.scale(Duration.ofSeconds(30)),
                Duration.ofMillis(350),
                Duration.ofMillis(1200),
                Duration.ofMillis(250),
                Duration.ofMillis(900),
                8,
                10,
                true,
                Duration.ofSeconds(6),
                Duration.ofSeconds(10),
                14,
                10L,
                Duration.ofMillis(900),
                DEFAULT_PEER_TRANSPORT_CONFIG,
                DEFAULT_CLIENT_TRANSPORT_CONFIG
        );
    }

    private ScenarioConfig extraLongConfig() {
        return new ScenarioConfig(
                "extra-long",
                0xC0FFEE03L,
                Duration.ofSeconds(240),
                Duration.ofMillis(600),
                Duration.ofMillis(1800),
                Duration.ofMillis(700),
                Duration.ofMillis(3200),
                12,
                14,
                true,
                Duration.ofSeconds(10),
                Duration.ofSeconds(15),
                20,
                15L,
                Duration.ofMillis(1200),
                DEFAULT_PEER_TRANSPORT_CONFIG,
                DEFAULT_CLIENT_TRANSPORT_CONFIG
        );
    }

    private static final class ScenarioConfig {
        final String name;
        final long seed;
        final Duration duration;
        final Duration restartIntervalMin;
        final Duration restartIntervalMax;
        final Duration outageMin;
        final Duration outageMax;
        final int clientCount;
        final int workerThreads;
        final boolean includeFullRecycle;
        final Duration opAwaitTimeout;
        final Duration scanAwaitTimeout;
        final int casAttempts;
        final long counterTolerance;
        final Duration fullRecycleOutage;
        final TcpTransportConfig peerTransportConfig;
        final ClientTransportConfig clientTransportConfig;

        ScenarioConfig(final String name,
                       final long seed,
                       final Duration duration,
                       final Duration restartIntervalMin,
                       final Duration restartIntervalMax,
                       final Duration outageMin,
                       final Duration outageMax,
                       final int clientCount,
                       final int workerThreads,
                       final boolean includeFullRecycle,
                       final Duration opAwaitTimeout,
                       final Duration scanAwaitTimeout,
                       final int casAttempts,
                       final long counterTolerance,
                       final Duration fullRecycleOutage,
                       final TcpTransportConfig peerTransportConfig,
                       final ClientTransportConfig clientTransportConfig) {
            this.name = name;
            this.seed = seed;
            this.duration = duration;
            this.restartIntervalMin = restartIntervalMin;
            this.restartIntervalMax = restartIntervalMax;
            this.outageMin = outageMin;
            this.outageMax = outageMax;
            this.clientCount = clientCount;
            this.workerThreads = workerThreads;
            this.includeFullRecycle = includeFullRecycle;
            this.opAwaitTimeout = opAwaitTimeout;
            this.scanAwaitTimeout = scanAwaitTimeout;
            this.casAttempts = casAttempts;
            this.counterTolerance = counterTolerance;
            this.fullRecycleOutage = fullRecycleOutage;
            this.peerTransportConfig = peerTransportConfig;
            this.clientTransportConfig = clientTransportConfig;
        }
    }
}
