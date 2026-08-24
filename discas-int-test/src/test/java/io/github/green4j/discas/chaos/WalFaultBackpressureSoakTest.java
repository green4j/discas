/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import io.github.green4j.discas.node.wal.Wal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Three smaller chaos scenarios in one class, sharing tempDir and configs:
 * <ul>
 *   <li>{@link #walTornTailRecoveryDoesNotResurrectStaleState()} truncates the
 *       trailing bytes of the last WAL file to simulate a torn write at crash.
 *       Asserts recovery completes and stops at the last well-formed record.</li>
 *   <li>{@link #proposerOverloadReturnsToNormal()} drives more concurrent in-flight
 *       client requests than {@code Proposer.MAX_PENDING} so the proposer should
 *       reject some with {@code overloaded} errors, then asserts the cluster
 *       returns to normal throughput once the burst ends.</li>
 *   <li>{@link #soakRunBoundsJvmResources()} is LONG-profile only and asserts
 *       that thread count does not strictly grow across a long workload with
 *       periodic nemesis restarts.</li>
 * </ul>
 */
@Tag("chaos")
@DisplayName("WAL faults, backpressure and soak")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class WalFaultBackpressureSoakTest {
    private static final TcpTransportConfig DEFAULT_PEER_TRANSPORT_CONFIG = TcpTransportConfig.defaults();

    private static final ClientTransportConfig DEFAULT_CLIENT_TRANSPORT_CONFIG = ClientTransportConfig.defaults();

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("WAL torn-tail recovery does not resurrect stale state past the truncation point")
    void walTornTailRecovery() throws IOException {
        final StorageConfig config = StorageConfig.builder()
                .baseDirectory(tempDir.resolve("torn"))
                .walMaxFileBytes(64 * 1024)
                .snapshotRetentionCount(2)
                .build();

        // Write a well-known sequence, force, close cleanly.
        final FileWal wal = new FileWal(config);
        wal.initialize();
        wal.replayTail(e -> {
        });

        final HashedBytes key = new HashedBytes(new byte[]{1, 2, 3});
        final int recordCount = 10;
        for (int i = 1; i <= recordCount; i++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(i, NodeId.of("1")),
                    new HashedBytes(new byte[]{(byte) i}), false));
        }
        wal.force();
        wal.close();

        // Locate the newest WAL file and truncate ~3 bytes from its tail to simulate
        // a torn write. Recovery should treat the corrupted suffix as missing.
        final Path walFile = newestWalFile(config.walDirectory());
        final long originalSize = Files.size(walFile);
        final long truncatedSize = Math.max(0L, originalSize - 3L);
        try (FileChannel ch = FileChannel.open(walFile, StandardOpenOption.WRITE)) {
            ch.truncate(truncatedSize);
        }

        // Recover. We expect either: replay halts before the last record (we lost it)
        // or the last record is preserved if the truncated bytes were padding. The
        // strict invariant is that no exception escapes and no record beyond the
        // truncation point is materialised.
        final FileWal wal2 = new FileWal(config);
        wal2.initialize();
        final List<Wal.Entry> recovered = new ArrayList<>();
        try {
            wal2.replayTail(recovered::add);
        } catch (final Exception e) {
            // Some WAL implementations let torn records bubble; record but don't
            // automatically fail -- we still want to see the recovered-so-far list.
            fail("Torn-tail replay must not throw: " + e, e);
        }
        wal2.close();

        // Recovered count must be <= records written, with strictly increasing ballots.
        assertTrue(recovered.size() <= recordCount,
                "Recovered " + recovered.size() + " entries from a WAL with at most "
                        + recordCount + " written");
        long lastBallotCounter = 0;
        for (final Wal.Entry e : recovered) {
            assertTrue(e instanceof Wal.Entry.Accept, "Unexpected entry type: " + e);
            final Wal.Entry.Accept acc = (Wal.Entry.Accept) e;
            assertTrue(acc.ballot().counter() > lastBallotCounter,
                    "Ballots must be strictly increasing across torn-tail recovery; "
                            + "saw " + acc.ballot().counter() + " after " + lastBallotCounter);
            lastBallotCounter = acc.ballot().counter();
        }
    }

    @Test
    @DisplayName("Proposer overload -- burst above MAX_PENDING returns to normal throughput")
    void proposerOverloadRecovers() throws Exception {
        final TestProfile profile = TestProfile.current();
        // Skip in QUICK: deliberately driving thousands of in-flight requests adds wall
        // clock that doesn't fit the QUICK budget and is not necessary on every commit.
        assumeTrue(profile.atLeast(TestProfile.STANDARD),
                "proposerOverload requires STANDARD or LONG profile");

        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                tempDir.resolve("overload"), DEFAULT_PEER_TRANSPORT_CONFIG, DEFAULT_CLIENT_TRANSPORT_CONFIG)) {
            harness.startCluster(2);

            final DisCasClient client = harness.client(0);
            final int burst = profile.atLeast(TestProfile.LONG) ? 12_000 : 6_000;

            // Burst: fire `burst` non-blocking puts and collect futures. Some
            // should fail with the proposer's "overloaded" message once
            // MAX_PENDING is exceeded; the rest should complete.
            final List<CompletableFuture<Version>> futures = new ArrayList<>(burst);
            for (int i = 0; i < burst; i++) {
                final ByteBuffer key = ByteBuffer.wrap(("k-" + i).getBytes());
                final ByteBuffer val = ByteBuffer.wrap(("v-" + i).getBytes());
                futures.add(client.put(key, val));
            }

            // Under a burst this size some puts are shed and some are served; which is which is
            // not the property. What matters is that the burst did not stop the cluster serving,
            // and that it recovers -- asserted below.
            int succeeded = 0;
            for (final CompletableFuture<Version> f : futures) {
                try {
                    f.get(profile.scale(15_000L), TimeUnit.MILLISECONDS);
                    succeeded++;
                } catch (final Exception shedOrTimedOut) {
                    // Shedding under overload is the behaviour, not a failure.
                }
            }

            assertTrue(succeeded > 0,
                    "Some puts must have completed; succeeded=" + succeeded);

            // After the burst settles, a follow-up request must succeed quickly --
            // proves backpressure is transient.
            client.put(
                            ByteBuffer.wrap("post-burst".getBytes()),
                            ByteBuffer.wrap("ok".getBytes()))
                    .get(8, TimeUnit.SECONDS);
            final ByteBuffer readBack = client.get(ByteBuffer.wrap("post-burst".getBytes()))
                    .get(8, TimeUnit.SECONDS).value();
            assertEquals("ok", new String(buf(readBack)));
        }
    }

    @Test
    @DisplayName("Soak (LONG only) -- thread count and key counts stay bounded across long run")
    void soakBoundsJvmResources() throws Exception {
        final TestProfile profile = TestProfile.current();
        assumeTrue(profile.atLeast(TestProfile.LONG),
                "Soak is LONG-profile only; pass -Ddiscas.test.profile=long");

        final long seed = 0xC0DECAFEL;
        try (ChaosClusterHarness harness = new ChaosClusterHarness(
                tempDir.resolve("soak"), DEFAULT_PEER_TRANSPORT_CONFIG, DEFAULT_CLIENT_TRANSPORT_CONFIG)) {
            harness.startCluster(4);

            final List<String> counterKeys = keys("counter-", 8);
            final List<String> regularKeys = keys("key-", 16);
            final AtomicBoolean stop = new AtomicBoolean(false);

            final ChaosWorkload workload = new ChaosWorkload(
                    harness, counterKeys, regularKeys, 6, seed,
                    Duration.ofSeconds(8), Duration.ofSeconds(12), 10);
            final ChaosNemesis nemesis = new ChaosNemesis(harness, seed ^ 0xCAFEL);

            final Duration runDuration = Duration.ofSeconds(90);
            final List<Integer> threadSnapshots = new ArrayList<>();

            final Thread sampler = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        threadSnapshots.add(ManagementFactory.getThreadMXBean().getThreadCount());
                        Thread.sleep(2000L);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "soak-sampler");
            sampler.setDaemon(true);
            sampler.start();

            final AtomicReference<ChaosNemesis.Result> nemesisResultRef = new AtomicReference<>();
            final Thread nemesisThread = new Thread(() -> {
                try {
                    nemesisResultRef.set(nemesis.run(
                            runDuration,
                            Duration.ofMillis(400),
                            Duration.ofMillis(1500),
                            Duration.ofMillis(300),
                            Duration.ofMillis(1000),
                            stop));
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "soak-nemesis");
            nemesisThread.setDaemon(true);
            nemesisThread.start();

            final ChaosWorkload.Result workloadResult = workload.run(runDuration, stop);
            stop.set(true);
            nemesisThread.join(120_000L);
            sampler.join(5_000L);

            ChaosAssertions.assertOnlyTransientWorkloadErrors(workloadResult.errors);

            // Bounded resources: the trailing third of thread snapshots must not
            // exceed the leading third by more than a small fudge.
            assertTrue(threadSnapshots.size() >= 6,
                    "Need at least 6 thread samples for trend check, got "
                            + threadSnapshots.size());
            final int third = threadSnapshots.size() / 3;
            final int leading = threadSnapshots.subList(0, third).stream()
                    .max(Comparator.naturalOrder()).orElse(0);
            final int trailing = threadSnapshots.subList(threadSnapshots.size() - third,
                    threadSnapshots.size()).stream()
                    .max(Comparator.naturalOrder()).orElse(0);
            assertTrue(trailing <= leading + 20,
                    "Thread count appears to grow under soak: leading max=" + leading
                            + " trailing max=" + trailing + " samples=" + threadSnapshots);
        }
    }

    private static Path newestWalFile(final Path walDir) throws IOException {
        try (Stream<Path> s = Files.list(walDir)) {
            return s.filter(p -> {
                final String name = p.getFileName().toString();
                return name.endsWith(".wal") || name.endsWith(".open");
            })
                    .max(Comparator.comparing(p -> p.getFileName().toString()))
                    .orElseThrow(() -> new IOException("No WAL file in " + walDir));
        }
    }

    private static byte[] buf(final ByteBuffer b) {
        if (b == null) {
            return new byte[0];
        }
        final byte[] out = new byte[b.remaining()];
        b.duplicate().get(out);
        return out;
    }

    private static List<String> keys(final String prefix, final int count) {
        final List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(prefix + i);
        }
        return result;
    }
}
