/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.performance;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.common.client.ReadConsistency;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Why does an operation cost one link round trip when prepare-then-accept implies two?
 *
 * <p>The envelope measures 2x the one-way delay per write at both 25ms and 50ms per link, while
 * {@code Proposer} completes a round only once {@code acceptOkCount >= quorumSize}, which for N=3
 * needs one network reply after the coordinator's own vote -- and the same again for prepare. Either
 * a phase is cheaper than the code reads, or the harness is not delaying what it appears to.
 *
 * <p>So this counts rather than times. Every delaying link tallies the frames it carries, so the
 * crossings per operation say directly how many times the mesh spoke:
 * <ul>
 *   <li><b>8 per write</b> (2 peers x 2 phases x there-and-back) means both phases go over the wire
 *       and the timing measurement is the thing to distrust;</li>
 *   <li><b>4 per write</b> means exactly one phase crosses, and the question becomes which, and
 *       why the other does not.</li>
 * </ul>
 *
 * <p>Three workloads, because they separate the plausible causes: repeated writes to one key (where
 * a coordinator might reuse something it already holds), writes to a key never seen before (where it
 * cannot), and a linearizable read (which has a documented short-circuit after prepare and so is the
 * control case).
 *
 * <pre>{@code ./gradlew :discas-performance:roundTripProbe}</pre>
 */
public final class RoundTripProbe {

    private static final long DELAY_MS = 25L;
    private static final int WARMUP = 20;
    private static final int OPS = 100;

    private RoundTripProbe() {
    }

    public static void main(final String[] args) throws Exception {
        final Path root = Files.createTempDirectory("discas-rtt-probe-");
        try (BenchCluster cluster = new BenchCluster(3, 1, DELAY_MS, root)) {
            final DisCasClient client = cluster.client(0);

            probe(cluster, "write, same key repeatedly", () -> {
                client.put(utf8("probe/same"), utf8("v")).get(30, TimeUnit.SECONDS);
                return null;
            });

            final int[] fresh = {0};
            probe(cluster, "write, a key never written before", () -> {
                client.put(utf8("probe/fresh-" + fresh[0]++), utf8("v")).get(30, TimeUnit.SECONDS);
                return null;
            });

            probe(cluster, "read linearizable, same key", () -> {
                client.get(utf8("probe/same"), ReadConsistency.LINEARIZABLE)
                        .get(30, TimeUnit.SECONDS);
                return null;
            });
        }
    }

    @FunctionalInterface
    private interface Op {
        Object run() throws Exception;
    }

    /** Run {@code op} {@link #OPS} times and report crossings and latency per operation. */
    private static void probe(final BenchCluster cluster, final String what, final Op op)
            throws Exception {
        for (int i = 0; i < WARMUP; i++) {
            op.run();
        }
        final long crossingsBefore = cluster.linkCrossings();
        final long[] nanos = new long[OPS];
        for (int i = 0; i < OPS; i++) {
            final long start = System.nanoTime();
            op.run();
            nanos[i] = System.nanoTime() - start;
        }
        final long crossings = cluster.linkCrossings() - crossingsBefore;
        Arrays.sort(nanos);
        System.out.printf("%-38s %5.2f crossings/op   p50 %7.2fms   (~%.1f link round trips)%n",
                what, crossings / (double) OPS, nanos[OPS / 2] / 1e6,
                nanos[OPS / 2] / 1e6 / (2.0 * DELAY_MS));
    }

    private static ByteBuffer utf8(final String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }
}
