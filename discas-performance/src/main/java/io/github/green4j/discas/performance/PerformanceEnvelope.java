/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.performance;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.common.client.ReadConsistency;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The performance envelope: what this store costs, and under exactly what conditions.
 *
 * <pre>{@code ./gradlew :discas-performance:envelope}</pre>
 *
 * <h2>What is measured</h2>
 * <ol>
 *   <li><b>Ping RTT</b>, first, because everything else sits on top of it. Without it a reader
 *       cannot tell an expensive protocol from an expensive machine.</li>
 *   <li><b>Single-key write latency</b>, N=3, local and with an injected inter-region delay. A write
 *       is two round trips to a quorum, so the second figure should be about the first plus four
 *       crossings of the link -- saying so in advance is what lets the measurement disagree.</li>
 *   <li><b>Read latency, linearizable against serializable</b>, same conditions. A linearizable read
 *       is a full round; a serializable one is answered from the coordinator's committed state. The
 *       gap is what {@code ?stale} buys.</li>
 *   <li><b>Throughput on uncontended keys</b> at 1, 2, 4 and 8 concurrent clients, for N=3 and N=5.
 *       Cluster size is a first-order effect here and nowhere else: a round costs the coordinator
 *       {@code 2(N-1)} messages. The client sweep is what separates "the store is saturated" from
 *       "one client is waiting for a round trip" -- a single throughput number cannot tell those
 *       apart, and they call for opposite reactions.</li>
 *   <li><b>What same-key contention costs</b>, over the same sweep. The README names this as the
 *       workload discas handles least well; the duel is documented, its price was not. Reported as
 *       the ratio against the uncontended run with the same client count, because the interesting
 *       quantity is how badly adding clients backfires.</li>
 * </ol>
 *
 * <h2>Reading these numbers honestly</h2>
 * Every node and every client is in one JVM on one machine. That flatters absolute latency (there
 * is no real network) and penalises throughput (nodes compete for the cores the clients use). What
 * it reports faithfully is <em>shape</em>: the ratio between read levels, the cost of a delay on the
 * peer mesh, how throughput answers more clients, and what one hot key does to all of it.
 */
public final class PerformanceEnvelope {

    /** Operations timed per latency figure, after {@link #WARMUP_OPS} untimed ones. */
    private static final int MEASURED_OPS = 2_000;
    private static final int WARMUP_OPS = 300;

    /** One-way delay injected on every peer link for the inter-region figures. */
    private static final long INTER_REGION_DELAY_MS = 25L;

    /** The client sweep. Powers of two so saturation shows as a bend rather than a slope. */
    private static final int[] CLIENT_COUNTS = {1, 2, 4, 8};
    private static final int MAX_CLIENTS = CLIENT_COUNTS[CLIENT_COUNTS.length - 1];
    private static final int OPS_PER_LOAD_CLIENT = 400;

    private static final List<String> REPORT = new ArrayList<>();

    private PerformanceEnvelope() {
    }

    /**
     * @param args which sections to run: {@code latency}, {@code throughput}, {@code contention},
     *             or nothing for all three. Sections are separable because a full run is minutes
     *             long, and an investigation into one number should not have to pay for the others.
     */
    public static void main(final String[] args) throws Exception {
        final Path root = Files.createTempDirectory("discas-envelope-");
        final List<String> sections = args.length == 0
                ? Arrays.asList("latency", "throughput", "contention", "idle")
                : Arrays.asList(args);
        try {
            pingBaseline();
            if (sections.contains("latency")) {
                latency(root.resolve("latency"));
            }
            if (sections.contains("throughput")) {
                throughput(root.resolve("throughput"));
            }
            if (sections.contains("contention")) {
                contention(root.resolve("contention"));
            }
            if (sections.contains("idle")) {
                idleCost(root.resolve("idle"));
            }
        } finally {
            printReport();
        }
    }

    /** The floor under everything else: a bare TCP round trip on this machine. */
    private static void pingBaseline() throws Exception {
        record("ping RTT", "direct loopback, 1 byte", PingRtt.measure());
    }

    private static void latency(final Path root) throws Exception {
        for (final long delayMs : new long[] {0L, INTER_REGION_DELAY_MS}) {
            final String link = delayMs == 0 ? "local" : delayMs + "ms/link";
            try (BenchCluster cluster = new BenchCluster(3, 1, delayMs, root.resolve("d" + delayMs))) {
                final DisCasClient client = cluster.client(0);
                final ByteBuffer key = utf8("bench/latency");
                final int[] sequence = {0};

                // A changing value on every operation, which is what a write normally is. Writing
                // the same bytes repeatedly measures something else entirely: a quorum that confirms
                // the register already holds the desired value completes the round after prepare and
                // never runs the accept phase, so the operation costs one round trip instead of two.
                record("write", "N=3 " + link, time(() ->
                        client.put(key.duplicate(), utf8("v" + sequence[0]++))));
                record("write, value unchanged", "N=3 " + link, time(() ->
                        client.put(key.duplicate(), utf8("same"))));
                record("read linearizable", "N=3 " + link,
                        time(() -> client.get(key.duplicate(), ReadConsistency.LINEARIZABLE)));
                record("read serializable", "N=3 " + link,
                        time(() -> client.get(key.duplicate(), ReadConsistency.SERIALIZABLE)));
            }
        }
    }

    private static void throughput(final Path root) throws Exception {
        for (final int clusterSize : new int[] {3, 5}) {
            try (BenchCluster cluster = new BenchCluster(
                    clusterSize, MAX_CLIENTS, 0L, root.resolve("n" + clusterSize))) {
                for (final int clients : CLIENT_COUNTS) {
                    final double rate = drive(cluster, clients, false);
                    record("throughput uncontended",
                            "N=" + clusterSize + " local, " + clients + " client(s)",
                            String.format("%.0f writes/s", rate));
                }
            }
        }
    }

    private static void contention(final Path root) throws Exception {
        try (BenchCluster cluster = new BenchCluster(3, MAX_CLIENTS, 0L, root)) {
            for (final int clients : CLIENT_COUNTS) {
                final double distinct = drive(cluster, clients, false);
                final double shared = drive(cluster, clients, true);
                record("throughput one key", "N=3 local, " + clients + " client(s)",
                        String.format("%.0f writes/s  (%.1fx the %.0f/s on distinct keys)",
                                shared, shared / Math.max(1e-9, distinct), distinct));
            }
        }
    }

    /**
     * What a cluster costs while nothing is happening.
     * <p>
     * Belongs in an envelope for the same reason the latency does: the event loop polls and backs
     * off rather than blocking in a selector, so idleness has a price and it should be a published
     * number. Measured on the process as a whole -- three nodes and their clients -- against the
     * machine's total capacity.
     */
    private static void idleCost(final Path root) throws Exception {
        try (BenchCluster cluster = new BenchCluster(3, 1, 0L, root)) {
            // One operation so every connection exists and every driver is registered; then leave
            // it completely alone.
            cluster.client(0).put(utf8("bench/idle"), utf8("v")).get(30, TimeUnit.SECONDS);
            final com.sun.management.OperatingSystemMXBean os =
                    (com.sun.management.OperatingSystemMXBean)
                            ManagementFactory.getOperatingSystemMXBean();
            os.getProcessCpuLoad();          // first call primes the measurement window
            Thread.sleep(2_000L);
            os.getProcessCpuLoad();
            Thread.sleep(5_000L);
            final double load = os.getProcessCpuLoad();
            final int cores = Runtime.getRuntime().availableProcessors();
            record("idle cost", "N=3 + 1 client, nothing in flight",
                    String.format("%.2f%% of %d cores (~%.3f core)", load * 100, cores, load * cores));
        }
    }


    /** One operation to time; the future it returns is awaited before the next one starts. */
    @FunctionalInterface
    private interface Op {
        CompletableFuture<?> start();
    }

    /**
     * Latency of {@code op}, issued one at a time, so what is measured is one operation's round trip
     * rather than how deep a queue the client accepts. Reported as p50/p99: a mean hides the tail
     * that a request deadline or a lease has to be sized against.
     */
    private static String time(final Op op) throws Exception {
        for (int i = 0; i < WARMUP_OPS; i++) {
            op.start().get(30, TimeUnit.SECONDS);
        }
        final long[] nanos = new long[MEASURED_OPS];
        for (int i = 0; i < MEASURED_OPS; i++) {
            final long start = System.nanoTime();
            op.start().get(30, TimeUnit.SECONDS);
            nanos[i] = System.nanoTime() - start;
        }
        Arrays.sort(nanos);
        return String.format("p50 %.2fms  p99 %.2fms",
                nanos[MEASURED_OPS / 2] / 1e6, nanos[(int) (MEASURED_OPS * 0.99)] / 1e6);
    }

    /**
     * Writes/second with {@code clientCount} clients driving concurrently, each keeping one
     * operation in flight.
     *
     * @param sameKey when true every client writes the same key, so every round is a duel: the
     *                losers re-run against a register another client has already moved
     */
    private static double drive(final BenchCluster cluster, final int clientCount,
                                final boolean sameKey) throws Exception {
        final CountDownLatch ready = new CountDownLatch(clientCount);
        final CountDownLatch go = new CountDownLatch(1);
        final AtomicLong applied = new AtomicLong();
        final List<Thread> threads = new ArrayList<>();

        for (int c = 0; c < clientCount; c++) {
            final DisCasClient client = cluster.client(c);
            final ByteBuffer key = utf8(sameKey ? "bench/shared" : "bench/key-" + c);
            final Thread thread = new Thread(() -> {
                try {
                    // Warm what this thread will then time: the connection, the coordinator choice,
                    // and the key's first round, which allocates its state on every node.
                    for (int i = 0; i < 20; i++) {
                        client.put(key.duplicate(), utf8("w")).get(30, TimeUnit.SECONDS);
                    }
                    ready.countDown();
                    go.await();
                    for (int i = 0; i < OPS_PER_LOAD_CLIENT; i++) {
                        client.put(key.duplicate(), utf8("v" + i)).get(30, TimeUnit.SECONDS);
                        applied.incrementAndGet();
                    }
                } catch (final Exception e) {
                    throw new IllegalStateException("Load client failed", e);
                }
            }, "bench-load-" + c);
            threads.add(thread);
            thread.start();
        }

        ready.await();
        final long start = System.nanoTime();
        go.countDown();
        for (final Thread thread : threads) {
            thread.join();
        }
        final long elapsedNanos = System.nanoTime() - start;
        return applied.get() / (elapsedNanos / 1e9);
    }

    private static ByteBuffer utf8(final String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }


    private static void record(final String what, final String conditions, final String result) {
        final String line = String.format("| %-24s | %-28s | %s", what, conditions, result);
        REPORT.add(line);
        // Printed as it is taken as well as collected: these runs are minutes long, and one that is
        // interrupted should still have said what it learned.
        System.out.println(line);
    }

    private static void printReport() {
        final StringBuilder sb = new StringBuilder("\n=== performance envelope ===\n");
        sb.append(String.format("| %-24s | %-28s | %s%n", "measurement", "conditions", "result"));
        for (final String line : REPORT) {
            sb.append(line).append('\n');
        }
        sb.append("java ").append(System.getProperty("java.version"))
                .append("  os ").append(System.getProperty("os.name"))
                .append(' ').append(System.getProperty("os.arch"))
                .append("  cores ").append(Runtime.getRuntime().availableProcessors())
                .append('\n');
        System.out.println(sb);
    }

    /** The bare TCP round trip this machine can do, with nothing of this project in the path. */
    private static final class PingRtt {
        private PingRtt() {
        }

        private static String measure() throws IOException, InterruptedException {
            try (ServerSocket server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress())) {
                final Thread echo = new Thread(() -> {
                    try (Socket accepted = server.accept()) {
                        accepted.setTcpNoDelay(true);
                        final InputStream in = accepted.getInputStream();
                        final OutputStream out = accepted.getOutputStream();
                        int b;
                        while ((b = in.read()) >= 0) {
                            out.write(b);
                            out.flush();
                        }
                    } catch (final IOException done) {
                        // closed
                    }
                }, "ping-echo");
                echo.setDaemon(true);
                echo.start();

                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()));
                    socket.setTcpNoDelay(true);
                    final OutputStream out = socket.getOutputStream();
                    final InputStream in = socket.getInputStream();
                    for (int i = 0; i < 200; i++) {
                        out.write(1);
                        out.flush();
                        in.read();
                    }
                    final long[] nanos = new long[1_000];
                    for (int i = 0; i < nanos.length; i++) {
                        final long start = System.nanoTime();
                        out.write(1);
                        out.flush();
                        in.read();
                        nanos[i] = System.nanoTime() - start;
                    }
                    Arrays.sort(nanos);
                    return String.format("p50 %.3fms  p99 %.3fms",
                            nanos[nanos.length / 2] / 1e6, nanos[(int) (nanos.length * 0.99)] / 1e6);
                }
            }
        }
    }
}
