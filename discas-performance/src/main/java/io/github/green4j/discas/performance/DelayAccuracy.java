/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.performance;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

/**
 * Measures the measuring instrument: how accurately {@code DelayLink} delivers the delay it was
 * asked for.
 *
 * <p>This exists because the inter-region latency figures are taken <em>through</em> that link, and
 * a round crosses it four times. An instrument that is a couple of milliseconds late per crossing
 * publishes a store that is ten milliseconds slower than it is, with nothing in the output to say
 * so. Run this on any machine before believing an inter-region figure taken on it: the error line
 * is the tolerance those figures inherit.
 *
 * <p>The numbers behind how the link waits, and what they cost, are on {@code DelayLink}.
 *
 * <p><b>The ping RTT is the first line of the report and not an afterthought.</b> Every figure here
 * and in {@link PerformanceEnvelope} sits on top of what a bare TCP round trip costs on this
 * machine; without it, a reader cannot tell an expensive protocol from an expensive loopback, and
 * the delay error cannot be separated from the cost of forwarding at all.
 *
 * <pre>{@code ./gradlew :discas-performance:delayAccuracy}</pre>
 */
public final class DelayAccuracy {

    private static final int WARMUP = 200;
    private static final int SAMPLES = 1_000;
    private static final long DELAY_MS = 25L;

    private DelayAccuracy() {
    }

    public static void main(final String[] args) throws Exception {
        try (EchoServer echo = new EchoServer()) {
            final long[] direct = pingSamples(echo.address());
            report("ping RTT, direct loopback", direct, 0L);

            try (DelayLink zero = new DelayLink(echo.address(), 0L)) {
                zero.start();
                report("ping RTT via link, delay=0", pingSamples(zero.listenAddress()), 0L);
            }
            try (DelayLink delayed = new DelayLink(echo.address(), DELAY_MS)) {
                delayed.start();
                // A round trip crosses the link twice, so the expected cost is two delays on top of
                // whatever the loopback and the forwarding already cost.
                report("ping RTT via link, delay=" + DELAY_MS + "ms",
                        pingSamples(delayed.listenAddress()), 2 * DELAY_MS);
            }
        }
    }

    /** Round trips of a single byte, which is the smallest thing that can measure a link. */
    private static long[] pingSamples(final InetSocketAddress target) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(target);
            socket.setTcpNoDelay(true);
            final OutputStream out = socket.getOutputStream();
            final InputStream in = socket.getInputStream();
            for (int i = 0; i < WARMUP; i++) {
                roundTrip(out, in);
            }
            final long[] nanos = new long[SAMPLES];
            for (int i = 0; i < SAMPLES; i++) {
                final long start = System.nanoTime();
                roundTrip(out, in);
                nanos[i] = System.nanoTime() - start;
            }
            Arrays.sort(nanos);
            return nanos;
        }
    }

    private static void roundTrip(final OutputStream out, final InputStream in) throws IOException {
        out.write(1);
        out.flush();
        if (in.read() < 0) {
            throw new IOException("Echo server closed");
        }
    }

    /**
     * @param expectedMs what the link was asked to add to a round trip; the error against it is the
     *                   number that decides whether an inter-region figure means anything
     */
    private static void report(final String what, final long[] sortedNanos, final long expectedMs) {
        final double p50 = sortedNanos[sortedNanos.length / 2] / 1e6;
        final double p99 = sortedNanos[(int) (sortedNanos.length * 0.99)] / 1e6;
        final double max = sortedNanos[sortedNanos.length - 1] / 1e6;
        final String error = expectedMs == 0 ? ""
                : String.format("  error vs %dms: p50 %+.3fms  p99 %+.3fms",
                        expectedMs, p50 - expectedMs, p99 - expectedMs);
        System.out.printf("%-42s p50 %7.3fms  p99 %7.3fms  max %7.3fms%s%n",
                what, p50, p99, max, error);
    }

    /** Reads a byte, writes it back, and nothing else -- the cheapest possible far end. */
    private static final class EchoServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private volatile boolean closed;

        private EchoServer() throws IOException {
            this.serverSocket = new ServerSocket(0, 64, InetAddress.getLoopbackAddress());
            final Thread thread = new Thread(this::acceptLoop, "echo-accept");
            thread.setDaemon(true);
            thread.start();
        }

        private InetSocketAddress address() {
            return new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
        }

        private void acceptLoop() {
            while (!closed) {
                final Socket socket;
                try {
                    socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                } catch (final IOException stopped) {
                    return;
                }
                final Thread thread = new Thread(() -> echo(socket), "echo-conn");
                thread.setDaemon(true);
                thread.start();
            }
        }

        private void echo(final Socket socket) {
            try (Socket open = socket) {
                final InputStream in = open.getInputStream();
                final OutputStream out = open.getOutputStream();
                int b;
                while ((b = in.read()) >= 0) {
                    out.write(b);
                    out.flush();
                }
            } catch (final IOException closedByPeer) {
                // done
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                serverSocket.close();
            } catch (final IOException ignored) {
                // teardown
            }
        }
    }
}
