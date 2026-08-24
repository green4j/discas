/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.performance;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A TCP hop that holds every byte for a fixed one-way delay, standing in for the link between two
 * regions.
 *
 * <h2>Why it busy-waits</h2>
 * The delay is the quantity every inter-region figure is taken against, and a consensus round
 * crosses this link four times -- so an error per crossing lands in the published latency
 * multiplied by four, with nothing in the output to tell it apart from the store being slow.
 * <p>
 * Measured against a plain TCP echo: parking until the deadline (what a {@code DelayQueue} does
 * underneath) delivers a 25ms link as a 56.6ms round trip at p50 and 60.4ms at p99, against the
 * 50ms asked for -- <b>+6.6ms and +10.4ms of pure instrument</b>, roughly 3ms per crossing, or 13ms
 * on a write. Busy-waiting delivers 50.33ms and 50.53ms, <b>+0.33ms and +0.53ms</b>, under the
 * noise of everything being measured. The forwarding itself is not the difference: with the delay
 * set to zero both are within 0.09ms of the 0.024ms direct loopback round trip.
 * <p>
 * So this spins, and it costs a core per direction while the link is in use. That is the trade this
 * module exists to be allowed to make -- a benchmark may own the machine, a general-purpose test
 * harness may not, which is also why the chaos suite's proxy stays scheduler-driven: its faults are
 * about whether a message arrives, not exactly when.
 *
 * <h2>Byte-level, not frame-level</h2>
 * It forwards bytes as they come, without understanding framing. Nothing here needs to know where a
 * message ends -- the delay is the same for every byte -- and a parser would only add a way to be
 * wrong about a protocol this module does not own.
 */
final class DelayLink implements AutoCloseable {

    /** How much is read from the socket at a time; sized to hold a whole peer message comfortably. */
    private static final int CHUNK_BYTES = 64 * 1024;

    private final InetSocketAddress target;
    private final long delayNanos;
    private final ServerSocket serverSocket;
    private final InetSocketAddress listenAddress;
    private final List<Socket> liveSockets = new CopyOnWriteArrayList<>();
    private volatile boolean closed;
    /** Chunks delivered in each direction; the instrument for "how many times did this cross?". */
    private final AtomicLong forwardChunks = new AtomicLong();
    private final AtomicLong reverseChunks = new AtomicLong();

    DelayLink(final InetSocketAddress target, final long delayMillis) throws IOException {
        this.target = target;
        this.delayNanos = delayMillis * 1_000_000L;
        this.serverSocket = new ServerSocket(0, 64, InetAddress.getLoopbackAddress());
        this.listenAddress = new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
    }

    InetSocketAddress listenAddress() {
        return listenAddress;
    }

    void start() {
        spawn("delay-accept-" + listenAddress.getPort(), this::acceptLoop);
    }

    private void acceptLoop() {
        while (!closed) {
            final Socket dialer;
            try {
                dialer = serverSocket.accept();
            } catch (final IOException closedSocket) {
                return;
            }
            final Socket upstream = new Socket();
            try {
                upstream.connect(target);
                dialer.setTcpNoDelay(true);
                upstream.setTcpNoDelay(true);
            } catch (final IOException unreachable) {
                closeQuietly(dialer);
                closeQuietly(upstream);
                continue;
            }
            liveSockets.add(dialer);
            liveSockets.add(upstream);
            pump(dialer, upstream, "fwd", forwardChunks);
            pump(upstream, dialer, "rev", reverseChunks);
        }
    }

    /** Chunks this link has carried, both directions summed. */
    long crossings() {
        return forwardChunks.get() + reverseChunks.get();
    }

    /**
     * One direction: a reader that stamps each chunk with its due time as it arrives, and a writer
     * that waits that time out. Splitting the two keeps the link pipelined -- a single thread that
     * waited before reading again would turn a delay into a throughput ceiling of one chunk per
     * interval, and the benchmark would then be measuring this class.
     */
    private void pump(final Socket in, final Socket out, final String tag,
                      final AtomicLong counter) {
        final Queue<Chunk> queue = new ConcurrentLinkedQueue<>();
        spawn("delay-read-" + tag + "-" + listenAddress.getPort(), () -> {
            try {
                final DataInputStream source = new DataInputStream(in.getInputStream());
                final byte[] buffer = new byte[CHUNK_BYTES];
                while (!closed) {
                    final int read = source.read(buffer);
                    if (read < 0) {
                        break;
                    }
                    final byte[] copy = new byte[read];
                    System.arraycopy(buffer, 0, copy, 0, read);
                    counter.incrementAndGet();
                    queue.add(new Chunk(copy, System.nanoTime() + delayNanos));
                }
            } catch (final EOFException | SocketException expected) {
                // the pair closed
            } catch (final IOException torn) {
                // treat as a torn connection
            } finally {
                closeQuietly(in);
                closeQuietly(out);
            }
        });
        spawn("delay-write-" + tag + "-" + listenAddress.getPort(), () -> {
            try {
                final DataOutputStream sink = new DataOutputStream(out.getOutputStream());
                while (!closed) {
                    final Chunk chunk = queue.poll();
                    if (chunk == null) {
                        Thread.onSpinWait();
                        continue;
                    }
                    // No parking, no sleeping, no scheduler between the deadline and the write.
                    while (System.nanoTime() - chunk.dueNanos < 0) {
                        Thread.onSpinWait();
                    }
                    sink.write(chunk.bytes);
                    sink.flush();
                }
            } catch (final IOException torn) {
                closeQuietly(in);
                closeQuietly(out);
            }
        });
    }

    private void spawn(final String name, final Runnable body) {
        final Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        thread.start();
    }

    private static final class Chunk {
        private final byte[] bytes;
        private final long dueNanos;

        private Chunk(final byte[] bytes, final long dueNanos) {
            this.bytes = bytes;
            this.dueNanos = dueNanos;
        }
    }

    @Override
    public void close() {
        closed = true;
        closeQuietly(serverSocket);
        for (final Socket socket : liveSockets) {
            closeQuietly(socket);
        }
    }

    private static void closeQuietly(final Closeable closeable) {
        try {
            closeable.close();
        } catch (final Exception ignored) {
            // teardown
        }
    }
}
