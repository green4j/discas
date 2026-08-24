/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.TestPorts;
import io.github.green4j.discas.common.transport.FrameCodec;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A frame-aware TCP proxy that sits on a single peer connection and injects network faults <b>at
 * the wire level</b>, so no fault hook has to leak into the production API.
 *
 * <p>The dialer connects to {@link #listenAddress()}; the proxy forwards to the
 * real target bind port. It parses the length-prefixed {@link FrameCodec} framing
 * (peer links are <b>plaintext</b> in chaos tests) and forwards each complete frame
 * as raw bytes, so dropping a whole frame never desyncs the receiver. TCP's two
 * directions are independent streams, so each is controlled separately:
 * <ul>
 *   <li><b>forward</b> = dialer -> target, <b>reverse</b> = target -> dialer;</li>
 *   <li>while a direction is <b>isolated</b>, or per {@code dropProbability}, its
 *       {@link FrameCodec#TYPE_PEER_MESSAGE} frames are dropped -- {@code PEER_HELLO}/
 *       {@code PEER_HELLO_RESP} and multi-frame chunk frames are always forwarded, so
 *       the connection stays authenticated and chunk reassembly isn't corrupted
 *       (matching the old message-drop semantics).</li>
 * </ul>
 */
public final class ChaosProxy implements AutoCloseable {

    private static final int MAX_FRAME_BYTES = 8 * 1024 * 1024;

    /** One controllable direction of the proxied connection. */
    public static final class Direction {
        private volatile boolean isolated;
        private volatile double dropProbability;
        private final Random rng;
        private final AtomicLong framesSeen = new AtomicLong();

        Direction(final long seed) {
            this.rng = new Random(seed);
        }

        private byte droppableType = FrameCodec.TYPE_PEER_MESSAGE;

        public void isolate(final boolean on) {
            this.isolated = on;
        }

        /**
         * Which frame type isolation and {@code dropProbability} apply to. Handshake and chunk
         * frames are never dropped whatever this is set to, so the link stays authenticated and
         * reassembly is not corrupted. Peer links drop {@code TYPE_PEER_MESSAGE} (the default);
         * client links drop {@code TYPE_CLIENT_MESSAGE}.
         */
        public void droppableType(final byte frameType) {
            this.droppableType = frameType;
        }

        public void dropProbability(final double p) {
            if (p < 0.0 || p > 1.0) {
                throw new IllegalArgumentException("dropProbability must be in [0, 1], was " + p);
            }
            this.dropProbability = p;
        }

        /**
         * Droppable frames that have crossed this direction, dropped or not.
         * <p>
         * The observable behind "the request is genuinely outstanding": a test that needs a request
         * to have left the client before it cuts the link can wait for this to move instead of
         * sleeping a guessed interval, which on a slow machine is a race it silently loses.
         */
        public long framesSeen() {
            return framesSeen.get();
        }

        private boolean shouldDrop(final byte frameType) {
            if (frameType == droppableType) {
                framesSeen.incrementAndGet();
            }
            if (frameType != droppableType) {
                return false; // keep handshake + chunk frames intact
            }
            if (isolated) {
                return true;
            }
            final double p = dropProbability;
            if (p <= 0.0) {
                return false;
            }
            synchronized (rng) {
                return rng.nextDouble() < p;
            }
        }
    }

    private final InetSocketAddress target;
    private final ServerSocket serverSocket;
    private final InetSocketAddress listenAddress;
    private final Direction forward;  // dialer -> target
    private final Direction reverse;  // target -> dialer
    private final List<Socket> liveSockets = new CopyOnWriteArrayList<>();
    private final Thread acceptThread;
    private volatile boolean closed;

    private volatile boolean refusingConnections;
    private final AtomicInteger acceptedConnections = new AtomicInteger();

    public ChaosProxy(final InetSocketAddress target, final long seed) {
        this.target = target;
        try {
            // Bind and HOLD a port, taken through the shared allocator so it can never be one
            // already earmarked for a node that has not bound yet.
            this.serverSocket = TestPorts.bindHeld();
        } catch (final RuntimeException e) {
            throw new RuntimeException("ChaosProxy could not bind a listen port", e);
        }
        this.listenAddress = new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
        this.forward = new Direction(seed);
        this.reverse = new Direction(seed ^ 0x5DEECE66DL);
        this.acceptThread = new Thread(this::acceptLoop, "chaos-proxy-" + serverSocket.getLocalPort());
        this.acceptThread.setDaemon(true);
    }

    public InetSocketAddress listenAddress() {
        return listenAddress;
    }

    public Direction forward() {
        return forward;
    }

    public Direction reverse() {
        return reverse;
    }

    public void start() {
        acceptThread.start();
    }

    /**
     * Tear down every live connection, as an abruptly dead peer would. Distinct from isolating a
     * direction: the dialer sees the socket close rather than silence, which is what exercises a
     * client's reconnect path instead of its timeout path.
     */
    public void cutLiveConnections() {
        for (final Socket s : liveSockets) {
            closeQuietly(s);
        }
        liveSockets.clear();
    }

    /**
     * How many connections this proxy has forwarded since it started. The way to tell an actual
     * re-dial from a link that was never broken: asserting only that operations still succeed
     * after a fault passes just as happily when the fault did nothing.
     */
    public int acceptedConnections() {
        return acceptedConnections.get();
    }

    /**
     * While refusing, accepted connections are closed immediately -- the target looks down to
     * anything dialing it, without this proxy giving up its listen port (so the address the client
     * was configured with stays stable across the outage).
     */
    public void refuseNewConnections(final boolean refuse) {
        this.refusingConnections = refuse;
    }

    private void acceptLoop() {
        while (!closed) {
            final Socket dialer;
            try {
                dialer = serverSocket.accept();
            } catch (final IOException e) {
                return; // socket closed
            }
            handleConnection(dialer);
        }
    }

    private void handleConnection(final Socket dialer) {
        if (refusingConnections) {
            closeQuietly(dialer);
            return;
        }
        final Socket upstream = new Socket();
        try {
            upstream.connect(target);
        } catch (final IOException e) {
            closeQuietly(dialer);
            closeQuietly(upstream);
            return;
        }
        acceptedConnections.incrementAndGet();
        liveSockets.add(dialer);
        liveSockets.add(upstream);
        startPump(dialer, upstream, forward, "fwd");
        startPump(upstream, dialer, reverse, "rev");
    }

    private void startPump(final Socket in, final Socket out, final Direction dir, final String tag) {
        final Thread t = new Thread(() -> pump(in, out, dir), "chaos-pump-" + tag + "-" + serverSocket.getLocalPort());
        t.setDaemon(true);
        t.start();
    }

    /** Frame-aware forwarding of one direction; drops whole PEER_MESSAGE frames per {@code dir}. */
    private void pump(final Socket in, final Socket out, final Direction dir) {
        try {
            final DataInputStream src = new DataInputStream(in.getInputStream());
            final DataOutputStream dst = new DataOutputStream(out.getOutputStream());
            while (!closed) {
                final int frameLen = src.readInt();
                if (frameLen <= 0 || frameLen > MAX_FRAME_BYTES) {
                    break; // malformed / not our framing -- tear the pair down
                }
                final byte[] body = new byte[frameLen];
                src.readFully(body);
                // body layout: [int32 crc][byte type][payload]; type is at CHECKSUM offset.
                final byte frameType = body[FrameCodec.FRAME_CHECKSUM_BYTES];
                if (dir.shouldDrop(frameType)) {
                    continue; // drop the whole frame -- receiver reads the next one intact
                }
                synchronized (dst) {
                    dst.writeInt(frameLen);
                    dst.write(body);
                    dst.flush();
                }
            }
        } catch (final EOFException | SocketException e) {
            // peer closed -- normal
        } catch (final IOException e) {
            // treat any IO error as a torn connection
        } finally {
            closeQuietly(in);
            closeQuietly(out);
        }
    }

    @Override
    public void close() {
        closed = true;
        closeQuietly(serverSocket);
        for (final Socket s : liveSockets) {
            closeQuietly(s);
        }
        acceptThread.interrupt();
    }

    private static void closeQuietly(final Closeable c) {
        try {
            c.close();
        } catch (final Exception ignored) {
        }
    }
}
