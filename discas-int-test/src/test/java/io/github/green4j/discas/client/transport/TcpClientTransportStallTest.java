/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ClientHelloRespCodec;
import io.github.green4j.discas.common.transport.ClientHelloRespStatus;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.TransportProtocol;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A node that starts a chunked reply and goes silent must be dropped, exactly as a node that stops
 * reading is.
 *
 * <p>Both peer-side transports have always checked {@code ChunkingEngine.isStalled} alongside the
 * slow-consumer window; the client transport checked only the window, so a half-delivered chunk
 * stream held its reassembly buffer -- and the transport byte budget it counts against -- for as
 * long as the connection stayed open, which is forever if the far side simply stops. Found by
 * reading the three transports side by side rather than by a failure.
 */
@DisplayName("TcpClientTransport eviction")
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class TcpClientTransportStallTest {

    private static final NodeId NODE = NodeId.of("n1");
    private static final int STREAM_TOTAL_BYTES = 4096;

    @Test
    @DisplayName("a chunked reply that stops halfway is evicted like a node that stopped reading")
    void stalledInboundAssemblyIsEvicted() throws Exception {
        final ClientTransportConfig config = ClientTransportConfig.builder()
                .maxFrameBytes(4096).maxRxBufferBytes(1 << 16).maxConnections(8).build();

        try (ServerSocket listener = new ServerSocket(0, 16, InetAddress.getLoopbackAddress())) {

            final Thread stallingNode = new Thread(() -> serveThenStall(listener, config),
                    "stalling-node");
            stallingNode.setDaemon(true);
            stallingNode.start();

            final EventLoop loop = new EventLoop("tcp-client-stall");
            final TcpClientTransport transport = new TcpClientTransport(
                    loop,
                    Collections.singletonMap(NODE,
                            new InetSocketAddress("127.0.0.1", listener.getLocalPort())),
                    config,
                    ClientId.of("stall-test"),
                    "",
                    PlaintextClientSecurity.PROVIDER);

            final CountDownLatch lost = new CountDownLatch(1);
            transport.register(message -> { });
            transport.registerConnectionLost(node -> lost.countDown());

            loop.start();
            try {
                loop.execute(() -> transport.send(NODE, new ClientMessage.ClientGetReq(
                        "stall-test", 1L,
                        ByteBuffer.wrap("k".getBytes(StandardCharsets.UTF_8)))));

                // The eviction timer runs once a second and the window is five, so a connection
                // that is going to be dropped is dropped well inside this.
                assertTrue(lost.await(30, TimeUnit.SECONDS),
                        "a connection holding a stalled chunk assembly must be evicted");
            } finally {
                transport.close();
                loop.shutdown();
                loop.awaitTermination(Duration.ofSeconds(5));
            }
        }
    }

    /** Accepts one connection, answers its hello, opens a chunk stream and then says nothing. */
    private static void serveThenStall(final ServerSocket listener,
                                       final ClientTransportConfig config) {
        try (Socket socket = listener.accept()) {
            final FrameCodec codec = new FrameCodec(config.maxFrameBytes());
            readFirstFrame(socket.getInputStream(), config.maxFrameBytes());

            final OutputStream out = socket.getOutputStream();
            write(out, codec.encode(FrameCodec.TYPE_CLIENT_HELLO_RESP,
                    ClientHelloRespCodec.encode(ClientHelloRespStatus.OK, 1,
                            System.currentTimeMillis())));

            final ByteBuffer start = ByteBuffer.allocate(Long.BYTES + Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN);
            start.putLong(1L);                  // stream id
            start.putInt(STREAM_TOTAL_BYTES);   // a payload that never arrives
            start.flip();
            write(out, codec.encode(FrameCodec.TYPE_CHUNK_START, start));

            Thread.sleep(TransportProtocol.SLOW_CONSUMER_TIMEOUT_MS * 6L);
        } catch (final Exception ignored) {
            // The client closing the socket is the outcome under test, not a failure here.
        }
    }

    private static void write(final OutputStream out, final ByteBuffer frame) throws Exception {
        final byte[] raw = new byte[frame.remaining()];
        frame.get(raw);
        out.write(raw);
        out.flush();
    }

    private static FrameCodec.Frame readFirstFrame(final InputStream in, final int maxFrameBytes)
            throws Exception {
        final FrameCodec codec = new FrameCodec(maxFrameBytes);
        final ByteBuffer rx = ByteBuffer.allocate(maxFrameBytes
                + FrameCodec.FRAME_LENGTH_PREFIX_BYTES);
        final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            final int b = in.read();
            if (b < 0) {
                break;
            }
            rx.put((byte) b);
            final List<FrameCodec.Frame> frames = codec.drain(rx);
            if (!frames.isEmpty()) {
                return frames.get(0);
            }
        }
        return fail("Timed out waiting for CLIENT_HELLO");
    }
}
