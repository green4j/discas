/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.TransportProtocol;
import io.github.green4j.discas.common.transport.security.PlaintextPeerSecurity;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.PeerMessage;
import io.github.green4j.discas.node.PeerMessageCodec;
import io.github.green4j.discas.node.TestPorts;
import io.github.green4j.discas.node.membership.InMemoryMembers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The handshake cases either side of the answer table: what happens before a hello arrives, what a
 * server does when its <em>own</em> replay is unfinished, and the two admissions the refusals must
 * leave alone.
 *
 * <p>Which hello produces which {@link PeerHelloRespStatus} is {@code PeerHandshakeRefusalMatrixTest},
 * where the space is enumerated and closed. The cases here are the ones that are not a row: they
 * turn on ordering and on the server's own state rather than on the shape of what arrived.
 */
@DisplayName("TCP peer transport handshake")
class TcpPeerTransportHandshakeCompatibilityTest {

    private static final ClusterId CLUSTER = ClusterId.of("test-cluster");

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    @Test
    @DisplayName("A frame sent before PEER_HELLO is dropped and the connection is closed")
    void rejectsPeerProtocolMessageBeforeHello() throws Exception {
        final int port = TestPorts.free();
        final EventLoop loop = new EventLoop("tcp-peer-peer-prehello");
        final TcpTransportConfig config = TcpTransportConfig.builder()
                .maxFrameBytes(1024).maxRxBufferBytes(1 << 16).maxConnections(64).build();
        final TcpPeerTransport peerTransport = new TcpPeerTransport(
                nid(1),
                CLUSTER,
                2,
                loop,
                new InetSocketAddress("127.0.0.1", port),
                InMemoryMembers.ofTcp(Map.of(
                        nid(1), new InetSocketAddress("127.0.0.1", port),
                        nid(2), new InetSocketAddress("127.0.0.1", port + 1))),
                PlaintextPeerSecurity.PROVIDER,
                config);
        final AtomicInteger delivered = new AtomicInteger(0);
        peerTransport.register(message -> delivered.incrementAndGet());

        loop.start();
        try {
            final FrameCodec frameCodec = new FrameCodec(config.maxFrameBytes());
            final ByteBuffer payload = PeerMessageCodec.encode(new PeerMessage.PrepareReq(
                    nid(2), 77L, new HashedBytes(ByteBuffer.wrap(new byte[]{5})), new Ballot(1, nid(2))));
            final ByteBuffer wire = frameCodec.encode(FrameCodec.TYPE_PEER_MESSAGE, payload);

            try (Socket socket = new Socket("127.0.0.1", port)) {
                final OutputStream out = socket.getOutputStream();
                final byte[] raw = new byte[wire.remaining()];
                wire.get(raw);
                out.write(raw);
                out.flush();
            }

            Thread.sleep(200L);
            assertFalse(delivered.get() > 0);
        } finally {
            peerTransport.close();
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("A replaced disk claiming less is admitted -- the case the guard must not catch")
    void admitsALowerCeilingUnderANewIncarnation() throws Exception {
        try (PeerServer server = startPeerServer()) {
            assertEquals(PeerHelloRespStatus.OK,
                    handshake(server.port(), TransportProtocol.PROTOCOL_VERSION, CLUSTER, nid(2),
                            SERVER_CLUSTER_SIZE, IncarnationId.generate(), 100L).status);

            // A wiped member: new storage, nothing to have lost, and it asks the cluster for a
            // promise floor before it serves anything. Refusing it would only stop it asking --
            // which is what INCARNATION_CHANGED was retired for.
            assertEquals(PeerHelloRespStatus.OK,
                    handshake(server.port(), TransportProtocol.PROTOCOL_VERSION, CLUSTER, nid(2),
                            SERVER_CLUSTER_SIZE, IncarnationId.generate(), 0L).status);
        }
    }

    @Test
    @DisplayName("A peer that has not replayed is refused; one that cannot prove its ceiling is not")
    void separatesNotReplayedFromUnproven() throws Exception {
        try (PeerServer server = startPeerServer()) {
            final HelloResp replaying = handshake(server.port(), TransportProtocol.PROTOCOL_VERSION,
                    CLUSTER, nid(2), SERVER_CLUSTER_SIZE, IncarnationId.generate(),
                    PeerTransport.PROMISE_CEILING_REPLAYING);
            assertEquals(PeerHelloRespStatus.NOT_REPLAYED, replaying.status,
                    "A handshake happens once, so a claim that cannot be checked is refused rather "
                            + "than admitted unchecked");
            assertEquals(PeerHelloRespStatus.CAUSE_THEIRS, replaying.cause);

            final HelloResp unproven = handshake(server.port(), TransportProtocol.PROTOCOL_VERSION,
                    CLUSTER, nid(2), SERVER_CLUSTER_SIZE, IncarnationId.generate(),
                    PeerTransport.PROMISE_CEILING_UNPROVEN);
            assertEquals(PeerHelloRespStatus.OK, unproven.status,
                    "But a node whose log has a hole must be let in: being let in is how it gets a "
                            + "floor from the cluster, and it serves nothing until it has one");
        }
    }

    @Test
    @DisplayName("A node that has not replayed refuses handshakes rather than making a claim")
    void refusesWhileItCannotStateItsOwnCeiling() throws Exception {
        try (PeerServer server = startPeerServer(PeerTransport.PROMISE_CEILING_REPLAYING)) {
            final HelloResp resp = handshake(server.port(), TransportProtocol.PROTOCOL_VERSION,
                    CLUSTER, nid(2), SERVER_CLUSTER_SIZE, IncarnationId.generate(), 100L);
            assertEquals(PeerHelloRespStatus.NOT_REPLAYED, resp.status);
            assertEquals(PeerHelloRespStatus.CAUSE_OURS, resp.cause,
                    "And it says which end is not ready, since only one of them can act on it");
        }
    }

    private interface PeerServer extends AutoCloseable {
        int port();

        @Override
        void close();
    }

    /** Start a peer server (self=node 1, members {1,2}, cluster {@link #CLUSTER}). */
    private PeerServer startPeerServer() throws Exception {
        return startPeerServer(0L);
    }

    /**
     * @param ownCeiling what this server claims about its own storage. A real node binds this from
     *                   its store once replay has established it; passing
     *                   {@link PeerTransport#PROMISE_CEILING_REPLAYING} is a node that has not got
     *                   that far, which refuses every handshake until it does.
     */
    private PeerServer startPeerServer(final long ownCeiling) throws Exception {
        final int port = TestPorts.free();
        final EventLoop loop = new EventLoop("hs-server-" + port);
        final TcpTransportConfig config = TcpTransportConfig.builder()
                .maxFrameBytes(1024).maxRxBufferBytes(1 << 16).maxConnections(64).build();
        final TcpPeerTransport peer = new TcpPeerTransport(
                nid(1),
                CLUSTER,
                2,
                loop,
                new InetSocketAddress("127.0.0.1", port),
                InMemoryMembers.ofTcp(Map.of(
                        nid(1), new InetSocketAddress("127.0.0.1", port),
                        nid(2), new InetSocketAddress("127.0.0.1", port + 1))),
                PlaintextPeerSecurity.PROVIDER,
                config);
        peer.register(message -> { });
        peer.bindPromiseCeiling(() -> ownCeiling);
        loop.start();
        return new PeerServer() {
            @Override
            public int port() {
                return port;
            }

            @Override
            public void close() {
                peer.close();
                loop.shutdown();
                loop.awaitTermination(Duration.ofSeconds(2));
            }
        };
    }

    /** The server built by {@link #startPeerServer()} has members {1,2} -> N = 2. */
    private static final int SERVER_CLUSTER_SIZE = 2;

    private static final class HelloResp {
        final PeerHelloRespStatus status;
        final String cause;

        HelloResp(final PeerHelloRespStatus status, final String cause) {
            this.status = status;
            this.cause = cause;
        }
    }

    /** Send a well-formed PEER_HELLO and return the decoded HELLO_RESP {status, cause}. */
    private static HelloResp handshake(final int serverPort, final int version,
                                       final ClusterId clusterId, final NodeId peerId,
                                       final int clusterSize, final IncarnationId incarnation,
                                       final long promiseCeiling) throws Exception {
        final FrameCodec frameCodec = new FrameCodec(1 << 20);
        final ByteBuffer hello = PeerHelloCodec.encode(version, clusterId, peerId,
                0L,                          // nonce
                System.currentTimeMillis(),  // fresh timestamp
                clusterSize,                 // our N (1..255)
                incarnation,
                promiseCeiling);
        final ByteBuffer wire = frameCodec.encode(FrameCodec.TYPE_PEER_HELLO, hello);
        try (Socket socket = new Socket("127.0.0.1", serverPort)) {
            socket.setSoTimeout(5000);
            final byte[] raw = new byte[wire.remaining()];
            wire.get(raw);
            socket.getOutputStream().write(raw);
            socket.getOutputStream().flush();
            final FrameCodec.Frame resp = readFirstFrame(socket.getInputStream(), 1 << 20);
            assertEquals(FrameCodec.TYPE_PEER_HELLO_RESP, resp.type);
            final PeerHelloRespCodec.Decoded decoded = PeerHelloRespCodec.decode(resp.payload);
            return new HelloResp(decoded.status, decoded.cause);
        }
    }

    private static FrameCodec.Frame readFirstFrame(final InputStream in, final int maxFrameBytes)
            throws Exception {
        final FrameCodec codec = new FrameCodec(maxFrameBytes);
        final ByteBuffer rx = ByteBuffer.allocate(maxFrameBytes
                + FrameCodec.FRAME_LENGTH_PREFIX_BYTES);
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            final int b = in.read();
            if (b < 0) {
                break;
            }
            rx.put((byte) b);
            // Try to drain a complete frame each byte. drain() flips rx and
            // compacts; if nothing is decodable yet, the loop continues.
            final List<FrameCodec.Frame> frames = codec.drain(rx);
            if (!frames.isEmpty()) {
                return frames.get(0);
            }
        }
        return fail("Timed out waiting for HELLO_RESP frame");
    }

}
