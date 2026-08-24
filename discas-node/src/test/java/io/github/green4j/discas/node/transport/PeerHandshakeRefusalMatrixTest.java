/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.TransportProtocol;
import io.github.green4j.discas.common.transport.security.PlaintextPeerSecurity;
import io.github.green4j.discas.node.TestPorts;
import io.github.green4j.discas.node.membership.InMemoryMembers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Every answer a node can give to a {@code PEER_HELLO}, and the one hello that produces it.
 *
 * <p>The axis is the response itself: {@link PeerHelloRespStatus} is a closed set of eleven, each
 * of which is either a refusal an operator has to act on -- every one maps to a named
 * {@code OperatorState} with a remedy -- or the admission that lets a member into the mesh. So the
 * space is small, and worth closing rather than sampling: a refusal with no test is a refusal
 * nobody notices going missing, and a wire status that no code path can produce is a promise the
 * protocol no longer keeps.
 *
 * <p><b>What keeps the space closed is the {@code switch} below</b>: exhaustive over the enum with
 * a failing {@code default}, so a twelfth status fails the build until it has a row. That is the
 * property this file exists for. Two statuses reachable in production had no row anywhere until it
 * was written -- a peer whose clock puts it outside the replay bound, and a copied data directory
 * arriving under a second member id.
 *
 * <p>One row asserts an <em>absence</em>. {@link PeerHelloRespStatus#INCARNATION_CHANGED} is
 * retired: a member that comes back on replaced storage is admitted and reported, never refused,
 * because refusing it would only stop it asking the cluster for the promise floor it needs. The
 * constant stays on the wire so an older node's rejection still decodes, and the row states that
 * nothing here sends it.
 *
 * <p>{@code TcpPeerTransportHandshakeCompatibilityTest} holds the cases either side of the table: a
 * frame that arrives before the hello, a server that refuses because <em>its own</em> replay is
 * unfinished, and the two admissions the refusals must leave alone.
 */
@DisplayName("PEER_HELLO -- every answer, and what produces it")
class PeerHandshakeRefusalMatrixTest {

    private static final ClusterId CLUSTER = ClusterId.of("test-cluster");
    /** Members {1,2,3}, self = 1. Three, so that two *other* ids can present one incarnation. */
    private static final int CLUSTER_SIZE = 3;
    private static final long BEYOND_THE_SKEW_BOUND_MS = Duration.ofMinutes(10).toMillis();

    /**
     * The table. One row per shape of hello an operator or a broken peer can put on the wire, and
     * the answer it must get -- which is the column that makes this a matrix rather than a list.
     */
    private enum Shape {
        // Nothing wrong with it: the row every refusal is measured against.
        WELL_FORMED(PeerHelloRespStatus.OK),

        // Wrong before the payload can be trusted. Checked first, because a version we cannot read
        // is a payload we must not parse past.
        OLDER_PROTOCOL(PeerHelloRespStatus.PROTOCOL_MISMATCH),

        // Pointed at the wrong deployment, or at nothing that admits it. These three are how a
        // membership is enforced without a coordinator: the list is the authority, and a node not
        // on it is simply refused.
        FOREIGN_CLUSTER(PeerHelloRespStatus.CLUSTER_MISMATCH),
        NOT_A_MEMBER(PeerHelloRespStatus.UNKNOWN_PEER),
        CLAIMS_OUR_OWN_ID(PeerHelloRespStatus.IDENTITY_MISMATCH),

        // Disagreeing about N. The guard that makes a mixed-N mesh impossible, and with it split
        // brain: the cost is a bounded outage during a resize, never two quorums over one key.
        DIFFERENT_N(PeerHelloRespStatus.CLUSTER_SIZE_MISMATCH),

        // A clock too far out for the hello to be judged fresh. A refusal rather than a warning
        // because past this bound the peer cannot join at all until somebody fixes NTP.
        CLOCK_BEYOND_BOUND(PeerHelloRespStatus.HELLO_TIMESTAMP_SKEW),

        // A ceiling it cannot state yet. A handshake happens once, so a claim that cannot be
        // checked is refused rather than admitted unchecked.
        STILL_REPLAYING(PeerHelloRespStatus.NOT_REPLAYED),

        // The two storage rows, and they answer oppositely on purpose. Replaced storage is state
        // that is *missing*, which the cluster can give back -- so it is admitted, and admitting it
        // is how the member reaches a quorum for the promise floor it cannot recover alone.
        REPLACED_STORAGE(PeerHelloRespStatus.OK),
        // A copy is state *duplicated*, which no recovery makes right.
        COPIED_DIRECTORY(PeerHelloRespStatus.INCARNATION_DUPLICATED),

        // The same storage claiming less than it already proved. No restart, lost tail or wipe
        // produces this; a directory restored from a copy does, and this is where it shows.
        CEILING_WENT_BACKWARDS(PeerHelloRespStatus.CEILING_ROLLED_BACK);

        private final PeerHelloRespStatus expected;

        Shape(final PeerHelloRespStatus expected) {
            this.expected = expected;
        }

        /** So a report row reads as a row of the table: the hello, then the answer it must get. */
        @Override
        public String toString() {
            return name() + " -> " + expected;
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Shape.class)
    void everyShapeGetsItsAnswer(final Shape shape) throws Exception {
        try (PeerServer server = startPeerServer()) {
            assertEquals(shape.expected, answerTo(shape, server).status,
                    "The wire answer is the contract an operator acts on");
            assertCause(shape, server);
        }
    }

    /**
     * The closure: every status the wire can carry is some row's answer, or is named here as one
     * nothing sends. Derived from the table above rather than restated, so the two cannot disagree.
     *
     * <p>{@link PeerHelloRespStatus#INCARNATION_CHANGED} is the one exception, and it is retired
     * rather than untested: a member on replaced storage is admitted and reported, never refused,
     * because refusing it would only stop it asking for the floor it needs. The constant stays on
     * the wire so an older node's rejection still decodes -- and {@code REPLACED_STORAGE} above is
     * the row that holds it to that, by requiring {@code OK}.
     */
    @Test
    @DisplayName("Every status the wire can carry is accounted for")
    void theTableCoversTheProtocol() {
        final EnumSet<PeerHelloRespStatus> answered = EnumSet.noneOf(PeerHelloRespStatus.class);
        for (final Shape shape : Shape.values()) {
            answered.add(shape.expected);
        }
        answered.add(PeerHelloRespStatus.INCARNATION_CHANGED);

        final EnumSet<PeerHelloRespStatus> everything = EnumSet.allOf(PeerHelloRespStatus.class);
        everything.removeAll(answered);
        assertEquals(EnumSet.noneOf(PeerHelloRespStatus.class), everything,
                "A status with no row is a refusal nobody would notice going missing. Add the "
                        + "shape of hello that produces it, or state here that nothing does.");
    }

    /** Builds the hello each shape describes and returns what the server answered. */
    private HelloResp answerTo(final Shape shape, final PeerServer server) throws Exception {
        switch (shape) {
            case WELL_FORMED:
                return hello(server).send();
            case OLDER_PROTOCOL:
                return hello(server).version(TransportProtocol.PROTOCOL_VERSION + 1).send();
            case FOREIGN_CLUSTER:
                return hello(server).cluster(ClusterId.of("other-cluster")).send();
            case NOT_A_MEMBER:
                return hello(server).from(nid(9)).send();
            case CLAIMS_OUR_OWN_ID:
                return hello(server).from(nid(1)).send();
            case DIFFERENT_N:
                return hello(server).clusterSize(CLUSTER_SIZE + 1).send();
            case CLOCK_BEYOND_BOUND:
                return hello(server).at(System.currentTimeMillis() + BEYOND_THE_SKEW_BOUND_MS).send();
            case STILL_REPLAYING:
                return hello(server).ceiling(PeerTransport.PROMISE_CEILING_REPLAYING).send();
            case REPLACED_STORAGE:
                admit(server, nid(2), IncarnationId.generate());
                // Same member, different disk. Indistinguishable on the wire from a wipe, and both
                // are admitted for the same reason.
                return hello(server).from(nid(2)).incarnation(IncarnationId.generate()).send();
            case COPIED_DIRECTORY:
                final IncarnationId oneDisk = IncarnationId.generate();
                admit(server, nid(2), oneDisk);
                // A second member id presenting the first one's storage.
                return hello(server).from(nid(3)).incarnation(oneDisk).send();
            case CEILING_WENT_BACKWARDS:
                final IncarnationId sameDisk = IncarnationId.generate();
                admit(server, nid(2), sameDisk, 100L);
                return hello(server).from(nid(2)).incarnation(sameDisk).ceiling(60L).send();
            default:
                // Reached only by a shape added without a hello to go with it.
                return fail(shape + " has no hello. Say what goes on the wire to produce "
                        + shape.expected + ".");
        }
    }

    /**
     * The rows whose {@code cause} is part of the contract. A status says what to do; a cause says
     * which of several ways you arrived at it, and only some statuses have more than one.
     */
    private void assertCause(final Shape shape, final PeerServer server) throws Exception {
        switch (shape) {
            case CLAIMS_OUR_OWN_ID:
                assertEquals(PeerHelloRespStatus.CAUSE_SELF_CLAIM, answerTo(shape, server).cause,
                        "One of the four ways an identity can disagree, and the caller's own doing");
                break;
            case STILL_REPLAYING:
                assertEquals(PeerHelloRespStatus.CAUSE_THEIRS, answerTo(shape, server).cause,
                        "Which end is not ready, since only one of them can act on it");
                break;
            case COPIED_DIRECTORY:
                assertEquals(nid(2).value(), answerTo(shape, server).cause,
                        "Names the member the storage belongs to, so an operator knows which of "
                                + "the two to stop");
                break;
            default:
                break;
        }
    }

    /** Complete a handshake that must succeed, so a later one has something to disagree with. */
    private void admit(final PeerServer server, final NodeId id, final IncarnationId incarnation)
            throws Exception {
        admit(server, id, incarnation, 0L);
    }

    private void admit(final PeerServer server, final NodeId id, final IncarnationId incarnation,
                       final long ceiling) throws Exception {
        assertEquals(PeerHelloRespStatus.OK,
                hello(server).from(id).incarnation(incarnation).ceiling(ceiling).send().status,
                "Setting up " + id.value() + " must itself be admitted");
    }

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    private Hello hello(final PeerServer server) {
        return new Hello(server.port());
    }

    /** One hello, with every field the matrix varies defaulted to the well-formed value. */
    private static final class Hello {
        private final int port;
        private int version = TransportProtocol.PROTOCOL_VERSION;
        private ClusterId cluster = CLUSTER;
        private NodeId peer = nid(2);
        private int clusterSize = CLUSTER_SIZE;
        private IncarnationId incarnation = IncarnationId.generate();
        private long ceiling;
        private long epochMs = System.currentTimeMillis();

        Hello(final int port) {
            this.port = port;
        }

        Hello version(final int v) {
            this.version = v;
            return this;
        }

        Hello cluster(final ClusterId c) {
            this.cluster = c;
            return this;
        }

        Hello from(final NodeId p) {
            this.peer = p;
            return this;
        }

        Hello clusterSize(final int n) {
            this.clusterSize = n;
            return this;
        }

        Hello incarnation(final IncarnationId i) {
            this.incarnation = i;
            return this;
        }

        Hello ceiling(final long c) {
            this.ceiling = c;
            return this;
        }

        Hello at(final long millis) {
            this.epochMs = millis;
            return this;
        }

        HelloResp send() throws Exception {
            final FrameCodec frameCodec = new FrameCodec(1 << 20);
            final ByteBuffer payload = PeerHelloCodec.encode(version, cluster, peer,
                    0L, epochMs, clusterSize, incarnation, ceiling);
            final ByteBuffer wire = frameCodec.encode(FrameCodec.TYPE_PEER_HELLO, payload);
            try (Socket socket = new Socket("127.0.0.1", port)) {
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
    }

    private static final class HelloResp {
        final PeerHelloRespStatus status;
        final String cause;

        HelloResp(final PeerHelloRespStatus status, final String cause) {
            this.status = status;
            this.cause = cause;
        }
    }

    private interface PeerServer extends AutoCloseable {
        int port();

        @Override
        void close();
    }

    /** Self = node 1, members {1,2,3}, a ceiling of its own so it can answer at all. */
    private PeerServer startPeerServer() throws Exception {
        final int port = TestPorts.free();
        final EventLoop loop = new EventLoop("hs-matrix-" + port);
        final TcpTransportConfig config = TcpTransportConfig.builder()
                .maxFrameBytes(1024).maxRxBufferBytes(1 << 16).maxConnections(64).build();
        final TcpPeerTransport peer = new TcpPeerTransport(
                nid(1),
                CLUSTER,
                CLUSTER_SIZE,
                loop,
                new InetSocketAddress("127.0.0.1", port),
                InMemoryMembers.ofTcp(Map.of(
                        nid(1), new InetSocketAddress("127.0.0.1", port),
                        nid(2), new InetSocketAddress("127.0.0.1", port + 1),
                        nid(3), new InetSocketAddress("127.0.0.1", port + 2))),
                PlaintextPeerSecurity.PROVIDER,
                config);
        peer.register(message -> { });
        peer.bindPromiseCeiling(() -> 0L);
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
            final List<FrameCodec.Frame> frames = codec.drain(rx);
            if (!frames.isEmpty()) {
                return frames.get(0);
            }
        }
        return fail("Timed out waiting for HELLO_RESP frame");
    }
}
