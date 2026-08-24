/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls;

import io.github.green4j.discas.TestPorts;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.PeerMessage;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpPeerTransport;
import io.github.green4j.discas.node.transport.TcpTransportConfig;

import org.junit.jupiter.api.DisplayName;
import io.github.green4j.discas.TestAwait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Focused two-node mTLS peer transport handshake + message delivery. */
@DisplayName("mTLS peer transport -- two-node delivery")
class MtlsTransportTest {

    private static final ClusterId CLUSTER = ClusterId.of("mtls-tx");

    @Test
    @DisplayName("Node 1 delivers a peer message to node 2 over an mTLS link")
    void deliversOverMtls(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final NodeId n1 = NodeId.of("1");
        final NodeId n2 = NodeId.of("2");
        final int p1 = freePort();
        final int p2 = freePort();
        final Map<NodeId, InetSocketAddress> addrs = Map.of(
                n1, new InetSocketAddress("127.0.0.1", p1),
                n2, new InetSocketAddress("127.0.0.1", p2));
        final TcpTransportConfig config = TcpTransportConfig.defaults();

        final EventLoop loop1 = new EventLoop("mtls-tx-1");
        final EventLoop loop2 = new EventLoop("mtls-tx-2");
        final TcpPeerTransport t1 = new TcpPeerTransport(n1, CLUSTER, 2, loop1,
                new InetSocketAddress("127.0.0.1", p1),
                InMemoryMembers.ofTcp(addrs), ca.provider(n1), config);
        final TcpPeerTransport t2 = new TcpPeerTransport(n2, CLUSTER, 2, loop2,
                new InetSocketAddress("127.0.0.1", p2),
                InMemoryMembers.ofTcp(addrs), ca.provider(n2), config);

        final CountDownLatch received = new CountDownLatch(1);
        // A transport that cannot state a promise ceiling neither dials nor accepts -- the
        // rollback guard. A node binds this from its store; a bare
        // transport has to be told, exactly as one.
        t1.bindPromiseCeiling(() -> 0L);
        t2.bindPromiseCeiling(() -> 0L);
        t1.register(msg -> { });
        t2.register(msg -> received.countDown());
        loop1.start();
        loop2.start();
        try {
            final PeerMessage msg = new PeerMessage.PrepareReq(
                    n1, 1L, new HashedBytes(ByteBuffer.wrap(new byte[]{7})), new Ballot(1, n1));
            loop1.execute(() -> t1.send(n2, msg));
            assertTrue(received.await(15, TimeUnit.SECONDS),
                    "Node 2 should receive the peer message over mTLS");
        } finally {
            t1.close();
            t2.close();
            loop1.shutdown();
            loop2.shutdown();
            loop1.awaitTermination(Duration.ofSeconds(2));
            loop2.awaitTermination(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("The single mTLS link carries messages in both directions")
    void bothDirectionsOverOneLink(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final NodeId n1 = NodeId.of("1");
        final NodeId n2 = NodeId.of("2");
        final int p1 = freePort();
        final int p2 = freePort();
        final Map<NodeId, InetSocketAddress> addrs = Map.of(
                n1, new InetSocketAddress("127.0.0.1", p1),
                n2, new InetSocketAddress("127.0.0.1", p2));
        final TcpTransportConfig config = TcpTransportConfig.defaults();

        final EventLoop loop1 = new EventLoop("mtls-bx-1");
        final EventLoop loop2 = new EventLoop("mtls-bx-2");
        final TcpPeerTransport t1 = new TcpPeerTransport(n1, CLUSTER, 2, loop1,
                new InetSocketAddress("127.0.0.1", p1),
                InMemoryMembers.ofTcp(addrs), ca.provider(n1), config);
        final TcpPeerTransport t2 = new TcpPeerTransport(n2, CLUSTER, 2, loop2,
                new InetSocketAddress("127.0.0.1", p2),
                InMemoryMembers.ofTcp(addrs), ca.provider(n2), config);

        final CountDownLatch got1 = new CountDownLatch(1);
        final CountDownLatch got2 = new CountDownLatch(1);
        // A transport that cannot state a promise ceiling neither dials nor accepts -- the
        // rollback guard. A node binds this from its store; a bare
        // transport has to be told, exactly as one.
        t1.bindPromiseCeiling(() -> 0L);
        t2.bindPromiseCeiling(() -> 0L);
        t1.register(msg -> got1.countDown());
        t2.register(msg -> got2.countDown());
        loop1.start();
        loop2.start();
        try {
            // node 1 (lower id) dials node 2; node 2 (higher id) sends back over
            // that same link once it exists. Both are retried (as consensus does)
            // until delivered -- send is best-effort until the link is up.
            TestAwait.until("both directions of the mTLS link to carry a message",
                    Duration.ofSeconds(20), () -> {
                        loop1.execute(() -> trySend(t1, n2, prepare(n1)));
                        loop2.execute(() -> trySend(t2, n1, prepare(n2)));
                        if (got1.getCount() > 0 || got2.getCount() > 0) {
                            throw new IllegalStateException(
                                    "Waiting: n1=" + got1.getCount() + " n2=" + got2.getCount());
                        }
                    });
            assertTrue(got2.await(1, TimeUnit.SECONDS), "Node 2 receives from node 1");
            assertTrue(got1.await(1, TimeUnit.SECONDS), "Node 1 receives from node 2");
        } finally {
            t1.close();
            t2.close();
            loop1.shutdown();
            loop2.shutdown();
            loop1.awaitTermination(Duration.ofSeconds(2));
            loop2.awaitTermination(Duration.ofSeconds(2));
        }
    }

    private static void trySend(final TcpPeerTransport t, final NodeId to, final PeerMessage msg) {
        try {
            t.send(to, msg);
        } catch (final Exception ignored) {
            // link not up yet -- retried
        }
    }

    private static PeerMessage prepare(final NodeId from) {
        return new PeerMessage.PrepareReq(from, 1L, new HashedBytes(ByteBuffer.wrap(new byte[]{7})),
                new Ballot(1, from));
    }

    /** Delegates to the shared allocator, which never reissues a port in this JVM. */
    private static int freePort() {
        return TestPorts.free();
    }
}
