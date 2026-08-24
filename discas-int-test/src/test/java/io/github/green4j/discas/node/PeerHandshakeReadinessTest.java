/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Readiness counts <em>handshaked</em> peers, not connected sockets.
 * <p>
 * This is the test that catches the tempting mistake. {@code TcpPeerTransport} has a
 * {@code markConnected()} at {@code SocketChannel.finishConnect()} which looks like the natural
 * place to report a peer up -- and is wrong, because the PEER_HELLO that follows can still be
 * refused. A peer whose handshake was rejected can carry no consensus traffic, so counting it would
 * make a node report ready when nothing it proposes can ever commit.
 * <p>
 * A cluster-size mismatch is the cheapest way to produce exactly that state: the TCP connection
 * establishes normally and the handshake is then refused by the reconfiguration guard.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
@DisplayName("Readiness -- a connected peer is not a handshaked peer")
class PeerHandshakeReadinessTest {

    private static final ClusterId CLUSTER = ClusterId.of("handshake-cluster");
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");

    @Test
    @DisplayName("A rejected handshake leaves the peer uncounted and the node without quorum")
    void rejectedHandshakeDoesNotCount(@TempDir final Path baseDir) throws Exception {
        // n1 believes N=2; n2 believes N=3. They connect and then refuse each other at PEER_HELLO.
        try (Cluster cluster = new Cluster(baseDir, 2, 3)) {
            cluster.start();

            // Give both ends time to connect, attempt the handshake and be refused. Reconnect
            // backoff means this repeats; the state must stay empty throughout.
            Thread.sleep(1_500L);

            assertEquals(0, cluster.peerState.handshakedPeerCount(),
                    "The socket connected, but PEER_HELLO was refused -- the peer must not count");
            assertFalse(cluster.peerState.quorumAvailable(),
                    "A node of 2 with no handshaked peer cannot reach a quorum");
            assertTrue(cluster.peerState.peerSnapshots().isEmpty(),
                    "A peer that never completed a handshake must not appear as though it had");
        }
    }

    @Test
    @DisplayName("A completed handshake does count -- the positive control")
    void completedHandshakeCounts(@TempDir final Path baseDir) throws Exception {
        // Same topology, agreeing on N=2, so the handshake succeeds. Without this the test above
        // could pass simply because nothing ever connects.
        try (Cluster cluster = new Cluster(baseDir, 2, 2)) {
            cluster.start();

            // A plain exception, not an assertion: TestAwait.until retries on Exception, and an
            // AssertionFailedError is an Error, so it would escape on the first poll instead.
            TestAwait.until("n1 sees n2 handshaked", Duration.ofSeconds(30), () -> {
                final int count = cluster.peerState.handshakedPeerCount();
                if (count != 1) {
                    throw new IllegalStateException("Handshaked peers = " + count);
                }
            });
            assertTrue(cluster.peerState.quorumAvailable(),
                    "Self plus one handshaked peer is a quorum of 2");

            final PeerStateObserver.PeerSnapshot peer = cluster.peerState.peerSnapshots().get(0);
            assertEquals("n2", peer.peer.value());
            assertTrue(peer.handshaked);
            assertTrue(peer.changedAtMillis > 0, "A transition must carry a timestamp");
        }
    }

    /** Two TCP nodes sharing a member list, each told its own N so a mismatch can be induced. */
    private static final class Cluster implements AutoCloseable {

        private final List<DisCasNode> nodes = new ArrayList<>();
        private final PeerStateObserver peerState;

        private Cluster(final Path baseDir, final int n1ClusterSize, final int n2ClusterSize)
                throws Exception {
            final Map<NodeId, ListenSocket> sockets = new LinkedHashMap<>();
            final Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
            for (final NodeId id : new NodeId[]{N1, N2}) {
                final ListenSocket socket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
                sockets.put(id, socket);
                addresses.put(id, socket.address());
            }
            // n2 may be told it belongs to a larger cluster than exists; pad its member list with an
            // address nothing listens on, since the transport requires exactly N members.
            final Map<NodeId, InetSocketAddress> n2Addresses = new LinkedHashMap<>(addresses);
            for (int extra = 3; extra <= n2ClusterSize; extra++) {
                n2Addresses.put(NodeId.of("n" + extra), new InetSocketAddress("127.0.0.1", 1));
            }

            peerState = new PeerStateObserver(n1ClusterSize, NodeObserver.NONE);
            nodes.add(node(baseDir, N1, n1ClusterSize, sockets.get(N1), addresses, peerState));
            nodes.add(node(baseDir, N2, n2ClusterSize, sockets.get(N2), n2Addresses,
                    NodeObserver.NONE));
        }

        private static DisCasNode node(final Path baseDir,
                                       final NodeId id,
                                       final int clusterSize,
                                       final ListenSocket socket,
                                       final Map<NodeId, InetSocketAddress> members,
                                       final NodeObserver observer) throws Exception {
            final Path walBase = Files.createDirectories(baseDir.resolve("wal-" + id.value()));
            final FileWal wal = new FileWal(StorageConfig.builder()
                    .baseDirectory(walBase)
                    .walMaxFileBytes(16 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build());
            wal.initialize();
            return DisCasNodeFactory.create(
                    new NodeConfig(id, CLUSTER, clusterSize),
                    new TcpPeerBootstrap(socket, InMemoryMembers.ofTcp(members),
                            TcpTransportConfig.defaults()),
                    wal,
                    observer);
        }

        private void start() {
            for (int i = 0; i < nodes.size(); i++) {
                nodes.get(i).start();
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    nodes.get(i).close();
                } catch (final Exception ignored) {
                }
            }
        }
    }
}
