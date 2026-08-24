/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.performance;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A cluster to take numbers against: {@code N} real nodes over the TCP peer transport with a
 * {@link FileWal} each, clients over the TCP client transport, and an optional {@link DelayLink}
 * per peer pair standing in for an inter-region hop.
 *
 * <p>Everything is one JVM, which is a condition of every number this produces rather than a
 * shortcut: nodes share a CPU and a page cache, so the figures describe the protocol's shape on a
 * fast link, not the hardware a deployment would give it.
 *
 * <p>Peer listeners are bound first and the member map is built from the addresses they actually
 * got, so no port is chosen and bound later -- the one place a benchmark could otherwise fail for
 * reasons that have nothing to do with what it measures. The delay is injected on the peer mesh
 * only, in the single-dialer shape the transport uses (lower id dials higher), because that is what
 * a consensus round crosses; the client-to-coordinator hop stays local, matching a deployment whose
 * client sits in a region with one of its nodes.
 */
final class BenchCluster implements AutoCloseable {

    private final int clusterSize;
    private final List<DisCasNode> nodes = new ArrayList<>();
    private final List<DisCasClient> clients = new ArrayList<>();
    private final List<DelayLink> links = new ArrayList<>();

    BenchCluster(final int clusterSize, final int clientCount, final long linkDelayMillis,
                 final Path rootDir) throws Exception {
        this.clusterSize = clusterSize;

        final Map<Integer, ListenSocket> peerSockets = new LinkedHashMap<>();
        final Map<Integer, InetSocketAddress> peerAddresses = new LinkedHashMap<>();
        for (int id = 1; id <= clusterSize; id++) {
            final ListenSocket socket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            peerSockets.put(id, socket);
            peerAddresses.put(id, socket.address());
        }

        // One delaying hop per single-dialer pair, in front of the higher id's real port.
        final Map<Long, DelayLink> byPair = new HashMap<>();
        if (linkDelayMillis > 0) {
            for (int low = 1; low <= clusterSize; low++) {
                for (int high = low + 1; high <= clusterSize; high++) {
                    final DelayLink link = new DelayLink(peerAddresses.get(high), linkDelayMillis);
                    link.start();
                    byPair.put(pairKey(low, high), link);
                    links.add(link);
                }
            }
        }

        final Map<NodeId, InetSocketAddress> clientAddresses = new HashMap<>();
        for (int id = 1; id <= clusterSize; id++) {
            final Path walBase = rootDir.resolve("node-" + id);
            Files.createDirectories(walBase);
            final FileWal wal = new FileWal(StorageConfig.builder()
                    .baseDirectory(walBase)
                    .walMaxFileBytes(64 * 1024 * 1024)
                    .build());
            wal.initialize();

            final Map<NodeId, InetSocketAddress> members = new HashMap<>();
            for (int peer = 1; peer <= clusterSize; peer++) {
                final DelayLink link = peer > id ? byPair.get(pairKey(id, peer)) : null;
                members.put(nid(peer),
                        link != null ? link.listenAddress() : peerAddresses.get(peer));
            }

            final DisCasNode node = DisCasNodeFactory.create(
                    NodeConfig.builder()
                            .nodeId(nid(id))
                            .clusterId(ClusterId.of("bench-cluster"))
                            .clusterSize(clusterSize)
                            .build(),
                    new TcpPeerBootstrap(peerSockets.get(id),
                            InMemoryMembers.ofTcp(members), TcpTransportConfig.defaults()),
                    wal);
            final TcpClientServerTransport clientServer =
                    DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                            new InetSocketAddress("127.0.0.1", 0), ClientTransportConfig.defaults()));
            clientAddresses.put(nid(id), new InetSocketAddress("127.0.0.1", clientServer.boundPort()));
            nodes.add(node);
        }
        for (final DisCasNode node : nodes) {
            node.start();
        }

        for (int i = 0; i < clientCount; i++) {
            clients.add(DisCasClientFactory.create(ClientId.of("bench-client-" + i),
                    new TcpClientBootstrap(clientAddresses, ClientTransportConfig.defaults())));
        }
        for (final DisCasClient client : clients) {
            awaitReady(client);
        }
    }

    DisCasClient client(final int index) {
        return clients.get(index);
    }

    /** Frames carried by every delaying link, both directions -- how much the mesh actually said. */
    long linkCrossings() {
        long total = 0;
        for (final DelayLink link : links) {
            total += link.crossings();
        }
        return total;
    }

    int clusterSize() {
        return clusterSize;
    }

    /**
     * Wait until the cluster answers a read. {@code node.start()} returns when the node is up, not
     * when the peer mesh has formed, so a benchmark that started measuring immediately would put
     * the mesh's formation into its first figures.
     */
    private static void awaitReady(final DisCasClient client) throws Exception {
        final ByteBuffer probe = ByteBuffer.wrap("__bench_ready__".getBytes(StandardCharsets.UTF_8));
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        Exception last = null;
        while (System.nanoTime() - deadline < 0) {
            try {
                client.get(probe.duplicate()).get(2, TimeUnit.SECONDS);
                return;
            } catch (final Exception notYet) {
                last = notYet;
                Thread.sleep(50L);
            }
        }
        throw new IllegalStateException("Cluster did not answer a read within 60s", last);
    }

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    private static long pairKey(final int a, final int b) {
        return ((long) Math.min(a, b) << 32) | (Math.max(a, b) & 0xFFFFFFFFL);
    }

    @Override
    public void close() {
        for (final DisCasClient client : clients) {
            closeQuietly(client);
        }
        for (final DisCasNode node : nodes) {
            closeQuietly(node);
        }
        for (final DelayLink link : links) {
            closeQuietly(link);
        }
    }

    private static void closeQuietly(final AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (final Exception ignored) {
            // teardown
        }
    }
}
