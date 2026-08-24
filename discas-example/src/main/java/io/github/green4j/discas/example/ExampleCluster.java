/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.example;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.transport.InProcessClientBootstrap;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.wal.Wal;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class ExampleCluster {
    private static final ClusterId EXAMPLE_CLUSTER = ClusterId.of("example-cluster");

    private ExampleCluster() {
    }

    static final class RunningCluster implements AutoCloseable {
        final List<DisCasNode> nodes = new ArrayList<>();
        final List<DisCasClient> clients = new ArrayList<>();
        final List<NodeId> nodeIds;
        final List<Path> walDirs = new ArrayList<>();
        final boolean unregisterClientIngressOnClose;

        RunningCluster(final List<NodeId> nodeIds,
                       final boolean unregisterClientIngressOnClose) {
            this.nodeIds = nodeIds;
            this.unregisterClientIngressOnClose = unregisterClientIngressOnClose;
        }

        DisCasClient client(final int index) {
            return clients.get(index);
        }

        @Override
        public void close() {
            for (final DisCasClient client : clients) {
                try {
                    client.close();
                } catch (final Exception ignored) {
                }
            }
            for (final DisCasNode node : nodes) {
                try {
                    node.close();
                } catch (final Exception ignored) {
                }
            }
            if (unregisterClientIngressOnClose) {
                for (final NodeId nodeId : nodeIds) {
                    InProcessClientRegistry.unregister(nodeId);
                }
            }
        }
    }

    static RunningCluster startInProcessCluster(final int clientCount) throws Exception {
        final List<NodeId> nodeIds = List.of(
                NodeId.of("1"),
                NodeId.of("2"),
                NodeId.of("3")
        );
        final RunningCluster cluster = new RunningCluster(nodeIds, false);

        for (final NodeId nodeId : nodeIds) {
            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, EXAMPLE_CLUSTER, nodeIds.size()),
                    new InProcessPeerBootstrap(InMemoryMembers.ofNodes(nodeIds)),
                    new NoopWal());
            cluster.nodes.add(node);
        }

        for (int i = 0; i < clientCount; i++) {
            cluster.clients.add(DisCasClientFactory.create(
                    ClientId.of("client-" + i),
                    new InProcessClientBootstrap(nodeIds)));
        }

        for (final DisCasNode node : cluster.nodes) {
            node.start();
        }
        awaitReady(cluster);
        return cluster;
    }

    static RunningCluster startTcpPeersWithFileWalCluster(final int clientCount, final Path baseDir)
            throws Exception {
        final List<NodeId> nodeIds = List.of(
                NodeId.of("1"),
                NodeId.of("2"),
                NodeId.of("3")
        );
        final TcpTransportConfig peerTransportConfig = defaultTcpTransportConfig();
        final ClientTransportConfig clientTransportConfig = defaultClientTransportConfig();
        final RunningCluster cluster = new RunningCluster(nodeIds, true);

        final Map<NodeId, InetSocketAddress> clusterAddresses = new HashMap<>();
        final Map<NodeId, InetSocketAddress> clientAddresses = new HashMap<>();
        // Peer addresses have to exist before any node does -- every member's address is in the
        // members map each node validates at construction. Client addresses do not: each client
        // server binds :0 below and the map is filled from the port it actually got.
        for (final NodeId nodeId : nodeIds) {
            clusterAddresses.put(nodeId, new InetSocketAddress("127.0.0.1", freePort()));
        }

        for (final NodeId nodeId : nodeIds) {
            final Path walBase = baseDir.resolve("node-" + nodeId);
            Files.createDirectories(walBase);
            final StorageConfig storageConfig = StorageConfig.builder()
                    .baseDirectory(walBase)
                    .walMaxFileBytes(16 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build();
            final FileWal fileWal = new FileWal(storageConfig);
            fileWal.initialize();
            cluster.walDirs.add(storageConfig.baseDirectory());

            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, EXAMPLE_CLUSTER, clusterAddresses.size()),
                    new TcpPeerBootstrap(
                            clusterAddresses.get(nodeId),
                            InMemoryMembers.ofTcp(clusterAddresses),
                            peerTransportConfig),
                    fileWal);

            // Symmetric with the client side below: a bootstrap handed whole to a factory.
            final TcpClientServerTransport clientServer =
                    DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                            new InetSocketAddress("127.0.0.1", 0), clientTransportConfig));
            clientAddresses.put(nodeId,
                    new InetSocketAddress("127.0.0.1", clientServer.boundPort()));

            cluster.nodes.add(node);
        }

        for (int i = 0; i < clientCount; i++) {
            cluster.clients.add(DisCasClientFactory.create(
                    ClientId.of("client-" + i),
                    new TcpClientBootstrap(clientAddresses, clientTransportConfig)));
        }

        for (final DisCasNode node : cluster.nodes) {
            node.start();
        }
        awaitReady(cluster);
        return cluster;
    }

    /**
     * Block until the cluster answers a read, and only then hand it to an example.
     * <p>
     * {@code node.start()} returns as soon as the node is up, not as soon as the peer mesh has
     * formed -- so the first operation of an example could arrive at a coordinator that cannot yet
     * see a majority, and be refused or time out through no fault of the example. That is not
     * hypothetical: {@code ContentionExample} failed exactly there, on its seed write, on a loaded
     * machine.
     * <p>
     * It is also the honest thing to show a reader: an application that starts a cluster and
     * immediately writes to it has to be prepared for the same answer, and waiting for a probe read
     * is how. The budget is generous because it covers a slow machine, and the failure carries the
     * last error rather than an empty timeout.
     */
    private static void awaitReady(final RunningCluster cluster) throws Exception {
        if (cluster.clients.isEmpty()) {
            return;
        }
        final DisCasClient client = cluster.clients.get(0);
        final ByteBuffer probe = ExampleBytes.encode("__example_ready__");
        final long deadline = System.nanoTime() + READY_BUDGET_MS * 1_000_000L;
        Exception last = null;
        while (System.nanoTime() - deadline < 0) {
            try {
                client.get(probe.duplicate()).get(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return;
            } catch (final Exception notYet) {
                last = notYet;
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
        throw new IllegalStateException(
                "Cluster did not answer a read within " + READY_BUDGET_MS + "ms", last);
    }

    /** How long a freshly started cluster gets to answer its first read. */
    private static final long READY_BUDGET_MS = 60_000L;
    /** Per-probe timeout; a probe that is still waiting says nothing a retry would not say. */
    private static final long PROBE_TIMEOUT_MS = 2_000L;
    /** Gap between probes. Readiness is usually one retry away. */
    private static final long POLL_INTERVAL_MS = 100L;

    private static TcpTransportConfig defaultTcpTransportConfig() {
        // maxQueuedOutBytes / maxInflightBytes must hold a max-size value-carrying peer
        // message (see PeerMessageCodec.MAX_MESSAGE_BYTES, derived from KvLimits).
        return TcpTransportConfig.defaults();
    }

    private static ClientTransportConfig defaultClientTransportConfig() {
        // maxQueuedOutBytes / maxInflightBytes must hold a max-size client message
        // (see ClientMessageCodec.MAX_MESSAGE_BYTES, derived from KvLimits).
        return ClientTransportConfig.defaults();
    }

    private static int freePort() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    private static final class NoopWal implements Wal {
        private final IncarnationId incarnation = IncarnationId.generate();

        @Override
        public IncarnationId incarnation() {
            return incarnation;
        }

        /** A log that stores nothing can prove nothing, which is the honest answer. */
        @Override
        public boolean ceilingIsProven() {
            return false;
        }

        @Override
        public boolean append(final Entry entry) {
            return true;
        }

        @Override
        public SnapshotReader openSnapshot() {
            return null;
        }

        @Override
        public void replayTail(final Consumer<Entry> consumer) {
        }

        @Override
        public SnapshotWriter beginSnapshot() {
            return new SnapshotWriter() {
                @Override
                public void write(final SnapshotEntry entry) {
                }

                @Override
                public void commit(final Wal.Reservations reservations) {
                }

                @Override
                public void abort() {
                }
            };
        }

        @Override
        public void truncateBeforeSnapshot() {
        }
    }
}
