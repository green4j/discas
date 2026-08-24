/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.admin;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.admin.runbook.ClusterPlan;
import io.github.green4j.discas.admin.runbook.Runbook;
import io.github.green4j.discas.TestCluster;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.dump.ClusterDump;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The end-to-end case: dump a cluster of three, build five members' directories from it, start
 * them, and read the pairs back out of a cluster that has never been written to.
 */
@DisplayName("A cluster seeded from a dump -- five members that have never met the cluster it came from")
class SeededClusterTest {

    private static final int SEEDED_MEMBERS = 5;
    private static final long TIMEOUT_MS = 20_000L;

    @TempDir
    Path directory;

    private final List<DisCasNode> nodes = new ArrayList<>();
    private final List<ListenSocket> peerSockets = new ArrayList<>();
    private final List<DisCasClient> clients = new ArrayList<>();
    private TestCluster source;

    @AfterEach
    void tearDown() {
        for (final DisCasClient client : clients) {
            client.close();
        }
        for (final DisCasNode node : nodes) {
            node.close();
        }
        if (source != null) {
            source.close();
        }
    }

    @Test
    @DisplayName("Every pair is on every member, and what was deleted did not travel")
    void seedsAWholeClusterFromADump() throws Exception {
        final Path dump = dumpOfAThreeMemberCluster();

        final Map<NodeId, InetSocketAddress> members = bindMembers();
        final Path clusterDirectory = directory.resolve("new-cluster");
        final ClusterPlan plan = ClusterPlan.builder()
                .clusterId(ClusterId.of("seeded-cluster"))
                .members(members)
                .dump(dump)
                .build();
        final InitOperation.InitSummary summary = InitOperation.run(clusterDirectory, plan, null);

        assertEquals(SEEDED_MEMBERS, summary.members());
        assertEquals(3, summary.pairsPerMember());
        assertTrue(Files.size(clusterDirectory.resolve(Runbook.FILE_NAME)) > 0,
                "RUN.md was not written");
        assertTrue(Files.size(clusterDirectory.resolve(Runbook.MEMBERS_FILE_NAME)) > 0,
                "members.conf was not written");

        final List<InetSocketAddress> clientPorts = startSeededCluster(clusterDirectory, members);

        // Read from each member's own storage rather than through a round: a serializable read is
        // answered locally, so it can only succeed if that member's directory carries the pairs --
        // which is the claim, as against "a quorum repaired the ones that did not".
        for (int i = 0; i < clientPorts.size(); i++) {
            final DisCasClient local = clientOf(members, clientPorts, i);
            assertEquals("alice", read(local, "users/1"), "member " + (i + 1) + " is missing a pair");
            assertEquals("bob", read(local, "users/2"), "member " + (i + 1) + " is missing a pair");
            assertEquals("widget", read(local, "orders/9"),
                    "member " + (i + 1) + " is missing a pair");
            assertNull(read(local, "users/3"), "A deleted key was seeded into member " + (i + 1));
        }
    }

    /** A three-member cluster holding three live pairs and one deleted key, dumped to a file. */
    private Path dumpOfAThreeMemberCluster() throws Exception {
        source = new TestCluster(3);
        source.start();
        source.awaitReady();
        final DisCasClient client = source.client(0);

        client.put("users/1", TestBytes.utf8("alice")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        client.put("users/2", TestBytes.utf8("bob")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        client.put("orders/9", TestBytes.utf8("widget")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        client.put("users/3", TestBytes.utf8("gone")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        client.delete("users/3").get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        final Path dump = directory.resolve("source.dump");
        try (FileChannel channel = FileChannel.open(dump,
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            ClusterDump.dump(client, Collections.emptyList(), channel)
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        return dump;
    }

    private Map<NodeId, InetSocketAddress> bindMembers() throws IOException {
        final Map<NodeId, InetSocketAddress> members = new LinkedHashMap<>();
        for (int i = 1; i <= SEEDED_MEMBERS; i++) {
            final ListenSocket socket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            peerSockets.add(socket);
            members.put(NodeId.of(Integer.toString(i)), socket.address());
        }
        return members;
    }

    /** @return each member's client address, in membership order */
    private List<InetSocketAddress> startSeededCluster(final Path clusterDirectory,
                                                       final Map<NodeId, InetSocketAddress> members)
            throws Exception {
        final ClusterId clusterId = ClusterId.of("seeded-cluster");
        final List<InetSocketAddress> clientPorts = new ArrayList<>();
        int index = 0;
        for (final NodeId nodeId : members.keySet()) {
            final FileWal wal = new FileWal(StorageConfig.builder()
                    .baseDirectory(clusterDirectory.resolve(nodeId.value()))
                    .build());
            wal.initialize();

            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, clusterId, members.size()),
                    new TcpPeerBootstrap(peerSockets.get(index),
                            InMemoryMembers.ofTcp(members), TcpTransportConfig.defaults()),
                    wal);
            final TcpClientServerTransport clientServer = DisCasNodeFactory.createClientServer(
                    node, new TcpClientServerBootstrap(
                            new InetSocketAddress("127.0.0.1", 0), ClientTransportConfig.defaults()));
            clientPorts.add(new InetSocketAddress("127.0.0.1", clientServer.boundPort()));
            nodes.add(node);
            index++;
        }
        for (final DisCasNode node : nodes) {
            node.start();
        }
        return clientPorts;
    }

    /** A client that knows one member, so a serializable read is answered by that member. */
    private DisCasClient clientOf(final Map<NodeId, InetSocketAddress> members,
                                  final List<InetSocketAddress> clientPorts,
                                  final int index) {
        final List<NodeId> ids = new ArrayList<>(members.keySet());
        final Map<NodeId, InetSocketAddress> one =
                Collections.singletonMap(ids.get(index), clientPorts.get(index));
        final DisCasClient client = DisCasClientFactory.create(
                ClientId.of("verify-" + ids.get(index).value()),
                new TcpClientBootstrap(one, ClientTransportConfig.defaults()),
                DisCasClientConfig.defaults());
        clients.add(client);
        return client;
    }

    private static String read(final DisCasClient client, final String key) throws Exception {
        return TestBytes.string(client.get(key, ReadConsistency.SERIALIZABLE)
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS).value());
    }
}
