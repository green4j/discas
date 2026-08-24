/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestCas;
import io.github.green4j.discas.TestAwait;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.TransportProtocol;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("DisCasNode uses distinct TCP ports for client vs peer traffic")
class DisCasNodeDualPortTest {

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    private static final List<NodeId> NODE_IDS = List.of(nid(1), nid(2), nid(3));

    @TempDir
    Path baseDir;

    private final List<DisCasNode> nodes = new ArrayList<>();
    private final List<DisCasClient> clients = new ArrayList<>();
    private final Map<NodeId, InetSocketAddress> peerAddresses = new HashMap<>();
    private final Map<NodeId, InetSocketAddress> clientAddresses = new HashMap<>();

    @BeforeEach
    void setup() throws Exception {
        final Map<NodeId, ListenSocket> peerSockets = new LinkedHashMap<>();
        final TcpTransportConfig peerConfig = TcpTransportConfig.defaults();
        final ClientTransportConfig clientConfig = ClientTransportConfig.defaults();

        // Phase 1: bind every peer listener at :0 and learn the address each one got.
        // Phase 2 (below) hands each node the members map built from those addresses,
        // which is the map every node validates at construction. Nothing is reserved and
        // then bound later, so no port can be taken in between.
        for (final NodeId nodeId : NODE_IDS) {
            final ListenSocket peerSocket =
                    ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            peerSockets.put(nodeId, peerSocket);
            peerAddresses.put(nodeId, peerSocket.address());
        }

        for (final NodeId nodeId : NODE_IDS) {
            final Path walBase = baseDir.resolve("node-" + nodeId);
            Files.createDirectories(walBase);
            final StorageConfig storageConfig = StorageConfig.builder()
                    .baseDirectory(walBase)
                    .walMaxFileBytes(16 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build();
            final FileWal wal = new FileWal(storageConfig);
            wal.initialize();

            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, ClusterId.of("dual-port-cluster"), peerAddresses.size()),
                    new TcpPeerBootstrap(peerSockets.get(nodeId),
                            InMemoryMembers.ofTcp(peerAddresses), peerConfig),
                    wal);

            final TcpClientServerTransport clientServer =
                    DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                            new InetSocketAddress("127.0.0.1", 0), clientConfig));
            clientAddresses.put(nodeId,
                    new InetSocketAddress("127.0.0.1", clientServer.boundPort()));

            nodes.add(node);
        }

        for (final DisCasNode node : nodes) {
            node.start();
        }

        clients.add(DisCasClientFactory.create(
                ClientId.of("dualport-client"),
                new TcpClientBootstrap(clientAddresses, clientConfig)));
    }


    @AfterEach
    void tearDown() {
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
    }

    @Test
    @DisplayName("Client CAS round-trip succeeds over the client port while peer traffic runs on the peer port")
    void clientRoundTripOverClientPort() throws Exception {
        final DisCasClient client = clients.get(0);
        TestAwait.awaitReady(client);

        final ByteBuffer key = TestBytes.utf8("dual-port-key");

        client.put(key, TestBytes.utf8("v1")).get(8, TimeUnit.SECONDS);

        final CasResult result = TestCas.swapValue(
                client, key, TestBytes.utf8("v1"), TestBytes.utf8("v2"), 8_000L);
        assertTrue(result.swapped(), "CAS v1 -> v2 should swap");

        final ByteBuffer got = client.get(key).get(8, TimeUnit.SECONDS).value();
        assertEquals("v2", TestBytes.string(got));
    }

    @Test
    @DisplayName("Peer port rejects a raw CLIENT_HELLO frame (distinct protocol)")
    void peerPortRejectsClientHello() throws Exception {
        final InetSocketAddress peerAddr = peerAddresses.get(nid(1));
        final FrameCodec frameCodec = new FrameCodec(64 * 1024);
        final ByteBuffer helloPayload = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        helloPayload.putInt(TransportProtocol.PROTOCOL_VERSION);
        helloPayload.flip();
        final ByteBuffer wire = frameCodec.encode(FrameCodec.TYPE_CLIENT_HELLO, helloPayload);

        try (Socket socket = new Socket(peerAddr.getAddress(), peerAddr.getPort())) {
            socket.setSoTimeout(5_000);
            final byte[] raw = new byte[wire.remaining()];
            wire.get(raw);
            socket.getOutputStream().write(raw);
            socket.getOutputStream().flush();

            final InputStream in = socket.getInputStream();
            final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deadline) {
                final int b = in.read();
                if (b < 0) {
                    return; // peer closed the connection -- expected
                }
            }
            fail("Peer port did not close connection after CLIENT_HELLO");
        }
    }



}
