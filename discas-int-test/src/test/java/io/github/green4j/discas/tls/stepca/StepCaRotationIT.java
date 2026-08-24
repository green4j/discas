/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls.stepca;

import io.github.green4j.discas.client.CasResult;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.chaos.TestProfile;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestAwait;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.Version;
import io.github.green4j.discas.common.client.ReadConsistency;
import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsPeerSecurityProvider;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * CA-rotation against a <b>real external CA</b> (dockerized smallstep step-ca).
 * A 3-node mTLS cluster runs on step-ca-issued certs, then every node's cert is
 * renewed from the live CA and hot-swapped; the cluster keeps serving.
 * <p>
 * Docker-only and slow -- gated behind {@code -Ddiscas.test.profile=long} so the
 * default suite stays Docker-free.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@Tag("chaos")
@DisplayName("CA rotation against dockerized step-ca")
class StepCaRotationIT {

    @BeforeEach
    void requireLongProfile() {
        // TestProfile, not @EnabledIfSystemProperty: that annotation matches the raw property
        // case-sensitively, so -Ddiscas.test.profile=LONG ran every other long-profile test and
        // silently skipped this one.
        assumeTrue(TestProfile.current().atLeast(TestProfile.LONG),
                "step-ca: long profile only (Docker required)");
    }

    private static final ClusterId CLUSTER = ClusterId.of("stepca-cluster");
    private static final List<NodeId> NODE_IDS =
            List.of(NodeId.of("sca-1"), NodeId.of("sca-2"), NodeId.of("sca-3"));

    private final List<DisCasNode> nodes = new ArrayList<>();
    private final List<DisCasClient> clients = new ArrayList<>();
    private StepCa ca;

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
        for (final NodeId nodeId : NODE_IDS) {
            InProcessClientRegistry.unregister(nodeId);
        }
        if (ca != null) {
            ca.close();
        }
    }

    @Test
    @DisplayName("Cluster serves through a live rotation of step-ca-issued certs")
    void clusterServesThroughStepCaRotation(@TempDir final Path baseDir) throws Exception {
        ca = new StepCa(CLUSTER);
        ca.start();

        final TcpTransportConfig peerConfig = TcpTransportConfig.defaults();
        final Map<NodeId, InetSocketAddress> peerAddresses = new LinkedHashMap<>();
        final Map<NodeId, ListenSocket> peerSockets = new LinkedHashMap<>();
        for (final NodeId nodeId : NODE_IDS) {
            // Bind first, then build the members map from the addresses actually bound.
            final ListenSocket peerSocket =
                    ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            peerSockets.put(nodeId, peerSocket);
            peerAddresses.put(nodeId, peerSocket.address());
        }

        final Map<NodeId, ReloadableTlsContext> contexts = new LinkedHashMap<>();
        for (final NodeId nodeId : NODE_IDS) {
            final ReloadableTlsContext ctx = ReloadableTlsContext.create(ca.issue(nodeId));
            contexts.put(nodeId, ctx);
            final PeerSecurityProvider tls =
                    new TlsPeerSecurityProvider(TlsConfig.of(ctx.sslContext()));

            final Path walBase = Files.createDirectories(baseDir.resolve("wal-" + nodeId.value()));
            final StorageConfig storageConfig = StorageConfig.builder()
                    .baseDirectory(walBase)
                    .walMaxFileBytes(16 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build();
            final FileWal wal = new FileWal(storageConfig);
            wal.initialize();

            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, CLUSTER, peerAddresses.size()),
                    new TcpPeerBootstrap(peerSockets.get(nodeId),
                            InMemoryMembers.ofTcp(peerAddresses), peerConfig, tls),
                    wal);
            node.registerClientMessages(registrar ->
                    InProcessClientRegistry.register(nodeId, node.loop(), registrar,
                            node.clusterSize()));
            nodes.add(node);
        }
        for (final DisCasNode node : nodes) {
            node.start();
        }

        final EventLoop clientLoop = new EventLoop("stepca-client");
        final DisCasClient client = new DisCasClient(
                ClientId.of("stepca-client"),
                new InProcessClientTransport(clientLoop, NODE_IDS, ClientId.of("stepca-client")), clientLoop);
        clients.add(client);
        TestAwait.awaitReady(client);

        final ByteBuffer key = TestBytes.utf8("stepca-key");
        client.put(key.duplicate(), TestBytes.utf8("v1")).get(8, TimeUnit.SECONDS);

        // Renew every node's cert from the live step-ca and hot-swap it.
        for (final NodeId nodeId : NODE_IDS) {
            contexts.get(nodeId).reload(ca.issue(nodeId));
        }

        assertEquals("v1", TestBytes.string(client.get(key.duplicate()).get(5, TimeUnit.SECONDS).value()));
        final Version atV1 = client
                .get(key.duplicate(), ReadConsistency.LINEARIZABLE)
                .get(8, TimeUnit.SECONDS)
                .version();
        final CasResult cas =
                client.cas(key.duplicate(), atV1, TestBytes.utf8("v2")).get(8, TimeUnit.SECONDS);
        assertEquals(true, cas.swapped(), "Consensus still works after step-ca rotation");
        assertEquals("v2", TestBytes.string(client.get(key.duplicate()).get(5, TimeUnit.SECONDS).value()));
    }




}
