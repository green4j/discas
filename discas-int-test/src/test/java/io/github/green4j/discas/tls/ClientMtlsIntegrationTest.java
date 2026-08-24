/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls;

import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.auth.AllowAllClientAuthenticator;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.acl.ClientOp;
import io.github.green4j.discas.node.acl.InMemoryClientAcl;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Client mTLS end-to-end -- CN=clientId authentication + prefix authorization")
class ClientMtlsIntegrationTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClusterId CLUSTER = ClusterId.of("acl-cluster");
    private static final ClientId WEB = ClientId.of("web-1");

    @TempDir
    Path baseDir;

    private DisCasNode node;
    private DisCasClient client;

    /** Start a single-node cluster whose client port requires mTLS, plus a client that
     * presents a certificate with {@code CN=certCn} and claims {@code helloId} in its hello. */
    private void start(final ClientId helloId, final String certCn) throws Exception {
        // A one-node cluster still has to name itself in its own members map, so bind the
        // peer listener first and build the map from the address it got.
        final ListenSocket peerSocket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        final InetSocketAddress peerAddr = peerSocket.address();

        final TcpTransportConfig peerConfig = TcpTransportConfig.defaults();
        final ClientTransportConfig clientConfig = ClientTransportConfig.defaults();

        final Path walBase = baseDir.resolve("node-1");
        Files.createDirectories(walBase);
        final FileWal wal = new FileWal(StorageConfig.builder()
                .baseDirectory(walBase)
                .walMaxFileBytes(16 * 1024 * 1024).snapshotRetentionCount(2).build());
        wal.initialize();

        node = DisCasNodeFactory.create(
                new NodeConfig(NODE, CLUSTER, 1),
                new TcpPeerBootstrap(peerSocket, InMemoryMembers.ofTcp(Map.of(NODE, peerAddr)), peerConfig),
                wal);

        final Path caDir = baseDir.resolve("ca");
        Files.createDirectories(caDir);
        final TestCa ca = new TestCa(caDir, CLUSTER);
        // mTLS on the client port: the cert CN is the authoritative identity (the default
        // AllowAll authenticator simply accepts it after the transport's CN cross-check).
        final ClientSecurityProvider serverSecurity = ca.clientProvider("node-1");
        final TcpClientServerTransport clientServer =
                DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                        new InetSocketAddress("127.0.0.1", 0), clientConfig, AllowAllClientAuthenticator.INSTANCE,
                                serverSecurity));
        final InetSocketAddress clientAddr =
                new InetSocketAddress("127.0.0.1", clientServer.boundPort());
        node.registerClientAcl(InMemoryClientAcl.builder()
                .grant(WEB, "app/", ClientOp.GET, ClientOp.PUT)
                .build());
        node.start();

        final ClientSecurityProvider clientSecurity = ca.clientProvider(certCn);
        client = DisCasClientFactory.create(helloId,
                new TcpClientBootstrap(Map.of(NODE, clientAddr), clientConfig, null, clientSecurity));
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            try {
                client.close();
            } catch (final Exception ignored) {
            }
        }
        if (node != null) {
            try {
                node.close();
            } catch (final Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("A client whose cert CN matches its claimed id is authenticated and authorized")
    void matchingCnAuthenticatesAndAuthorizes() throws Exception {
        start(WEB, "web-1");
        TestAwait.awaitReady(client, "app/__ready__");

        client.put(TestBytes.utf8("app/user/1"), TestBytes.utf8("alice")).get(8, TimeUnit.SECONDS);
        final ByteBuffer got = client.get(TestBytes.utf8("app/user/1")).get(8, TimeUnit.SECONDS).value();
        assertEquals("alice", TestBytes.string(got));
    }

    @Test
    @DisplayName("A hello id that does not match the cert CN is denied -- the client cannot operate")
    void mismatchedCnDenied() throws Exception {
        // Cert CN is web-1 but the client claims web-2 in its hello -> ACCESS_DENIED at hello.
        start(ClientId.of("web-2"), "web-1");

        assertThrows(Exception.class,
                () -> client.get(TestBytes.utf8("app/user/1")).get(10, TimeUnit.SECONDS));
    }




}
