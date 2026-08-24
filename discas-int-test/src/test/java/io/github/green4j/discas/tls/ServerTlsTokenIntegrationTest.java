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
import io.github.green4j.discas.common.client.auth.InMemoryClientTokenStore;
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.TlsClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsContexts;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Server-TLS + token -- encrypted channel, token identity (no client cert)")
class ServerTlsTokenIntegrationTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClusterId CLUSTER = ClusterId.of("acl-cluster");
    private static final ClientId WEB = ClientId.of("web-1");
    private static final String TOKEN = "s3cret";

    @TempDir
    Path baseDir;

    private DisCasNode node;
    private DisCasClient client;

    private void start(final String clientToken) throws Exception {
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

        // Server presents a cert (CN=node-1) but does NOT require a client cert; identity
        // comes from the token. The channel is encrypted either way.
        final ClientSecurityProvider serverSecurity = TlsClientSecurityProvider.serverAuthOnly(
                TlsConfig.of(ca.serverContext("node-1")));
        final TokenClientAuthenticator authenticator = new TokenClientAuthenticator(
                InMemoryClientTokenStore.builder().add(WEB, TOKEN, Long.MAX_VALUE).build());

        final TcpClientServerTransport clientServer =
                DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                        new InetSocketAddress("127.0.0.1", 0), clientConfig, authenticator, serverSecurity));
        final InetSocketAddress clientAddr =
                new InetSocketAddress("127.0.0.1", clientServer.boundPort());
        node.registerClientAcl(InMemoryClientAcl.builder()
                .grant(WEB, "app/", ClientOp.GET, ClientOp.PUT)
                .build());
        node.start();

        // Client verifies the server cert (trust-only, presents no cert) and authenticates
        // with a token over the encrypted channel.
        final ClientSecurityProvider clientSecurity = new TlsClientSecurityProvider(
                TlsConfig.of(TlsContexts.buildTrustOnly(ca.load(ca.trustStore()))));
        client = DisCasClientFactory.create(WEB,
                new TcpClientBootstrap(Map.of(NODE, clientAddr), clientConfig, clientToken, clientSecurity));
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
    @DisplayName("A correct token over server-TLS is authenticated and authorized")
    void correctTokenOverTls() throws Exception {
        start(TOKEN);
        TestAwait.awaitReady(client, "app/__ready__");

        client.put(TestBytes.utf8("app/user/1"), TestBytes.utf8("alice")).get(8, TimeUnit.SECONDS);
        assertEquals("alice", TestBytes.string(client.get(TestBytes.utf8("app/user/1")).get(8,
                TimeUnit.SECONDS).value()));
    }

    @Test
    @DisplayName("A wrong token over server-TLS is denied -- the client cannot operate")
    void wrongTokenOverTls() throws Exception {
        start("nope");
        assertThrows(Exception.class,
                () -> client.get(TestBytes.utf8("app/user/1")).get(10, TimeUnit.SECONDS));
    }




}
