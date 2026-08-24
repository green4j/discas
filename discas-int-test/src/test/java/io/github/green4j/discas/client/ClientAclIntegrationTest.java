/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.client.ClientErrorCode;
import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.auth.InMemoryClientTokenStore;
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Client ACL end-to-end -- token auth + default-deny prefix authorization")
class ClientAclIntegrationTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClientId WEB = ClientId.of("web-1");
    private static final String TOKEN = "s3cret";

    @TempDir
    Path baseDir;

    private DisCasNode node;
    private DisCasClient client;

    @BeforeEach
    void setup() throws Exception {
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
                new NodeConfig(NODE, ClusterId.of("acl-cluster"), 1),
                new TcpPeerBootstrap(peerSocket,
                        InMemoryMembers.ofTcp(Map.of(NODE, peerAddr)), peerConfig),
                wal);

        // Token authentication: only web-1 with the provisioned token may connect.
        final TokenClientAuthenticator authenticator = new TokenClientAuthenticator(
                InMemoryClientTokenStore.builder().add(WEB, TOKEN, Long.MAX_VALUE).build());
        final TcpClientServerTransport clientServer =
                DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                        new InetSocketAddress("127.0.0.1", 0), clientConfig, authenticator));
        final InetSocketAddress clientAddr =
                new InetSocketAddress("127.0.0.1", clientServer.boundPort());

        // Authorization: web-1 may GET/PUT/CAS under app/, and may only read under audit/.
        // The read-only prefix is what makes a refused write observable: a client that may not
        // write there and may not read there either could only report "denied" twice, which says
        // nothing about whether the store changed.
        node.registerClientAcl(InMemoryClientAcl.builder()
                .grant(WEB, "app/", ClientOp.GET, ClientOp.PUT, ClientOp.CAS)
                .grant(WEB, "audit/", ClientOp.GET)
                .build());

        node.start();

        client = DisCasClientFactory.create(
                WEB, new TcpClientBootstrap(Map.of(NODE, clientAddr), clientConfig, TOKEN));
        TestAwait.awaitReady(client, "app/__ready__");
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
    @DisplayName("An authorized prefix op succeeds; an out-of-prefix op is denied")
    void authorizedSucceedsUnauthorizedDenied() throws Exception {
        client.put(TestBytes.utf8("app/user/1"), TestBytes.utf8("alice")).get(8, TimeUnit.SECONDS);
        final ByteBuffer got = client.get(TestBytes.utf8("app/user/1")).get(8, TimeUnit.SECONDS).value();
        assertEquals("alice", TestBytes.string(got));

        final ExecutionException denied = assertThrows(ExecutionException.class,
                () -> client.put(TestBytes.utf8("other/secret"), TestBytes.utf8("x")).get(8, TimeUnit.SECONDS));
        assertEquals(ClientErrorCode.ACCESS_DENIED,
                assertInstanceOf(DisCasOperationException.class, denied.getCause()).code(),
                "Out-of-prefix write must be denied");
    }

    /**
     * A denial is a refusal to <em>apply</em>: authorization runs at ingress, before the round, so
     * a denied request leaves the store exactly as it was. Asserting only what the caller was told
     * would pass against a node that refused the client and ran the round anyway.
     */
    @Test
    @DisplayName("A denied write leaves the store as it was")
    void deniedWriteLeavesTheStoreUnchanged() throws Exception {
        // Absent to begin with, through the same read the assertion below uses -- otherwise a read
        // that always answered null would satisfy this test without the store being involved.
        assertNull(client.get(TestBytes.utf8("audit/trail")).get(8, TimeUnit.SECONDS).value(),
                "The key must start absent, or the assertion below proves nothing");

        final ExecutionException denied = assertThrows(ExecutionException.class,
                () -> client.put(TestBytes.utf8("audit/trail"), TestBytes.utf8("forged"))
                        .get(8, TimeUnit.SECONDS));
        assertEquals(ClientErrorCode.ACCESS_DENIED,
                assertInstanceOf(DisCasOperationException.class, denied.getCause()).code(),
                "A write to a read-only prefix must be denied");

        assertNull(client.get(TestBytes.utf8("audit/trail")).get(8, TimeUnit.SECONDS).value(),
                "The denied write must not have reached the store");
    }
}
