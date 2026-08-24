/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.agent.starter.DisCasAgentConfig;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.auth.AllowAllClientAuthenticator;
import io.github.green4j.discas.common.client.auth.ClientAuthenticator;
import io.github.green4j.discas.common.client.auth.InMemoryClientTokenStore;
import io.github.green4j.discas.common.client.auth.TokenClientAuthenticator;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.TlsClientSecurityProvider;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
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
import io.github.green4j.discas.tls.TestCa;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test that a {@link DisCasAgent} can reach a node whose client port is protected with
 * TLS, in both modes the {@code DisCasClient} supports: <b>mTLS</b> (the agent presents a client
 * cert with {@code CN=client-id}) and <b>server-authenticated TLS + token</b> (encrypted channel,
 * identity from the token).
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Agent -- TLS and mTLS to the cluster")
class AgentTlsIntegrationTest {

    private static final ClusterId CLUSTER = ClusterId.of("agent-tls-cluster");
    private static final NodeId NODE = NodeId.of("1");
    private static final ClientId CLIENT = ClientId.of("web-1");
    private static final String PASS = "changeit";
    private static final String TOKEN = "s3cret-api-key";

    private final TcpTransportConfig peerConfig = TcpTransportConfig.defaults();
    private final ClientTransportConfig clientConfig = ClientTransportConfig.defaults();
    private final HttpClient http = HttpClient.newHttpClient();

    @TempDir
    Path baseDir;

    private DisCasNode node;
    private DisCasAgent agent;
    private String baseUrl;

    @AfterEach
    void tearDown() {
        if (agent != null) {
            agent.close();
        }
        if (node != null) {
            try {
                node.close();
            } catch (final Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("Agent authenticates to a node's client port with mTLS (cert CN=client-id)")
    void mtlsRoundTrip() throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(baseDir.resolve("ca")), CLUSTER);
        // mTLS on the node client port: the cert CN is the authoritative identity.
        final InetSocketAddress clientAddr = startNode(
                ca.clientProvider("node-1"), AllowAllClientAuthenticator.INSTANCE);

        final Path keystore = ca.nodeStore(NodeId.of(CLIENT.value())); // CN=web-1
        final Path truststore = ca.trustStore();
        agent = startAgent(
                "--client-id", CLIENT.value(),
                "--nodes", "1=" + hostPort(clientAddr),
                "--http-bind", "127.0.0.1:0",
                "--tls",
                "--tls-keystore", keystore.toString(), "--tls-keystore-password", PASS,
                "--tls-truststore", truststore.toString(), "--tls-truststore-password", PASS);

        awaitClusterReady("app/__ready__");
        assertAuthorizedAndDenied();
    }

    @Test
    @DisplayName("Agent authenticates with server-authenticated TLS + token (no client cert)")
    void serverAuthTlsWithTokenRoundTrip() throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(baseDir.resolve("ca")), CLUSTER);
        // Server presents a cert (CN=node-1) but requires no client cert; identity is the token.
        final ClientSecurityProvider serverSecurity =
                TlsClientSecurityProvider.serverAuthOnly(TlsConfig.of(ca.serverContext("node-1")));
        final ClientAuthenticator authenticator = new TokenClientAuthenticator(
                InMemoryClientTokenStore.builder().add(CLIENT, TOKEN, Long.MAX_VALUE).build());
        final InetSocketAddress clientAddr = startNode(serverSecurity, authenticator);

        final Path truststore = ca.trustStore();
        agent = startAgent(
                "--client-id", CLIENT.value(),
                "--nodes", "1=" + hostPort(clientAddr),
                "--http-bind", "127.0.0.1:0",
                "--tls",
                "--tls-truststore", truststore.toString(), "--tls-truststore-password", PASS,
                "--token", TOKEN);

        awaitClusterReady("app/__ready__");
        assertAuthorizedAndDenied();
    }

    /** Start a single-node cluster with the given client-port security + authenticator, granting
     * {@link #CLIENT} GET/PUT under {@code app/}. Returns the node's client bind address. */
    private InetSocketAddress startNode(final ClientSecurityProvider serverSecurity,
                                        final ClientAuthenticator authenticator) throws Exception {
        // A one-node cluster still has to name itself in its own members map, so bind the
        // peer listener first and build the map from the address it got.
        final ListenSocket peerSocket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        final InetSocketAddress peerAddr = peerSocket.address();

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

        final TcpClientServerTransport clientServer =
                DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                        new InetSocketAddress("127.0.0.1", 0), clientConfig, authenticator,
                        serverSecurity));
        final InetSocketAddress clientAddr =
                new InetSocketAddress("127.0.0.1", clientServer.boundPort());
        node.registerClientAcl(InMemoryClientAcl.builder()
                .grant(CLIENT, "app/", ClientOp.GET, ClientOp.PUT)
                .build());
        node.start();
        return clientAddr;
    }

    private DisCasAgent startAgent(final String... args) throws Exception {
        // Ephemeral metrics port on top of whatever the caller passed, so concurrently running
        // agents in this suite never contend for the default 9601.
        final String[] withMetrics = Arrays.copyOf(args, args.length + 2);
        withMetrics[args.length] = "--observability-bind";
        withMetrics[args.length + 1] = "127.0.0.1:0";
        final DisCasAgent handle = DisCasAgent.start(DisCasAgentConfig.resolve(withMetrics, Map.of()));
        baseUrl = "http://127.0.0.1:" + handle.port();
        return handle;
    }

    /** An authorized PUT/GET under {@code app/} succeeds; an out-of-policy PUT is denied (403). */
    private void assertAuthorizedAndDenied() throws Exception {
        HttpResponse<String> r = put("/v1/kv/app/order", "PENDING");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"ok\":true"), r.body());

        r = get("/v1/kv/app/order?raw");
        assertEquals(200, r.statusCode(), r.body());
        assertEquals("PENDING", r.body());

        // Outside the granted app/ prefix. The node returns ClientErrorCode.ACCESS_DENIED and the
        // agent maps that code -- not the error text -- to 403. A 500 would tell callers to retry
        // something that can never succeed, hiding a policy problem in a generic server fault.
        r = put("/v1/kv/billing/secret", "x");
        assertEquals(403, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"error\":"), r.body());
    }

    /**
     * @param probeKey a key the agent's client is authorized to write. A cluster with client
     *                 authorization enabled denies a probe outside the granted prefix, which
     *                 never resolves into readiness.
     */
    private void awaitClusterReady(final String probeKey) throws Exception {
        TestAwait.until("the agent to answer a write to " + probeKey, () -> {
            final HttpResponse<String> response = put("/v1/kv/" + probeKey, "1");
            if (response.statusCode() != 200 || !response.body().contains("\"ok\":true")) {
                throw new IllegalStateException(
                        "Not ready: " + response.statusCode() + " " + response.body());
            }
        });
    }

    private static String hostPort(final InetSocketAddress a) {
        return a.getHostString() + ":" + a.getPort();
    }

    private HttpResponse<String> get(final String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> put(final String path, final String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                        .PUT(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
