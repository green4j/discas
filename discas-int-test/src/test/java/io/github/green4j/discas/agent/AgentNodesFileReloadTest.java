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
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of the agent's {@code --nodes-file}: the agent starts pointed at a
 * <em>subset</em> of the cluster, then the file is rewritten to the full membership and
 * {@code POST /v1/agent/reload} is called, and the agent must rebuild its client to reach the new
 * node set. A malformed rewrite must be refused (the last good client keeps serving) and must say
 * so, since an operator who rewrote the file and got no answer has no way to tell the difference.
 * <p>
 * Driven through the endpoint rather than the API behind it, because the endpoint is what an
 * operator has: a reload that works in-process and 500s over HTTP is a reload nobody can use.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Agent -- reloading the nodes file")
class AgentNodesFileReloadTest {

    private static final List<NodeId> NODE_IDS = List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));

    @TempDir
    Path baseDir;

    private final List<DisCasNode> nodes = new ArrayList<>();
    private final Map<NodeId, InetSocketAddress> peerAddresses = new LinkedHashMap<>();
    private final Map<NodeId, InetSocketAddress> clientAddresses = new LinkedHashMap<>();

    private DisCasAgent agent;
    private String baseUrl;
    private Path nodesFile;

    private final HttpClient http = HttpClient.newHttpClient();

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
            final Path walBase = baseDir.resolve("node-" + nodeId.value());
            Files.createDirectories(walBase);
            final FileWal wal = new FileWal(StorageConfig.builder()
                    .baseDirectory(walBase)
                    .walMaxFileBytes(16 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build());
            wal.initialize();

            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(nodeId, ClusterId.of("agent-reload-cluster"), peerAddresses.size()),
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

        // Agent starts pointed at a single node (a subset of the 3-node cluster).
        nodesFile = baseDir.resolve("nodes.conf");
        writeNodesFile(List.of(NodeId.of("1")));

        final DisCasAgentConfig cfg = DisCasAgentConfig.resolve(
                new String[] {"--nodes-file", nodesFile.toString(), "--http-bind", "127.0.0.1:0",
                        "--observability-bind", "127.0.0.1:0"}, Map.of());
        agent = DisCasAgent.start(cfg);
        baseUrl = "http://127.0.0.1:" + agent.port();
    }

    @AfterEach
    void tearDown() {
        if (agent != null) {
            agent.close();
        }
        for (final DisCasNode node : nodes) {
            try {
                node.close();
            } catch (final Exception ignored) {
            }
        }
    }

    @Test
    void reloadPicksUpAddedNodes() throws Exception {
        awaitClusterReady("__ready_probe__");

        // Subset mode: health reports only node 1, and KV round-trips (node 1 drives quorum).
        String health = get("/v1/agent/health").body();
        String nodesArray = extract(health, "\"nodes\":[", "]");
        assertTrue(nodesArray.contains("\"1\""), health);
        assertFalse(nodesArray.contains("\"2\""), "Expected only node 1 before reload: " + health);
        assertFalse(nodesArray.contains("\"3\""), "Expected only node 1 before reload: " + health);
        assertTrue(put("/v1/kv/reload/before", "v1").body().contains("\"ok\":true"));

        // Rewrite the file to the full membership and ask for a reload.
        writeNodesFile(NODE_IDS);
        final HttpResponse<String> reloaded = post("/v1/agent/reload");
        assertTrue(reloaded.statusCode() == 200, reloaded.body());
        assertTrue(reloaded.body().contains("\"status\":\"applied\""), reloaded.body());
        assertTrue(reloaded.body().contains(nodesFile.toString()),
                "The report names the file it read: " + reloaded.body());

        // The client was rebuilt: health now reports all three nodes and KV still works.
        awaitClusterReady("__ready_probe__");
        health = get("/v1/agent/health").body();
        nodesArray = extract(health, "\"nodes\":[", "]");
        assertTrue(nodesArray.contains("\"1\""), health);
        assertTrue(nodesArray.contains("\"2\""), health);
        assertTrue(nodesArray.contains("\"3\""), health);
        assertTrue(get("/v1/kv/reload/before?raw").body().equals("v1"), "Value survived the swap");
        assertTrue(put("/v1/kv/reload/after", "v2").body().contains("\"ok\":true"));

        // A malformed rewrite is refused, and refused out loud: the last good (3-node) client keeps
        // serving, and the caller is told which file it was and why.
        Files.writeString(nodesFile, "this is not = a valid : members file : at all\n");
        final HttpResponse<String> refused = post("/v1/agent/reload");
        assertTrue(refused.statusCode() == 400, refused.body());
        assertTrue(refused.body().contains("\"outcome\":\"failed\""), refused.body());
        health = get("/v1/agent/health").body();
        nodesArray = extract(health, "\"nodes\":[", "]");
        assertTrue(nodesArray.contains("\"1\"") && nodesArray.contains("\"2\"")
                && nodesArray.contains("\"3\""), "A refused reload must change nothing: " + health);
        assertTrue(get("/v1/kv/reload/after?raw").body().equals("v2"), health);
    }

    @Test
    @DisplayName("A GET on the reload endpoint is refused")
    void reloadRefusesAnythingButPost() throws Exception {
        // It changes what the agent is running on, and a GET that does is a GET something will
        // eventually retry, prefetch or cache.
        assertTrue(get("/v1/agent/reload").statusCode() == 405);
    }

    private void writeNodesFile(final Collection<NodeId> ids) throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (final NodeId id : ids) {
            final InetSocketAddress a = clientAddresses.get(id);
            sb.append("node.").append(id.value()).append(" = ")
                    .append(a.getHostString()).append(':').append(a.getPort()).append('\n');
        }
        Files.writeString(nodesFile, sb.toString());
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

    private HttpResponse<String> get(final String path) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build());
    }

    private HttpResponse<String> put(final String path, final String body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .PUT(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    private HttpResponse<String> post(final String path) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .POST(HttpRequest.BodyPublishers.noBody()).build());
    }

    private HttpResponse<String> send(final HttpRequest request) throws Exception {
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String extract(final String s, final String startMarker, final String endMarker) {
        final int i = s.indexOf(startMarker);
        if (i < 0) {
            return "";
        }
        final int from = i + startMarker.length();
        final int j = s.indexOf(endMarker, from);
        return j < 0 ? "" : s.substring(from, j);
    }
}
