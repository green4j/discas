/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.agent;

import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.io.ReloadObserver;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.chaos.ChaosProxy;
import io.github.green4j.discas.client.ClientLifecycleException;
import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Swapping the agent's client while requests are in flight.
 * <p>
 * {@code DisCasClient} captures its peer set at construction, so the agent applies a nodes-file
 * change by building an entirely new client and swapping it in, retiring the old one on a separate
 * thread (its {@code shutdown()} blocks on event-loop termination and must not stall the thread that
 * asked for the reload). Anything already in flight on the outgoing client is therefore caught
 * mid-request.
 * <p>
 * That moment had no coverage: {@code AgentNodesFileReloadTest} drives the reload end-to-end over
 * HTTP but only asserts that requests work again <em>afterwards</em>. What matters operationally is
 * that an in-flight request resolves rather than hanging, that the swap is visible immediately, and
 * that a redundant reload does not churn the client -- {@code ReloadableFileSource} replays the
 * current value to every new subscriber, so a rebuild on equal input would drop live connections
 * on every startup.
 */
@DisplayName("ReloadableClient -- swapping the client under live requests")
class ReloadableClientSwapTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClientId CLIENT = ClientId.of("agent");

    @TempDir
    Path baseDir;

    private DisCasNode node;
    private ChaosProxy proxy;
    private ReloadableClient holder;
    private InetSocketAddress clientAddr;

    private static ClientTransportConfig clientConfig() {
        return ClientTransportConfig.defaults();
    }

    @BeforeEach
    void setUp() throws Exception {
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
                new NodeConfig(NODE, ClusterId.of("reload-cluster"), 1),
                new TcpPeerBootstrap(peerSocket, InMemoryMembers.ofTcp(Map.of(NODE, peerAddr)),
                        TcpTransportConfig.defaults()),
                wal);
        final TcpClientServerTransport clientServer =
                DisCasNodeFactory.createClientServer(node, new TcpClientServerBootstrap(
                        new InetSocketAddress("127.0.0.1", 0), clientConfig()));
        clientAddr = new InetSocketAddress("127.0.0.1", clientServer.boundPort());
        node.start();

        // The client reaches the node through the proxy, so a response can be withheld to hold a
        // request open across the swap.
        proxy = new ChaosProxy(clientAddr, 7L);
        proxy.forward().droppableType(FrameCodec.TYPE_CLIENT_MESSAGE);
        proxy.reverse().droppableType(FrameCodec.TYPE_CLIENT_MESSAGE);
        proxy.start();
    }

    @AfterEach
    void tearDown() {
        if (holder != null) {
            holder.close();
        }
        if (proxy != null) {
            proxy.close();
        }
        if (node != null) {
            try {
                node.close();
            } catch (final Exception ignored) {
                // best-effort
            }
        }
    }

    private Map<NodeId, InetSocketAddress> nodesVia(final InetSocketAddress address) {
        final Map<NodeId, InetSocketAddress> map = new LinkedHashMap<>();
        map.put(NODE, address);
        return map;
    }

    private ReloadableClient newHolder(final Map<NodeId, InetSocketAddress> nodes) {
        return new ReloadableClient(CLIENT, clientConfig(), null, null, nodes,
                DisCasClientConfig.defaults(),
                Executors.newSingleThreadExecutor(r -> new Thread(r, "retire-test")),
                ReloadObserver.NONE,
                ClientObserver.NONE);
    }



    @Test
    @DisplayName("A reload swaps the client and republishes the node ids atomically")
    void reloadSwapsTheClient() {
        holder = newHolder(nodesVia(proxy.listenAddress()));
        final DisCasClient before = holder.current();

        // A different address for the same node id: a real re-addressing reload.
        holder.onNodesReloaded(nodesVia(clientAddr));

        assertNotSame(before, holder.current(), "A changed node map must produce a new client");
        assertEquals(Set.of(NODE), holder.currentNodeIds());
    }

    @Test
    @DisplayName("A reload with an unchanged map does not rebuild the client")
    void redundantReloadIsANoOp() {
        final Map<NodeId, InetSocketAddress> nodes = nodesVia(proxy.listenAddress());
        holder = newHolder(nodes);
        final DisCasClient before = holder.current();

        // ReloadableFileSource replays the current value to every new subscriber, so this exact call
        // happens on normal startup. Rebuilding here would tear down live connections for nothing.
        holder.onNodesReloaded(nodesVia(proxy.listenAddress()));

        assertSame(before, holder.current(), "An equal node map must not churn the client");
    }

    @Test
    @DisplayName("A request in flight across the swap resolves instead of hanging")
    void inFlightRequestResolvesAcrossSwap() throws Exception {
        holder = newHolder(nodesVia(proxy.listenAddress()));
        final DisCasClient before = holder.current();
        before.put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(20, TimeUnit.SECONDS);

        // Withhold the response so the read is genuinely outstanding when the swap happens.
        proxy.reverse().isolate(true);
        final long sentBefore = proxy.forward().framesSeen();
        final CompletableFuture<GetResult> inFlight = before.get(TestBytes.utf8("k"));
        // The read has to be genuinely outstanding when the swap happens, so wait for it to cross
        // the proxy rather than guessing how long that takes.
        TestAwait.until("the read to reach the server", () -> {
            if (proxy.forward().framesSeen() == sentBefore) {
                throw new IllegalStateException("Request not on the wire yet");
            }
        });

        holder.onNodesReloaded(nodesVia(clientAddr));
        proxy.reverse().isolate(false);

        // The retired client is shut down off-thread; its pending futures must be completed
        // exceptionally rather than left dangling for a caller that will never hear back.
        final ExecutionException failed = assertThrows(ExecutionException.class,
                () -> inFlight.get(20, TimeUnit.SECONDS),
                "An in-flight request on a retired client must resolve, not hang");

        // Assert HOW it resolved, not merely that it did. "It threw eventually" would also be true
        // if the retirement never happened and the request simply timed out on its own after the
        // per-attempt budget -- which would leave the drain path untested.
        final Throwable cause = failed.getCause();
        assertTrue(cause instanceof ClientLifecycleException,
                "Expected the retirement to drain the request, but it failed as: " + cause);
        assertEquals(ClientLifecycleException.Phase.CLOSING,
                ((ClientLifecycleException) cause).phase(),
                "The request must be failed by the shutdown of the retired client");
    }

    @Test
    @DisplayName("Requests issued after the swap run on the new client")
    void requestsAfterSwapUseTheNewClient() throws Exception {
        holder = newHolder(nodesVia(proxy.listenAddress()));
        holder.current().put(TestBytes.utf8("k"), TestBytes.utf8("v1")).get(20, TimeUnit.SECONDS);

        // Swap to the node's real address, bypassing the proxy entirely.
        holder.onNodesReloaded(nodesVia(clientAddr));

        // Cutting the proxy proves the new client is not still routed through it.
        proxy.cutLiveConnections();
        proxy.refuseNewConnections(true);

        final DisCasClient after = holder.current();
        after.put(TestBytes.utf8("k"), TestBytes.utf8("v2")).get(20, TimeUnit.SECONDS);
        assertEquals("v2", TestBytes.string(after.get(TestBytes.utf8("k")).get(20, TimeUnit.SECONDS).value()));
    }

    @Test
    @DisplayName("Close after a swap shuts down cleanly and is idempotent")
    void closeAfterSwapIsClean() throws Exception {
        holder = newHolder(nodesVia(proxy.listenAddress()));
        holder.current().put(TestBytes.utf8("k"), TestBytes.utf8("v")).get(20, TimeUnit.SECONDS);
        holder.onNodesReloaded(nodesVia(clientAddr));

        // close() races the retirer executor; a second close must not throw either.
        holder.close();
        holder.close();
        holder = null; // already closed; keep tearDown from closing a third time
    }

    @Test
    @DisplayName("A reload after close is ignored")
    void reloadAfterCloseIsIgnored() {
        holder = newHolder(nodesVia(proxy.listenAddress()));
        holder.close();

        final DisCasClient afterClose = holder.current();
        holder.onNodesReloaded(nodesVia(clientAddr));

        assertSame(afterClose, holder.current(),
                "A late reload callback must not resurrect a closed holder with a live client");
        holder = null;
    }
}
