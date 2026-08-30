/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.client.transport.ColocatedClientBootstrap;
import io.github.green4j.discas.client.transport.ColocatedClientTransport;
import io.github.green4j.discas.client.transport.TcpClientBootstrap;
import io.github.green4j.discas.client.transport.TcpClientTransport;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.ListenSocket;
import io.github.green4j.discas.common.transport.TransportSetupException;
import io.github.green4j.discas.common.transport.security.PlaintextClientSecurity;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.NodeState;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerBootstrap;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.wal.InMemoryWal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A client inside member 1 of a three-member TCP cluster.
 * <p>
 * The fixture is built so that the local path is not merely preferred but the <b>only</b> way to
 * reach member 1: that node is given no client server at all, and the address the client is
 * configured with for it points at a port nothing listens on. A colocated client that quietly fell
 * back to the loopback, or that failed over off the local member, would still answer every call
 * correctly -- the other two members form a quorum -- so the assertions are on what the transport
 * did, not only on what the caller got back.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@DisplayName("Colocated client -- local member in process, the rest over TCP")
class ColocatedClientTest {

    private static final NodeId LOCAL = NodeId.of("1");
    private static final List<NodeId> MEMBERS = List.of(NodeId.of("1"), NodeId.of("2"), NodeId.of("3"));
    private static final ClusterId CLUSTER = ClusterId.of("colocated-tcp");

    private final Map<NodeId, DisCasNode> nodes = new LinkedHashMap<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final RecordingObserver observer = new RecordingObserver();

    /** Client-port addresses as the client sees them; member 1's is deliberately dead. */
    private Map<NodeId, InetSocketAddress> clientAddresses;
    private DisCasClient client;

    @BeforeEach
    void setup() throws Exception {
        // Bind every peer listener first: each node validates the whole members map at
        // construction, so the addresses have to be facts before the first node is built.
        final Map<NodeId, ListenSocket> peerSockets = new LinkedHashMap<>();
        final Map<NodeId, InetSocketAddress> peerAddresses = new LinkedHashMap<>();
        for (final NodeId member : MEMBERS) {
            final ListenSocket socket = ListenSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            peerSockets.put(member, socket);
            peerAddresses.put(member, socket.address());
        }

        clientAddresses = new LinkedHashMap<>();
        for (final NodeId member : MEMBERS) {
            final DisCasNode node = DisCasNodeFactory.create(
                    new NodeConfig(member, CLUSTER, MEMBERS.size()),
                    new TcpPeerBootstrap(peerSockets.get(member),
                            InMemoryMembers.ofTcp(peerAddresses), TcpTransportConfig.defaults()),
                    new InMemoryWal());
            nodes.put(member, node);

            if (LOCAL.equals(member)) {
                // No client server. The only ingress this node has is the in-process one, which is
                // what makes the local path observable: nothing else can reach it.
                node.registerClientMessages(ingress -> InProcessClientRegistry.register(
                        member, node.loop(), ingress, node.clusterSize()));
                clientAddresses.put(member, deadAddress());
            } else {
                final TcpClientServerTransport clientServer = DisCasNodeFactory.createClientServer(
                        node, new TcpClientServerBootstrap(new InetSocketAddress("127.0.0.1", 0),
                                ClientTransportConfig.defaults()));
                clientAddresses.put(member,
                        new InetSocketAddress("127.0.0.1", clientServer.boundPort()));
            }
        }

        for (final DisCasNode node : nodes.values()) {
            node.start();
        }
        TestAwait.until("every node serves", Duration.ofSeconds(60), () -> {
            for (final DisCasNode node : nodes.values()) {
                final NodeState state = node.healthSource().state();
                if (state != NodeState.SERVING) {
                    throw new IllegalStateException(node.nodeId().value() + " is " + state);
                }
            }
        });

        client = DisCasClientFactory.createColocated(ClientId.of("colocated-1"), bootstrap(observer));
        TestAwait.awaitReady(client);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(client);
        for (final AutoCloseable closeable : closeables) {
            closeQuietly(closeable);
        }
        for (final DisCasNode node : nodes.values()) {
            closeQuietly(node);
        }
        InProcessClientRegistry.unregister(LOCAL);
    }

    /**
     * Every key lands, and member 1 was never dialled. The second half is the one that matters:
     * with a quorum available among members 2 and 3, a broken local path would show up only as a
     * connection that should not exist.
     */
    @Test
    @DisplayName("The local member is reached without a socket, every other over TCP")
    void localMemberIsReachedWithoutASocket() throws Exception {
        for (int i = 0; i < 30; i++) {
            final String key = "colo/key-" + i;
            client.put(TestBytes.utf8(key), TestBytes.utf8("v" + i)).get(8, TimeUnit.SECONDS);
            assertEquals("v" + i, TestBytes.string(
                    client.get(TestBytes.utf8(key)).get(8, TimeUnit.SECONDS).value()));
        }

        assertFalse(observer.handshakes.contains(LOCAL),
                "The local member must never be reached over TCP");
        assertFalse(observer.failures.contains(LOCAL),
                "A send to the local member must not fail: " + observer.failures);
        // 30 keys over three coordinators: the remote pair is reached, so the run really did
        // exercise both paths rather than routing everything one way.
        assertTrue(observer.handshakes.contains(NodeId.of("2"))
                        && observer.handshakes.contains(NodeId.of("3")),
                "Both remote members should have been dialled, saw " + observer.handshakes);
    }

    /**
     * {@code scan} is the operation that fans out to every member at once, so one call proves the
     * two paths compose: the local answer and the remote ones are merged into a single quorum.
     */
    @Test
    @DisplayName("A scan reaches quorum across the local answer and the remote ones")
    void scanReachesQuorumAcrossBothPaths() throws Exception {
        for (int i = 0; i < 5; i++) {
            client.put(TestBytes.utf8("scan/key-" + i), TestBytes.utf8("v")).get(8, TimeUnit.SECONDS);
        }

        final ScanPage page = client.scan("scan/", ScanCoverage.QUORUM).get(15, TimeUnit.SECONDS);
        assertTrue(page.quorumReached(), "A scan must reach a quorum");
        assertEquals(3, page.clusterSize());
        assertEquals(5, page.results().size(), "Every committed key must appear");
    }

    /**
     * Coordinator affinity follows a peer's position in the client's list, so a colocated client
     * that enumerated its members differently from a plain one would send the same key to a
     * different coordinator -- and two proposers on one key is a ballot duel, not a faster path.
     */
    @Test
    @DisplayName("Peer order matches a plain TCP client built from the same map")
    void peerOrderMatchesAPlainClient() {
        final EventLoop colocatedLoop = new EventLoop("peers-colocated");
        final EventLoop plainLoop = new EventLoop("peers-plain");
        final ColocatedClientTransport colocated = colocatedTransport(colocatedLoop);
        final TcpClientTransport plain = new TcpClientTransport(plainLoop, clientAddresses,
                ClientTransportConfig.defaults(), ClientId.of("plain"), null);
        closeables.add(colocated);
        closeables.add(plain);

        assertEquals(plain.peers(), colocated.peers(),
                "A colocated client must resolve keys to the same coordinators as any other client");
    }

    /**
     * The registry carries the cluster's frozen {@code N}, so a scan issued before any TCP
     * handshake has completed can still work out what a majority is.
     */
    @Test
    @DisplayName("The cluster size is known before any connection is made")
    void clusterSizeIsKnownBeforeAnyConnection() {
        final EventLoop loop = new EventLoop("size");
        final ColocatedClientTransport transport = colocatedTransport(loop);
        closeables.add(transport);

        assertEquals(MEMBERS.size(), transport.clusterSize());
    }

    /**
     * A local member that is gone is an unreachable coordinator like any other: the request moves
     * on rather than falling back to a loopback connection that would authenticate differently.
     */
    @Test
    @DisplayName("Work continues through the remaining members when the local one goes away")
    void failsOverWhenTheLocalMemberGoesAway() throws Exception {
        client.put(TestBytes.utf8("gone/before"), TestBytes.utf8("v")).get(8, TimeUnit.SECONDS);

        InProcessClientRegistry.unregister(LOCAL);
        nodes.get(LOCAL).close();

        for (int i = 0; i < 20; i++) {
            final String key = "gone/key-" + i;
            client.put(TestBytes.utf8(key), TestBytes.utf8("v")).get(15, TimeUnit.SECONDS);
            assertEquals("v", TestBytes.string(
                    client.get(TestBytes.utf8(key)).get(15, TimeUnit.SECONDS).value()));
        }
        assertFalse(observer.handshakes.contains(LOCAL),
                "A vanished local member must not be dialled over TCP instead");
    }

    @Test
    @DisplayName("A bootstrap that leaves the local member out of the cluster is refused")
    void bootstrapWithoutTheLocalMemberIsRefused() {
        final Map<NodeId, InetSocketAddress> withoutLocal = new HashMap<>(clientAddresses);
        withoutLocal.remove(LOCAL);

        assertThrows(IllegalArgumentException.class, () -> new ColocatedClientBootstrap(LOCAL,
                new TcpClientBootstrap(withoutLocal, ClientTransportConfig.defaults())));
    }

    /**
     * A node registers its client ingress when it is constructed, so this fires only when the
     * client was built first -- a wiring order to fix, and better said at construction than as one
     * key mysteriously failing over later.
     */
    @Test
    @DisplayName("A colocated transport without its node registered is refused at construction")
    void unregisteredLocalNodeIsRefusedAtConstruction() {
        final EventLoop loop = new EventLoop("unregistered");
        InProcessClientRegistry.unregister(LOCAL);

        final TransportSetupException thrown = assertThrows(TransportSetupException.class,
                () -> colocatedTransport(loop));
        assertEquals(TransportSetupException.Fault.UNKNOWN_TARGET, thrown.fault());
        loop.shutdown();
    }

    private ColocatedClientBootstrap bootstrap(final ClientObserver clientObserver) {
        return new ColocatedClientBootstrap(LOCAL, new TcpClientBootstrap(clientAddresses,
                ClientTransportConfig.defaults(), null, PlaintextClientSecurity.PROVIDER,
                clientObserver));
    }

    private ColocatedClientTransport colocatedTransport(final EventLoop loop) {
        return new ColocatedClientTransport(loop, LOCAL, clientAddresses,
                ClientTransportConfig.defaults(), ClientId.of("probe"), null,
                PlaintextClientSecurity.PROVIDER, ClientObserver.NONE);
    }

    /** A port that was bound and released: connecting to it is refused rather than accepted. */
    private static InetSocketAddress deadAddress() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
            return new InetSocketAddress("127.0.0.1", socket.getLocalPort());
        }
    }

    private static void closeQuietly(final AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (final Exception ignored) {
        }
    }

    /** Which members were actually dialled, and which sends failed. */
    private static final class RecordingObserver implements ClientObserver {
        private final Set<NodeId> handshakes = ConcurrentHashMap.newKeySet();
        private final Set<NodeId> failures = ConcurrentHashMap.newKeySet();

        @Override
        public void serverHandshakeCompleted(final NodeId peer) {
            handshakes.add(peer);
        }

        @Override
        public void sendFailed(final NodeId target, final Throwable error) {
            failures.add(target);
        }
    }
}
