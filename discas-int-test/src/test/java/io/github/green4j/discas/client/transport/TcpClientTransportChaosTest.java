/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.client.GetResult;
import java.util.List;
import io.github.green4j.discas.chaos.TestProfile;
import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.TestPorts;
import io.github.green4j.discas.chaos.ChaosProxy;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientFactory;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire-level fault injection against the <b>real NIO client transport</b>.
 * <p>
 * Every other chaos test in the suite drives its clients through
 * {@code InProcessClientTransport}, and {@code ChaosProxy} was only ever placed on <em>peer</em>
 * links -- so {@link TcpClientTransport} had no coverage of the things that actually go wrong on a
 * client link: a connection dying mid-request, a node going away and coming back, responses
 * vanishing. That transport also has no connect timeout, no backoff and no reconnect scheduler
 * (it re-dials lazily on the next send), which makes the absence of tests worse, not better.
 * <p>
 * A single node is used deliberately. With several, the client's peer failover would mask a
 * broken reconnect: the point here is that the transport recovers <em>the same connection path</em>,
 * not that the client can route around it.
 */
@Tag("chaos")
@DisplayName("TcpClientTransport -- wire-level faults on the client link")
class TcpClientTransportChaosTest {

    private static final NodeId NODE = NodeId.of("1");
    private static final ClientId CLIENT = ClientId.of("chaos-client");

    /** Comfortably above the client's 5s per-attempt timeout so a retry can complete. */
    private static final long OP_TIMEOUT_SECONDS = 20;

    /**
     * How long the client is given to become usable again after a fault. Bounds a retry loop, so
     * it costs nothing when recovery is immediate and only comes into play when the first send
     * after a fault is the one that gets lost.
     */
    private static final long RECOVERY_BUDGET_SECONDS = 60;

    @TempDir
    Path baseDir;

    private DisCasNode node;
    private TcpClientServerTransport clientServer;
    private ChaosProxy proxy;
    private DisCasClient client;
    private InetSocketAddress clientAddr;

    private static ClientTransportConfig clientConfig() {
        return ClientTransportConfig.defaults();
    }

    @BeforeEach
    void requireChaosProfile() {
        assumeTrue(TestProfile.current().atLeast(TestProfile.STANDARD),
                "chaos: standard profile or above");
    }

    @BeforeEach
    void setUp() throws Exception {
        // Both ports are pre-allocated here, deliberately. restartNode() rebinds the *same*
        // client address, because the proxy in front of it forwards to a fixed target -- so the
        // bind-:0-and-read-back trick the other tests use does not apply: a fresh ephemeral port
        // on every restart would leave the proxy pointing at the old one.
        final List<Integer> ports = TestPorts.allocate(2);
        final int peerPort = ports.get(0);
        final int clientPort = ports.get(1);
        final InetSocketAddress peerAddr = new InetSocketAddress("127.0.0.1", peerPort);
        clientAddr = new InetSocketAddress("127.0.0.1", clientPort);

        startNode(peerAddr);

        // The client is pointed at the proxy, never at the node, so the link can be faulted
        // without the client's configured address changing.
        proxy = new ChaosProxy(clientAddr, 42L);
        proxy.forward().droppableType(FrameCodec.TYPE_CLIENT_MESSAGE);
        proxy.reverse().droppableType(FrameCodec.TYPE_CLIENT_MESSAGE);
        proxy.start();

        client = DisCasClientFactory.create(CLIENT,
                new TcpClientBootstrap(Map.of(NODE, proxy.listenAddress()), clientConfig()));
    }

    private void startNode(final InetSocketAddress peerAddr) throws Exception {
        final Path walBase = baseDir.resolve("node-1-" + System.nanoTime());
        Files.createDirectories(walBase);
        final FileWal wal = new FileWal(StorageConfig.builder()
                .baseDirectory(walBase)
                .walMaxFileBytes(16 * 1024 * 1024).snapshotRetentionCount(2).build());
        wal.initialize();

        node = DisCasNodeFactory.create(
                new NodeConfig(NODE, ClusterId.of("chaos-client-cluster"), 1),
                new TcpPeerBootstrap(peerAddr, InMemoryMembers.ofTcp(Map.of(NODE, peerAddr)),
                        TcpTransportConfig.defaults()),
                wal);
        clientServer = new TcpClientServerTransport(
                node.loop(), clientAddr, clientConfig(), node.clusterSize());
        node.addLifecycleCloseable(clientServer);
        node.registerClientMessages(clientServer::registerIngress);
        node.start();
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            try {
                client.close();
            } catch (final Exception ignored) {
                // teardown is best-effort
            }
        }
        if (proxy != null) {
            proxy.close();
        }
        if (node != null) {
            try {
                node.close();
            } catch (final Exception ignored) {
                // teardown is best-effort
            }
        }
    }



    private void put(final String k, final String v) throws Exception {
        client.put(TestBytes.utf8(k), TestBytes.utf8(v)).get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private String get(final String k) throws Exception {
        return TestBytes.string(client.get(TestBytes.utf8(k)).get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS).value());
    }

    @FunctionalInterface
    private interface Attempt<T> {
        T run() throws Exception;
    }

    /**
     * Retries an operation until it succeeds, within {@link #RECOVERY_BUDGET_SECONDS}.
     * <p>
     * Used for the <b>first</b> operation after a fault, and only that one. This transport
     * re-dials lazily on the next send and has no reconnect scheduler, so a send issued before the
     * client has processed the socket close goes out on a channel that is already gone. With a
     * single node there is no peer to fail over to, so that request burns its per-attempt budget
     * and fails with a timeout.
     * <p>
     * That is what a lazily reconnecting transport does, not a defect -- which makes "the very
     * next request after a cut succeeds" a stronger claim than the transport makes, and it is why
     * these tests failed intermittently. What they are actually about is that the client
     * <em>becomes</em> usable again and does so over a genuinely new connection; both of those are
     * still asserted, the second by the {@code acceptedConnections()} checks that make each test
     * bite.
     */
    private <T> T untilRecovered(final Attempt<T> attempt) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RECOVERY_BUDGET_SECONDS);
        int attempts = 0;
        Exception last = null;
        while (System.nanoTime() < deadline) {
            attempts++;
            try {
                return attempt.run();
            } catch (final Exception e) {
                last = e;
                Thread.sleep(100L); // a connect refusal fails instantly; do not spin on it
            }
        }
        return fail("The client never recovered: " + attempts + " attempt(s) over "
                + RECOVERY_BUDGET_SECONDS + "s, last failure: " + last, last);
    }

    /** The first write after a fault; see {@link #untilRecovered}. */
    private void putAfterFault(final String k, final String v) throws Exception {
        untilRecovered(() -> {
            put(k, v);
            return null;
        });
    }

    /** The first read after a fault; see {@link #untilRecovered}. */
    private String getAfterFault(final String k) throws Exception {
        return untilRecovered(() -> get(k));
    }

    @Test
    @DisplayName("The client reconnects after its connection is cut")
    void reconnectsAfterConnectionCut() throws Exception {
        put("k", "v1");
        assertEquals("v1", get("k"));
        final int connectionsBefore = proxy.acceptedConnections();

        // Abrupt close, as a dead node would look. The transport has no reconnect scheduler, so
        // recovery has to come from the next send re-dialing.
        proxy.cutLiveConnections();

        // Only the first send after the cut may be lost to the dead channel; once it lands, the
        // link is healthy and the read below is held to the strict single-attempt standard.
        putAfterFault("k", "v2");
        assertEquals("v2", get("k"), "The transport must re-dial and carry on after a cut link");
        // The load-bearing assertion. Without it this test passes even when the cut does nothing,
        // because "operations still succeed" is equally true of a link that was never broken.
        assertTrue(proxy.acceptedConnections() > connectionsBefore,
                "The transport must have opened a new connection, not silently reused a dead one");
    }

    @Test
    @DisplayName("A request in flight when the link dies fails, and the next one succeeds")
    void inFlightRequestSurvivesLinkDeath() throws Exception {
        put("k", "before");

        // Drop the response so the request is genuinely outstanding, then kill the socket under it.
        proxy.reverse().isolate(true);
        final long sentBefore = proxy.forward().framesSeen();
        final CompletableFuture<GetResult> inFlight = client.get(TestBytes.utf8("k"));
        // Wait for the request to actually cross the proxy: cutting the link before it does would
        // test something else entirely, and a fixed pause only guesses when that is.
        TestAwait.until("the read to reach the server", () -> {
            if (proxy.forward().framesSeen() == sentBefore) {
                throw new IllegalStateException("Request not on the wire yet");
            }
        });
        proxy.cutLiveConnections();
        proxy.reverse().isolate(false);
        final int connectionsBefore = proxy.acceptedConnections();

        // Whether it fails or completes late, what matters is that it resolves rather than hanging
        // forever, and that the client is usable afterwards.
        try {
            inFlight.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final ExecutionException expected) {
            // A torn connection legitimately fails the outstanding request.
        }

        assertEquals("before", getAfterFault("k"),
                "The client must still work after an in-flight failure");
        assertTrue(proxy.acceptedConnections() > connectionsBefore,
                "The recovery read must travel over a newly dialed connection");
    }

    @Test
    @DisplayName("The client recovers when the node goes away and comes back")
    void recoversAcrossNodeRestart() throws Exception {
        put("k", "v1");

        // Take the node down entirely and refuse connections, so re-dials fail outright.
        proxy.refuseNewConnections(true);
        proxy.cutLiveConnections();
        node.close();
        node = null;

        assertThrows(Exception.class,
                () -> client.get(TestBytes.utf8("k")).get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "With the only node down the request must fail rather than hang");

        // Bring a node back on the same client port, replaying the WAL is not required -- what is
        // under test is that the client re-dials rather than staying wedged on the dead connection.
        startNode(new InetSocketAddress("127.0.0.1", TestPorts.free()));
        final int connectionsBefore = proxy.acceptedConnections();
        proxy.refuseNewConnections(false);

        putAfterFault("k", "v2");
        assertEquals("v2", get("k"), "The client must re-dial the restarted node");
        assertTrue(proxy.acceptedConnections() > connectionsBefore,
                "Recovery must come from a fresh connection to the restarted node");
    }

    @Test
    @DisplayName("Dropped request frames surface as a timeout, then the client keeps working")
    void droppedFramesTimeOutThenRecover() throws Exception {
        put("k", "v1");

        // Silence rather than a close: this is the timeout path, not the reconnect path.
        proxy.forward().isolate(true);
        assertThrows(Exception.class,
                () -> client.get(TestBytes.utf8("k")).get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "A request whose frames never arrive must time out, not hang forever");

        proxy.forward().isolate(false);
        assertEquals("v1", get("k"), "The client must recover once the link is healthy again");
    }

    @Test
    @DisplayName("Repeated cuts do not leak the client into an unusable state")
    void repeatedCutsKeepTheClientUsable() throws Exception {
        final int connectionsBefore = proxy.acceptedConnections();
        for (int i = 0; i < 5; i++) {
            // Every iteration but the first writes immediately after a cut.
            putAfterFault("k" + i, "v" + i);
            proxy.cutLiveConnections();
        }
        for (int i = 0; i < 5; i++) {
            assertEquals("v" + i, get("k" + i),
                    "Value written before cut " + i + " must still be readable");
        }
        // Each cut must have forced its own re-dial; a smaller count would mean some cut was a
        // no-op and the loop was not actually exercising reconnection five times.
        assertTrue(proxy.acceptedConnections() >= connectionsBefore + 5,
                "Expected a re-dial per cut, saw "
                        + (proxy.acceptedConnections() - connectionsBefore));
    }
}
