/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.node.membership.InMemoryMembers;

import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.NodeState;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Co-located DisCasClient -- shared event loop with DisCasNode")
class DisCasClientColocatedTest {
    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    private final List<NodeId> nodeIds = List.of(nid(1), nid(2), nid(3));
    private final Map<NodeId, EventLoop> loops = new HashMap<>();
    private final Map<NodeId, DisCasNode> nodes = new HashMap<>();
    private final List<DisCasClient> clients = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (final DisCasClient client : clients) {
            try {
                client.close();
            } catch (final Exception ignored) {
            }
        }
        for (final DisCasNode node : nodes.values()) {
            try {
                node.close();
            } catch (final Exception ignored) {
            }
        }
        for (final NodeId nodeId : nodeIds) {
            InProcessClientRegistry.unregister(nodeId);
        }
    }

    private void startCluster() throws Exception {
        for (final NodeId nodeId : nodeIds) {
            final EventLoop loop = new EventLoop("colocated-node-" + nodeId);
            loops.put(nodeId, loop);
            final InProcessPeerTransport peerTransport =
                    new InProcessPeerTransport(nodeId, nodeIds.size(), loop, InMemoryMembers.ofNodes(nodeIds));
            final DisCasNode node = new DisCasNode(new NodeConfig(nodeId, ClusterId.of("colocated-cluster"),
                    nodeIds.size()),
                    new InMemoryWal(), loop, peerTransport);
            node.registerClientMessages(ingress ->
                    InProcessClientRegistry.register(nodeId, loop, ingress, node.clusterSize()));
            nodes.put(nodeId, node);
        }
        for (final DisCasNode node : nodes.values()) {
            node.start();
        }
        // Wait for the cluster rather than sleeping at it. A fixed pause encodes a guess about how
        // fast a machine is, and this one is not about startup latency at all -- it is about what a
        // co-located client does once the cluster serves. On a loaded CI box the guess is wrong and
        // the test fails for a reason it is not testing.
        TestAwait.until("every node serves", Duration.ofSeconds(60), () -> {
            for (final DisCasNode node : nodes.values()) {
                final NodeState state = node.healthSource().state();
                if (state != NodeState.SERVING) {
                    throw new IllegalStateException(node.nodeId().value() + " is " + state);
                }
            }
        });
    }

    @Test
    @DisplayName("End-to-end put/get through a co-located client sharing node 1's loop")
    void colocatedClientRoundTrip() throws Exception {
        startCluster();

        final DisCasClient client = colocated(nodes.get(nid(1)));
        clients.add(client);

        final ByteBuffer key = ByteBuffer.wrap("colocated-key".getBytes());
        final ByteBuffer value = ByteBuffer.wrap("colocated-value".getBytes());

        client.put(key.duplicate(), value.duplicate()).get(5, TimeUnit.SECONDS);
        final ByteBuffer read = client.get(key.duplicate()).get(5, TimeUnit.SECONDS).value();
        assertEquals("colocated-value", new String(readBytes(read)));
    }

    @Test
    @DisplayName("Client shutdown does not stop the shared node loop")
    void shutdownDoesNotStopSharedLoop() throws Exception {
        startCluster();

        final DisCasNode node1 = nodes.get(nid(1));
        final EventLoop sharedLoop = node1.loop();

        final DisCasClient client = colocated(node1);
        clients.add(client);

        client.put(ByteBuffer.wrap("k".getBytes()), ByteBuffer.wrap("v".getBytes()))
                .get(5, TimeUnit.SECONDS);

        client.close();
        clients.remove(client);

        assertTrue(sharedLoop.isRunning(), "Node loop must survive client shutdown");

        // A fresh co-located client on the same node must still be functional
        final DisCasClient recreated = colocated(node1);
        clients.add(recreated);
        final ByteBuffer read = recreated.get(ByteBuffer.wrap("k".getBytes())).get(5, TimeUnit.SECONDS).value();
        assertEquals("v", new String(readBytes(read)));
    }

    @Test
    @DisplayName("Client with owned loop stops the loop on shutdown (baseline)")
    void ownedLoopIsStoppedOnShutdown() throws Exception {
        startCluster();

        final EventLoop dedicatedLoop = new EventLoop("owned-loop");
        final ClientId ownedClientId = ClientId.of("owned-loop-client");
        final InProcessClientTransport transport =
                new InProcessClientTransport(dedicatedLoop, nodeIds, ownedClientId);
        final DisCasClient client = new DisCasClient(ownedClientId, transport, dedicatedLoop);
        clients.add(client);

        client.put(ByteBuffer.wrap("owned-k".getBytes()), ByteBuffer.wrap("owned-v".getBytes()))
                .get(5, TimeUnit.SECONDS);

        client.close();
        clients.remove(client);

        assertFalse(dedicatedLoop.isRunning(), "Owned loop must be stopped by client shutdown");
    }

    /**
     * Wire a {@link DisCasClient} co-located with {@code node}, sharing the node's
     * event loop via the in-process transport (does not own the loop). Uses only
     * public client/node API -- no dependency on the example module.
     */
    private static DisCasClient colocated(final DisCasNode node) {
        final ClientId clientId = ClientId.of("colocated-" + node.nodeId().value());
        final InProcessClientTransport transport =
                new InProcessClientTransport(node.loop(), List.of(node.nodeId()), clientId);
        return new DisCasClient(clientId, transport, node.loop(), false);
    }

    private static byte[] readBytes(final ByteBuffer buf) {
        final byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }
}
