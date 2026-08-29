/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.DisCasClientConfig;
import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.node.NodeState;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.InProcessPeerTransport;
import io.github.green4j.discas.node.transport.PartitionablePeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Test harness that spins up N DisCasNode instances and M DisCasClient instances
 * using in-process transports and in-memory WALs
 */
public final class TestCluster implements AutoCloseable {
    private final List<Integer> nodeIds;
    private final Map<Integer, DisCasNode> nodes = new HashMap<>();
    private final Map<Integer, InMemoryWal> wals = new HashMap<>();
    private final Map<Integer, PartitionablePeerTransport> transports = new HashMap<>();
    private final Map<Integer, EventLoop> nodeLoops = new HashMap<>();
    private final List<DisCasClient> clients = new ArrayList<>();

    public TestCluster(final int clusterSize) {
        this(clusterSize, clusterSize);
    }

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    public TestCluster(final int clusterSize, final int numClients) {
        this(clusterSize, numClients, UnaryOperator.identity(), DisCasClientConfig.defaults());
    }

    /**
     * As above, with the node and client timing knobs opened up.
     * <p>
     * A test that wants a coordinator to stall rather than fail needs the node's
     * {@code roundTimeout} to outlast the stall, and the client's {@code perAttemptTimeout} to be
     * far shorter than it -- the two defaults (5s and 5s) make that arrangement impossible to
     * express.
     *
     * @param nodeConfig applied to each node's builder after the identity fields are set
     */
    public TestCluster(final int clusterSize,
                       final int numClients,
                       final UnaryOperator<NodeConfig.Builder> nodeConfig,
                       final DisCasClientConfig clientConfig) {
        nodeIds = new ArrayList<>();
        for (int i = 1; i <= clusterSize; i++) {
            nodeIds.add(i);
        }
        final List<NodeId> allNodeIds = new ArrayList<>();
        for (final int id : nodeIds) {
            allNodeIds.add(nid(id));
        }

        for (final int nodeId : nodeIds) {
            final EventLoop loop = new EventLoop("node-" + nodeId);
            nodeLoops.put(nodeId, loop);

            final InProcessPeerTransport rawTransport =
                    new InProcessPeerTransport(nid(nodeId), allNodeIds.size(), loop,
                            InMemoryMembers.ofNodes(allNodeIds));
            final PartitionablePeerTransport transport =
                    new PartitionablePeerTransport(nid(nodeId), rawTransport);
            transports.put(nodeId, transport);

            final InMemoryWal wal = new InMemoryWal();
            wals.put(nodeId, wal);
        }

        for (final int nodeId : nodeIds) {
            final NodeId nidNode = nid(nodeId);
            final DisCasNode node = new DisCasNode(
                    nodeConfig.apply(NodeConfig.builder()
                            .nodeId(nidNode)
                            .clusterId(ClusterId.of("test-cluster"))
                            .clusterSize(nodeIds.size())).build(),
                    wals.get(nodeId), nodeLoops.get(nodeId),
                    transports.get(nodeId));
            node.registerClientMessages(ingress ->
                    InProcessClientRegistry.register(nidNode, nodeLoops.get(nodeId), ingress,
                            node.clusterSize()));
            nodes.put(nodeId, node);
        }

        for (int c = 0; c < numClients; c++) {
            final EventLoop clientLoop = new EventLoop("client-" + c);
            final ClientId clientId = ClientId.of("client-" + c);
            final InProcessClientTransport ct =
                    new InProcessClientTransport(clientLoop, allNodeIds, clientId);
            clients.add(new DisCasClient(clientId, ct, clientLoop, true,
                    ClientObserver.NONE, clientConfig));
        }
    }

    public void start() {
        for (final DisCasNode node : nodes.values()) {
            node.start();
        }
    }

    /**
     * Wait for the cluster to be usable -- by asking it, never by sleeping a guessed interval. A
     * fixed pause encodes how fast the machine is, which on a loaded CI box is a guess that fails
     * tests for reasons they are not about.
     * <p>
     * Every node must report {@code SERVING}, and <em>then</em> a client read must succeed. A read
     * alone is not enough: it needs a quorum, so it starts answering with the last node still
     * replaying, and that node is invisibly dead weight. It drops every peer message it is sent
     * ({@code DisCasNode.dispatchPeer} sheds anything that arrives before {@code SERVING}) and
     * answers a client with {@code NOT_READY}, which puts it in the client's peer backoff and
     * routes the next requests around it. A test that then targets that node -- holding it,
     * isolating it, counting what it sends -- is silently testing nothing. Ordering the two checks
     * matters for the same reason: probing only once everyone serves keeps the readiness probe from
     * being what poisons the backoff.
     */
    public void awaitReady() throws Exception {
        TestAwait.until("every node serves", Duration.ofSeconds(60), () -> {
            for (final DisCasNode node : nodes.values()) {
                final NodeState state = node.healthSource().state();
                if (state != NodeState.SERVING) {
                    throw new IllegalStateException(node.nodeId().value() + " is " + state);
                }
            }
        });
        if (clients.isEmpty()) {
            return;
        }
        TestAwait.awaitReady(clients.get(0));
    }

    public DisCasNode node(final int nodeId) {
        return nodes.get(nodeId);
    }

    public InMemoryWal wal(final int nodeId) {
        return wals.get(nodeId);
    }

    public PartitionablePeerTransport transport(final int nodeId) {
        return transports.get(nodeId);
    }

    public DisCasClient client(final int index) {
        return clients.get(index);
    }

    public List<Integer> nodeIds() {
        return nodeIds;
    }

    public int clusterSize() {
        return nodeIds.size();
    }

    @Override
    public void close() {
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
    }
}
