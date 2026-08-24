/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.chaos;

import io.github.green4j.discas.client.DisCasClient;
import io.github.green4j.discas.client.GetResult;
import io.github.green4j.discas.node.DisCasNode;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.node.wal.FileWal;
import io.github.green4j.discas.node.wal.StorageConfig;
import io.github.green4j.discas.node.DisCasNodeFactory;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.NodeConfig;
import io.github.green4j.discas.client.transport.InProcessClientTransport;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.node.transport.TcpPeerBootstrap;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

final class ChaosClusterHarness implements AutoCloseable {
    private static final int CLUSTER_SIZE = 3;

    private final TcpTransportConfig peerTransportConfig;
    private final ClientTransportConfig clientTransportConfig;
    private final Path rootDir;
    private final Duration repairInterval;
    private final boolean withFaults;
    // One frame-aware proxy per single-dialer pair (lower dials higher), keyed by pairKey.
    private final Map<Long, ChaosProxy> proxies = new HashMap<>();
    private final Map<Integer, InetSocketAddress> clusterAddresses;
    private final Map<Integer, InetSocketAddress> clientAddresses;
    private final Map<Integer, Path> nodeStorageDirs;
    private final Map<Integer, DisCasNode> nodes = new HashMap<>();
    private final List<DisCasClient> clients = new ArrayList<>();

    ChaosClusterHarness(final Path rootDir,
                        final TcpTransportConfig peerTransportConfig,
                        final ClientTransportConfig clientTransportConfig) {
        this(rootDir, peerTransportConfig, clientTransportConfig,
                NodeConfig.builder().repairInterval(), false, 0L);
    }

    ChaosClusterHarness(final Path rootDir,
                        final TcpTransportConfig peerTransportConfig,
                        final ClientTransportConfig clientTransportConfig,
                        final Duration repairInterval,
                        final boolean withFaultyTransport,
                        final long faultSeed) {
        this.rootDir = rootDir;
        this.peerTransportConfig = peerTransportConfig;
        this.clientTransportConfig = clientTransportConfig;
        this.repairInterval = repairInterval;

        final Map<Integer, Integer> ports = ChaosPortAllocator.allocateNodePorts(CLUSTER_SIZE);
        final Map<Integer, Integer> clientPorts = ChaosPortAllocator.allocateNodePorts(CLUSTER_SIZE);
        this.clusterAddresses = new HashMap<>();
        this.clientAddresses = new HashMap<>();
        this.nodeStorageDirs = new HashMap<>();

        for (int nodeId = 1; nodeId <= CLUSTER_SIZE; nodeId++) {
            clusterAddresses.put(nodeId, new InetSocketAddress("127.0.0.1", ports.get(nodeId)));
            clientAddresses.put(nodeId, new InetSocketAddress("127.0.0.1", clientPorts.get(nodeId)));
            nodeStorageDirs.put(nodeId, rootDir.resolve("node-" + nodeId));
        }

        this.withFaults = withFaultyTransport;
        if (withFaultyTransport) {
            // Stand up a frame-aware proxy in front of each single-dialer connection:
            // the lower id dials the proxy, which forwards to the higher id's real port.
            for (int low = 1; low <= CLUSTER_SIZE; low++) {
                for (int high = low + 1; high <= CLUSTER_SIZE; high++) {
                    final ChaosProxy proxy = new ChaosProxy(
                            clusterAddresses.get(high), faultSeed ^ pairKey(low, high));
                    proxy.start();
                    proxies.put(pairKey(low, high), proxy);
                }
            }
        }
    }

    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    private static long pairKey(final int a, final int b) {
        return ((long) Math.min(a, b) << 32) | (Math.max(a, b) & 0xFFFFFFFFL);
    }

    /**
     * The addresses node {@code nodeId} advertises: for peers it dials (higher ids) the
     * proxy listen address (fault-injected), otherwise the peer's real port (unused for
     * dialing under single-dialer).
     */
    private Map<NodeId, InetSocketAddress> memberAddressesFor(final int nodeId) {
        final Map<NodeId, InetSocketAddress> byNid = new HashMap<>();
        for (int peer = 1; peer <= CLUSTER_SIZE; peer++) {
            final InetSocketAddress addr = (withFaults && peer > nodeId)
                    ? proxies.get(pairKey(nodeId, peer)).listenAddress()
                    : clusterAddresses.get(peer);
            byNid.put(nid(peer), addr);
        }
        return byNid;
    }

    void startCluster(final int clientCount) {
        for (int nodeId = 1; nodeId <= CLUSTER_SIZE; nodeId++) {
            startNode(nodeId);
        }

        for (int i = 0; i < clientCount; i++) {
            final EventLoop loop = new EventLoop("chaos-client-" + i);
            final ClientId clientId = ClientId.of("chaos-client-" + i);
            final InProcessClientTransport transport =
                    new InProcessClientTransport(loop, List.of(nid(1), nid(2), nid(3)), clientId);
            final DisCasClient client = new DisCasClient(clientId, transport, loop);
            clients.add(client);
        }
    }

    void startNode(final int nodeId) {
        if (nodes.containsKey(nodeId)) {
            return;
        }
        try {
            final Path storageDir = nodeStorageDirs.get(nodeId);
            final StorageConfig storageConfig = StorageConfig.builder()
                    .baseDirectory(storageDir)
                    .walMaxFileBytes(8 * 1024 * 1024)
                    .snapshotRetentionCount(2)
                    .build();
            final FileWal wal = new FileWal(storageConfig);
            wal.initialize();

            final NodeConfig nodeConfig = NodeConfig.builder()
                    .nodeId(nid(nodeId))
                    .clusterId(ClusterId.of("chaos-cluster"))
                    .clusterSize(CLUSTER_SIZE)
                    .repairInterval(repairInterval)
                    .build();
            final TcpPeerBootstrap bootstrap = new TcpPeerBootstrap(
                    clusterAddresses.get(nodeId),
                    InMemoryMembers.ofTcp(memberAddressesFor(nodeId)),
                    peerTransportConfig);
            final DisCasNode node = DisCasNodeFactory.create(nodeConfig, bootstrap, wal);
            // Clients are driven through the in-process path, so the in-proc registrar is hooked
            // up directly: a TCP bootstrap wires no client server transport.
            node.registerClientMessages(registrar ->
                    InProcessClientRegistry.register(nid(nodeId), node.loop(), registrar,
                            node.clusterSize()));
            node.start();
            nodes.put(nodeId, node);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start node " + nodeId, e);
        }
    }

    void stopNode(final int nodeId) {
        final DisCasNode node = nodes.remove(nodeId);
        if (node == null) {
            return;
        }
        // Proxies persist across node restarts (they front the fixed real bind ports).
        node.close();
    }

    void restartNode(final int nodeId, final Duration outage) {
        stopNode(nodeId);
        try {
            Thread.sleep(Math.max(0L, outage.toMillis()));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during node restart delay", e);
        }
        startNode(nodeId);
    }

    DisCasClient client(final int index) {
        return clients.get(index);
    }

    List<DisCasClient> clients() {
        return clients;
    }

    List<Integer> nodeIds() {
        return List.of(1, 2, 3);
    }

    /**
     * Returns a fault-control handle for the given node, mapping isolate/drop calls
     * onto the frame-aware {@link ChaosProxy}es. Only available when the harness was
     * constructed with {@code withFaultyTransport=true}.
     */
    FaultyLink faultyTransport(final int nodeId) {
        if (!withFaults) {
            throw new IllegalStateException("Harness was not configured with faults");
        }
        return new FaultyLink(nodeId);
    }

    /**
     * Per-node fault control, mapped onto the per-pair proxies (replaces the old
     * in-process {@code FaultyPeerTransport}). {@code node->peer} traffic is the proxy
     * <b>forward</b> direction when {@code node < peer} else <b>reverse</b>.
     */
    final class FaultyLink {
        private final int node;

        FaultyLink(final int node) {
            this.node = node;
        }

        private ChaosProxy.Direction dir(final int from, final int to) {
            final ChaosProxy proxy = proxies.get(pairKey(from, to));
            return from < to ? proxy.forward() : proxy.reverse();
        }

        void isolateOutbound(final int target) {
            dir(node, target).isolate(true);
        }

        void isolateInbound(final int source) {
            dir(source, node).isolate(true);
        }

        void isolate(final int peer) {
            isolateOutbound(peer);
            isolateInbound(peer);
        }

        void healOutbound(final int target) {
            dir(node, target).isolate(false);
        }

        void healInbound(final int source) {
            dir(source, node).isolate(false);
        }

        void heal(final int peer) {
            healOutbound(peer);
            healInbound(peer);
        }

        void healAll() {
            for (int peer = 1; peer <= CLUSTER_SIZE; peer++) {
                if (peer != node) {
                    heal(peer);
                }
            }
        }

        void dropProbability(final double p) {
            for (int peer = 1; peer <= CLUSTER_SIZE; peer++) {
                if (peer == node) {
                    continue;
                }
                dir(node, peer).dropProbability(p); // this node's outbound to peer
                dir(peer, node).dropProbability(p); // this node's inbound from peer
            }
        }
    }

    void restartAllSequentially(final Duration outage) {
        for (final int nodeId : nodeIds()) {
            restartNode(nodeId, outage);
        }
    }

    /**
     * Waits until the cluster reaches a fixed point where two consecutive
     * per-client snapshots of {@code keys} are identical AND every client agrees
     * on every key. Returns the agreed key->value map.
     * <p>
     * This is runner-speed-independent: a fast runner converges in ~50 ms
     * (two rounds); a slow runner takes more rounds but only exits on the
     * invariant. {@code maxWait} is a "never expected to fire" safety wall --
     * if it does, quiescence never happened, which is a real bug (permanent
     * divergence) and the AssertionError includes a full diagnostics dump.
     * <p>
     * Preconditions: workload and nemesis must be stopped before calling.
     * Otherwise there is no fixed point.
     */
    Map<String, String> awaitClusterQuiescent(final List<String> keys,
                                              final Duration maxWait) throws Exception {
        final long deadlineNanos = System.nanoTime() + maxWait.toNanos();
        Map<Integer, Map<String, String>> previous = null;
        Map<Integer, Map<String, String>> current = null;
        long backoffMs = 25L;
        int iterations = 0;
        while (System.nanoTime() < deadlineNanos) {
            iterations++;
            current = snapshotAllClients(keys);
            final Map<String, String> agreed = allClientsAgree(current);
            if (agreed != null && current.equals(previous)) {
                return agreed;
            }
            previous = current;
            Thread.sleep(backoffMs);
            backoffMs = Math.min(200L, backoffMs * 2L);
        }
        throw new AssertionError(quiescenceDiagnostics(keys, maxWait, iterations, current, previous));
    }

    /**
     * Reads every {@code key} from every client in parallel via
     * CompletableFuture.allOf. If any read fails, the failing entry is marked
     * with a sentinel string that will never agree with any real value, so the
     * fixed-point detector will not converge on a partial round.
     */
    private Map<Integer, Map<String, String>> snapshotAllClients(final List<String> keys) throws Exception {
        final int n = clients.size();
        final List<Map<String, CompletableFuture<GetResult>>> perClientFutures = new ArrayList<>(n);
        final List<CompletableFuture<?>> allFutures = new ArrayList<>(n * keys.size());
        for (int c = 0; c < n; c++) {
            final Map<String, CompletableFuture<GetResult>> perClient = new LinkedHashMap<>();
            for (final String key : keys) {
                final CompletableFuture<GetResult> f = clients.get(c).get(ChaosWorkload.buf(key));
                perClient.put(key, f);
                allFutures.add(f);
            }
            perClientFutures.add(perClient);
        }
        try {
            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
                    .get(SNAPSHOT_ROUND_WALL_SECONDS, TimeUnit.SECONDS);
        } catch (final Exception ignored) {
            // Individual failures are captured per-key below; the whole round
            // is still returned so the caller can retry.
        }
        final Map<Integer, Map<String, String>> snapshot = new LinkedHashMap<>();
        for (int c = 0; c < n; c++) {
            final Map<String, String> row = new LinkedHashMap<>();
            for (final Map.Entry<String, CompletableFuture<GetResult>> e : perClientFutures.get(c).entrySet()) {
                final CompletableFuture<GetResult> f = e.getValue();
                String value;
                if (!f.isDone() || f.isCompletedExceptionally() || f.isCancelled()) {
                    value = SNAPSHOT_READ_FAILED;
                } else {
                    try {
                        value = ChaosWorkload.str(f.get().value());
                    } catch (final Exception fx) {
                        value = SNAPSHOT_READ_FAILED;
                    }
                }
                row.put(e.getKey(), value);
            }
            snapshot.put(c, row);
        }
        return snapshot;
    }

    /**
     * If every client's row is equal AND no read failed, returns the shared
     * map; otherwise returns null.
     */
    private static Map<String, String> allClientsAgree(final Map<Integer, Map<String, String>> snapshot) {
        Map<String, String> first = null;
        for (final Map<String, String> row : snapshot.values()) {
            if (row.containsValue(SNAPSHOT_READ_FAILED)) {
                return null;
            }
            if (first == null) {
                first = row;
            } else if (!first.equals(row)) {
                return null;
            }
        }
        return first;
    }

    private static String quiescenceDiagnostics(final List<String> keys,
                                                final Duration maxWait,
                                                final int iterations,
                                                final Map<Integer, Map<String, String>> current,
                                                final Map<Integer, Map<String, String>> previous) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Cluster did not reach quiescence within ").append(maxWait).append(".\n");
        sb.append("Iterations: ").append(iterations).append('\n');
        sb.append("Last snapshot (client -> {key -> value}):\n");
        if (current != null) {
            for (final Map.Entry<Integer, Map<String, String>> e : current.entrySet()) {
                sb.append("  client[").append(e.getKey()).append("]: ").append(e.getValue()).append('\n');
            }
        }
        if (current != null && !current.isEmpty()) {
            sb.append("Disagreements (key -> per-client values):\n");
            boolean any = false;
            for (final String key : keys) {
                final Map<Integer, String> perClient = new LinkedHashMap<>();
                final Set<String> distinct = new TreeSet<>();
                for (final Map.Entry<Integer, Map<String, String>> e : current.entrySet()) {
                    final String v = e.getValue().get(key);
                    perClient.put(e.getKey(), v);
                    distinct.add(String.valueOf(v));
                }
                if (distinct.size() > 1) {
                    sb.append("  ").append(key).append(": ").append(perClient).append('\n');
                    any = true;
                }
            }
            if (!any) {
                sb.append("  (all clients agree; blocked by consecutive-snapshot mismatch)\n");
            }
        }
        if (previous != null && current != null) {
            sb.append("Delta vs. previous snapshot:\n");
            boolean any = false;
            for (final Integer c : current.keySet()) {
                final Map<String, String> curr = current.get(c);
                final Map<String, String> prev = previous.get(c);
                if (prev == null) {
                    continue;
                }
                for (final Map.Entry<String, String> e : curr.entrySet()) {
                    final String p = prev.get(e.getKey());
                    if (!Objects.equals(p, e.getValue())) {
                        sb.append("  client[").append(c).append("].").append(e.getKey())
                                .append(": ").append(p).append(" -> ").append(e.getValue()).append('\n');
                        any = true;
                    }
                }
            }
            if (!any) {
                sb.append("  (no changes since previous snapshot -- permanently stuck)\n");
            }
        }
        return sb.toString();
    }

    private static final long SNAPSHOT_ROUND_WALL_SECONDS = 25L;
    private static final String SNAPSHOT_READ_FAILED = "<snapshot-read-failed>";

    @Override
    public void close() {
        for (final DisCasClient client : clients) {
            try {
                client.close();
            } catch (final Exception ignored) {
            }
        }
        clients.clear();

        for (final int nodeId : new ArrayList<>(nodes.keySet())) {
            try {
                stopNode(nodeId);
            } catch (final Exception ignored) {
            }
        }

        for (final ChaosProxy proxy : proxies.values()) {
            try {
                proxy.close();
            } catch (final Exception ignored) {
            }
        }
        proxies.clear();
    }
}
