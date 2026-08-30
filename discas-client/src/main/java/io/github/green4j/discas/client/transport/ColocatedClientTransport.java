/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.client.ClientObserver;
import io.github.green4j.discas.client.ClusterClock;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.TransportSetupException;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A {@link ClientTransport} for a client running inside one of the cluster's own members: the hop
 * to <em>that</em> member is made in process, and every other member is reached over TCP.
 * <p>
 * The local hop skips the socket and the codec both -- a {@link ClientMessage} is handed to the
 * node as an object. What it does not skip is any part of the protocol above the wire: the request
 * is the same request, coordinated the same way, and answered on the same terms.
 *
 * <h2>Routing, and why the coordinator choice is untouched</h2>
 * The only decision made here is by node id: the local member goes one way, everyone else the
 * other. <em>Which</em> member coordinates a key remains the client's decision, and
 * {@link #peers()} therefore delegates to the TCP transport rather than rebuilding a list of its
 * own -- so a colocated client and a plain TCP client built from the same address map enumerate
 * their peers identically and pick the same coordinator for the same key. That matters more than
 * it looks: coordinator affinity follows list position, and two clients that disagree about the
 * order route one key to two coordinators, which is a ballot duel rather than a serialized key.
 * <p>
 * The TCP transport is given the <b>full</b> address map, the local member's entry included, even
 * though nothing is ever routed to it there. Its own cross-check of the configured peers against
 * the {@code N} a node reports would otherwise see {@code N-1} and warn about a cluster that is
 * perfectly well configured.
 *
 * <h2>No fallback to the loopback</h2>
 * When the local node is not in {@link InProcessClientRegistry} -- it was closed, or never
 * constructed -- {@link #send} throws, and the client fails the attempt over to another
 * coordinator exactly as it does for an unreachable socket. It deliberately does <b>not</b> dial
 * the local member over TCP instead, because the two paths differ in what they prove about the
 * caller: in process the client's own {@link ClientId} is handed to the node as the trusted one,
 * with no handshake and no credential, whereas over TCP that identity is authenticated. The
 * <em>policy</em> is the same either way -- the node's authorizer keys off that id on both paths,
 * so a bound ACL grants and refuses identically -- but a transport that silently swapped an
 * asserted identity for a proven one, or the reverse, would make which of the two applied depend
 * on which member a key happened to hash to.
 * <p>
 * For the same reason the credentials are not optional here. {@code N-1} of the hops are ordinary
 * client connections and are authenticated as such; a colocated client built without the token or
 * the TLS material its cluster requires works for the keys that route locally and fails for the
 * rest.
 */
public final class ColocatedClientTransport implements ClientTransport {

    private final NodeId localNodeId;
    private final InProcessClientTransport local;
    private final TcpClientTransport remote;
    private volatile boolean closed = false;

    /**
     * @param loop           the loop both delegates run on -- the client's own, or the node's when
     *                       the deployment is meant to be single-threaded
     * @param localNodeId    the member this client lives inside; must be a key of
     *                       {@code nodeAddresses} and must already be registered with
     *                       {@link InProcessClientRegistry}, which a node does when it is
     *                       constructed
     * @param nodeAddresses  every member of the cluster, the local one included
     */
    public ColocatedClientTransport(
            final EventLoop loop,
            final NodeId localNodeId,
            final Map<NodeId, InetSocketAddress> nodeAddresses,
            final ClientTransportConfig config,
            final ClientId clientId,
            final String token,
            final ClientSecurityProvider securityProvider,
            final ClientObserver observer) {
        if (localNodeId == null) {
            throw new IllegalArgumentException("localNodeId is required");
        }
        if (nodeAddresses == null || !nodeAddresses.containsKey(localNodeId)) {
            throw new IllegalArgumentException(
                    "nodeAddresses must contain the local node " + localNodeId
                            + ": the client addresses the whole cluster, and leaving one member out"
                            + " changes which coordinator every key resolves to");
        }
        // Checked here rather than at the first send, where it would surface as one key failing
        // over for no visible reason. A node registers its client ingress when it is constructed,
        // so this only fails when the client was built first -- which is a wiring order to fix,
        // not a runtime condition to tolerate.
        if (InProcessClientRegistry.lookup(localNodeId) == null) {
            throw new TransportSetupException(TransportSetupException.Fault.UNKNOWN_TARGET,
                    "No in-process node registered as " + localNodeId
                            + ": construct the node before the colocated client");
        }
        this.localNodeId = localNodeId;
        this.local = new InProcessClientTransport(loop, List.of(localNodeId), clientId);
        this.remote = new TcpClientTransport(loop, nodeAddresses, config, clientId, token,
                securityProvider, observer);
    }

    /** The member this client lives inside, and the one target that never touches a socket. */
    public NodeId localNode() {
        return localNodeId;
    }

    @Override
    public void send(final NodeId targetNodeId, final ClientMessage message) {
        if (localNodeId.equals(targetNodeId)) {
            local.send(targetNodeId, message);
            return;
        }
        remote.send(targetNodeId, message);
    }

    @Override
    public void register(final Consumer<ClientMessage> handler) {
        local.register(handler);
        remote.register(handler);
    }

    /**
     * Only the TCP side can lose a connection; the in-process side has none to lose. A local node
     * that goes away is reported the other way -- by {@link #send} refusing the next request to it.
     */
    @Override
    public void registerConnectionLost(final Consumer<NodeId> handler) {
        remote.registerConnectionLost(handler);
    }

    @Override
    public List<NodeId> peers() {
        return remote.peers();
    }

    /**
     * Taken from the registry first: the local node knows the cluster's frozen {@code N} without a
     * connection, so a {@code scan} issued before any TCP handshake has completed can still work
     * out what a majority is instead of seeing 0.
     */
    @Override
    public int clusterSize() {
        final int localSize = local.clusterSize();
        return localSize > 0 ? localSize : remote.clusterSize();
    }

    /**
     * Bound on the TCP side alone. The clock is corrected against the coordinator's reading in the
     * hello response, and the local member has no hello -- but it also needs none, because its
     * clock <em>is</em> this process's clock and an offset of zero there is exact rather than
     * assumed.
     */
    @Override
    public void bindClock(final ClusterClock clock) {
        remote.bindClock(clock);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            local.close();
        } finally {
            remote.close();
        }
    }
}
