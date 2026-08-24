/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.node.NodeObserver;
import io.github.green4j.discas.node.PeerMessage;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.InProcessClientRegistry;
import io.github.green4j.discas.common.transport.TransportSetupException;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.membership.MemberInfo;
import io.github.green4j.discas.node.membership.Members;
import io.github.green4j.discas.node.membership.MembersSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * In-process peer transport for local tests and deterministic execution
 */
public final class InProcessPeerTransport implements PeerTransport {
    private static final class Endpoint {
        final EventLoop loop;
        final Consumer<PeerMessage> handler;

        private Endpoint(final EventLoop loop, final Consumer<PeerMessage> handler) {
            this.loop = loop;
            this.handler = handler;
        }
    }

    private static final Map<NodeId, Endpoint> ENDPOINTS = new ConcurrentHashMap<>();

    private final NodeId nodeId;
    private final int clusterSize;
    private final EventLoop loop;
    private final List<NodeId> peers;
    private final NodeObserver observer;
    private Consumer<PeerMessage> handler;
    private volatile boolean closed = false;

    public InProcessPeerTransport(final NodeId nodeId,
                                  final int clusterSize,
                                  final EventLoop loop,
                                  final Members<MemberInfo> members) {
        this(nodeId, clusterSize, loop, members, NodeObserver.NONE);
    }

    /**
     * @param observer receives the synthetic peer-lifecycle events described on
     *                 {@link #register(Consumer)}. {@link NodeObserver#NONE} is silent.
     */
    public InProcessPeerTransport(final NodeId nodeId,
                                  final int clusterSize,
                                  final EventLoop loop,
                                  final Members<MemberInfo> members,
                                  final NodeObserver observer) {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.loop = loop;
        this.observer = observer == null ? NodeObserver.NONE : observer;
        final MembersSnapshot<MemberInfo> snap = members.snapshot();
        if (snap.ids().size() != clusterSize) {
            throw new IllegalArgumentException("Member list must define exactly N=" + clusterSize
                    + " nodes, found " + snap.ids().size());
        }
        if (!snap.contains(nodeId)) {
            throw new IllegalArgumentException("Member list must include this node ("
                    + nodeId.value() + ")");
        }
        final List<NodeId> otherNodes = new ArrayList<>();
        for (final NodeId id : snap.ids()) {
            if (!id.equals(nodeId)) {
                otherNodes.add(id);
            }
        }
        this.peers = Collections.unmodifiableList(otherNodes);
    }

    @Override
    public int clusterSize() {
        return clusterSize; // explicit N (frozen), like the TCP transport
    }

    @Override
    public void send(final NodeId targetNodeId, final PeerMessage message) {
        final Endpoint endpoint = ENDPOINTS.get(targetNodeId);
        if (endpoint == null || endpoint.handler == null) {
            throw new TransportSetupException(TransportSetupException.Fault.UNKNOWN_TARGET,
                    "No in-process peer endpoint registered for node " + targetNodeId);
        }
        endpoint.loop.execute(() -> endpoint.handler.accept(message));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Also reports every configured peer as handshaked. There is no socket and no PEER_HELLO here,
     * so membership <em>is</em> connectivity: an in-process peer cannot be unreachable, cannot fail
     * a handshake, and needs no dial. Emitting the events anyway is what lets readiness -- and every
     * in-process test of it -- run the same state machine as the TCP transport instead of a
     * special case that is never exercised.
     */
    @Override
    public void register(final Consumer<PeerMessage> handler) {
        this.handler = handler;
        ENDPOINTS.put(nodeId, new Endpoint(loop, handler));
        for (int i = 0; i < peers.size(); i++) {
            observer.peerHandshakeCompleted(peers.get(i));
        }
    }

    @Override
    public List<NodeId> peers() {
        return peers;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        ENDPOINTS.remove(nodeId);
        InProcessClientRegistry.unregister(nodeId);
        handler = null;
        for (int i = 0; i < peers.size(); i++) {
            observer.peerDisconnected(peers.get(i), "transport closed");
        }
    }
}
