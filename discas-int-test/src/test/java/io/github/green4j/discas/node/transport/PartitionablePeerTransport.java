/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.PeerMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Decorator around PeerTransport that supports network partition injection.
 * Thread-safe: isolation sets use ConcurrentHashMap-backed sets. The
 * {@code isolate}/{@code heal} test API takes an {@code int} node id for
 * convenience and maps it to the string {@link NodeId} used on the wire.
 * <p>
 * {@code isolate} <em>drops</em> traffic; {@link #holdOutbound()} <em>defers</em> it. The
 * difference decides which failures a test can express. A dropped prepare makes a round fail
 * (its acceptors never answer); a held one makes the round <em>stall</em> and then complete
 * late, which is the only way to reproduce a coordinator the client has already given up on
 * still driving its proposal to a quorum.
 */
public final class PartitionablePeerTransport implements PeerTransport {

    /** One outbound message parked by {@link #holdOutbound()}, waiting to be delivered late. */
    private static final class Held {
        private final NodeId target;
        private final PeerMessage message;

        private Held(final NodeId target, final PeerMessage message) {
            this.target = target;
            this.message = message;
        }
    }

    private final PeerTransport delegate;
    private final NodeId nodeId;
    private final Set<NodeId> isolated = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final List<Held> held = new ArrayList<>();
    private volatile boolean holdingOutbound;

    public PartitionablePeerTransport(final NodeId nodeId, final PeerTransport delegate) {
        this.nodeId = nodeId;
        this.delegate = delegate;
    }

    /**
     * Drop all messages to/from the given node
     */
    public void isolate(final int targetNodeId) {
        isolated.add(NodeId.of(Integer.toString(targetNodeId)));
    }

    /**
     * Restore connectivity to the given node
     */
    public void heal(final int targetNodeId) {
        isolated.remove(NodeId.of(Integer.toString(targetNodeId)));
    }

    public boolean isIsolated(final int targetNodeId) {
        return isolated.contains(NodeId.of(Integer.toString(targetNodeId)));
    }

    /**
     * Park every outbound peer message instead of delivering it, until {@link #releaseOutbound()}.
     * <p>
     * The node keeps running: it still receives, still promises, still answers its own event loop.
     * What it cannot do is finish a round it started -- which is exactly the state a coordinator
     * is in when a client's per-attempt timeout expires and the request moves elsewhere.
     */
    public void holdOutbound() {
        holdingOutbound = true;
    }

    /**
     * Deliver everything {@link #holdOutbound()} parked, in the order it was sent, and resume
     * normal delivery.
     *
     * @return how many messages were released; zero means the hold never had anything to catch,
     *         because the node stayed silent throughout. It may be the wrong node, but it is just
     *         as likely to be the right one saying nothing: a node that is not yet {@code SERVING}
     *         sheds every peer message rather than answering it, and a client that has put it in
     *         peer backoff routes around it. Both leave the hold empty and the test asserting
     *         nothing, so callers should treat zero as a failure rather than a quiet pass.
     */
    public int releaseOutbound() {
        final List<Held> drained;
        synchronized (held) {
            holdingOutbound = false;
            drained = new ArrayList<>(held);
            held.clear();
        }
        for (final Held pending : drained) {
            delegate.send(pending.target, pending.message);
        }
        return drained.size();
    }

    public boolean isHoldingOutbound() {
        return holdingOutbound;
    }

    @Override
    public void send(final NodeId targetNodeId, final PeerMessage message) {
        if (isolated.contains(targetNodeId)) {
            return;
        }
        if (holdingOutbound) {
            synchronized (held) {
                if (holdingOutbound) {
                    held.add(new Held(targetNodeId, message));
                    return;
                }
            }
        }
        delegate.send(targetNodeId, message);
    }

    @Override
    public void register(final Consumer<PeerMessage> handler) {
        delegate.register(message -> {
            if (!isolated.contains(message.senderId())) {
                handler.accept(message);
            }
        });
    }

    @Override
    public List<NodeId> peers() {
        return delegate.peers();
    }

    @Override
    public int clusterSize() {
        return delegate.clusterSize();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
