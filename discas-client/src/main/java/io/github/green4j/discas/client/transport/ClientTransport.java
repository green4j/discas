/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.client.ClusterClock;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.common.identity.NodeId;

import java.util.List;
import java.util.function.Consumer;

/**
 * The client's side of the wire: outbound requests to cluster members and the responses that come
 * back. There is no server socket and no inbound connection to accept.
 */
public interface ClientTransport extends AutoCloseable {

    /**
     * Send a request to a specific peer
     */
    void send(NodeId targetNodeId, ClientMessage message);

    /**
     * Register inbound response handler
     */
    void register(Consumer<ClientMessage> handler);

    /**
     * Register a handler told which peer a connection was just lost to.
     * <p>
     * A dropped socket is the transport's private business right up until a request is riding on
     * it. The transport knows immediately; without this the caller finds out only when its
     * per-attempt timer expires, so a coordinator that died the instant after accepting a request
     * costs a full timeout that nobody needed to wait for.
     * <p>
     * Called only for connections that were dropped unexpectedly -- not during
     * {@link #close()}, where every pending request is being failed anyway.
     * <p>
     * Default no-op: an implementation that cannot lose a connection, such as an in-process one,
     * has nothing to report.
     */
    default void registerConnectionLost(final Consumer<NodeId> handler) {
        // no-op
    }

    /**
     * Node IDs this client is configured to talk to. <b>Should be the full cluster membership.</b>
     * <p>
     * These are the <em>coordinators</em> the client may address. Giving it every node is the
     * intended contract, and the transport cross-checks this list against the size the cluster
     * reports, warning when they differ.
     * <p>
     * A shorter list still works and is not rejected -- the agent's
     * {@code --nodes-file} bootstrap starts on a subset and reloads to the full membership. But it
     * degrades two things: {@code get}/{@code put}/{@code cas}/{@code delete} keep working (the
     * receiving node coordinates a full round across all {@code N}) yet can only fail over among
     * the listed nodes, and {@code scan} needs a majority of the real {@code N} to respond -- see
     * {@link #clusterSize()}.
     */
    List<NodeId> peers();

    /**
     * The cluster's authoritative size {@code N} as reported by a node, or {@code 0} if no
     * node has been reached yet.
     * <p>
     * {@code scan} is the one operation the client aggregates itself (each node answers from
     * its local store, with no consensus round), so it needs the real {@code N} to know what
     * a majority is. Deriving {@code N} from {@link #peers()} would be wrong whenever the
     * configured list is a subset of the cluster: 2 responses out of a 5-node cluster look
     * like a majority of 3 but need not intersect a committing quorum.
     */
    int clusterSize();

    /**
     * Hand the transport the clock it should report coordinator time into.
     * <p>
     * The client owns the clock -- it is what lock leases are expressed on -- but only the transport
     * sees a handshake, which is where the coordinator's own reading arrives. Called once, during
     * client construction, before anything is sent.
     * <p>
     * Default no-op, and correct for an in-process transport: the "cluster" is this JVM, so its
     * clock <em>is</em> this client's clock and an offset of zero is not an approximation.
     */
    default void bindClock(final ClusterClock clock) {
        // no-op
    }

    /**
     * Release transport resources. Default no-op for legacy implementations.
     */
    @Override
    default void close() {
        // no-op
    }
}
