/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.NodeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-global rendezvous between an in-process client transport and the
 * node it targets. A node registers its client ingress (keyed by node id);
 * the client-side in-process transport looks the node up by id to deliver
 * requests and receive responses on the node's event loop.
 * <p>
 * Kept in {@code common.client} because both the client module and the node
 * module must reach it, and neither may depend on the other.
 */
public final class InProcessClientRegistry {

    /**
     * A registered node's in-process client endpoint: the loop the node
     * runs on, and the ingress that consumes a client message together with
     * the sink to reply through.
     */
    public static final class NodeEndpoint {
        public final EventLoop loop;
        public final ClientIngress clientIngress;
        /**
         * The node's frozen cluster size {@code N}. The in-process counterpart of the
         * {@code clusterSize} a TCP node reports in CLIENT_HELLO_RESP: it lets a client
         * derive the real quorum rather than assuming its own node list is complete.
         */
        public final int clusterSize;

        NodeEndpoint(final EventLoop loop,
                     final ClientIngress clientIngress,
                     final int clusterSize) {
            this.loop = loop;
            this.clientIngress = clientIngress;
            this.clusterSize = clusterSize;
        }
    }

    private static final Map<NodeId, NodeEndpoint> NODE_ENDPOINTS = new ConcurrentHashMap<>();

    private InProcessClientRegistry() {
    }

    public static void register(
            final NodeId nodeId,
            final EventLoop nodeLoop,
            final ClientIngress clientIngress,
            final int clusterSize) {
        if (clusterSize < 1 || clusterSize > 255) {
            throw new IllegalArgumentException(
                    "clusterSize must be in [1, 255], got " + clusterSize);
        }
        NODE_ENDPOINTS.put(nodeId, new NodeEndpoint(nodeLoop, clientIngress, clusterSize));
    }

    public static void unregister(final NodeId nodeId) {
        NODE_ENDPOINTS.remove(nodeId);
    }

    public static NodeEndpoint lookup(final NodeId nodeId) {
        return NODE_ENDPOINTS.get(nodeId);
    }
}
