/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.identity.NodeId;

/**
 * What {@link ColocatedClientTransport} needs: which member this client lives inside, and how to
 * reach the cluster it belongs to.
 * <p>
 * The second half is a whole {@link TcpClientBootstrap} rather than a repetition of its fields,
 * and that is the point rather than an economy. All but one of a colocated client's hops are
 * ordinary client connections -- authenticated, and encrypted if the cluster says so -- so it
 * needs everything a plain TCP client needs. A bootstrap that let the caller leave the token or
 * the TLS material out "because we are local" would build a client that works for the keys that
 * route to the local member and fails for every other key, which reads like a flapping network
 * rather than like the configuration mistake it is.
 */
public final class ColocatedClientBootstrap {

    /** The member this client runs inside. Must be one of {@code cluster}'s addresses. */
    public final NodeId localNodeId;
    /** How the other members are reached, and the credentials every one of them will ask for. */
    public final TcpClientBootstrap cluster;

    public ColocatedClientBootstrap(final NodeId localNodeId, final TcpClientBootstrap cluster) {
        if (localNodeId == null) {
            throw new IllegalArgumentException("localNodeId is required");
        }
        if (cluster == null) {
            throw new IllegalArgumentException("Cluster bootstrap is required");
        }
        if (!cluster.nodeAddresses.containsKey(localNodeId)) {
            throw new IllegalArgumentException(
                    "The cluster bootstrap must address the local node " + localNodeId
                            + ": a client enumerates the whole membership, and a list missing one"
                            + " member resolves every key to a different coordinator than the rest"
                            + " of the cluster's clients do");
        }
        this.localNodeId = localNodeId;
        this.cluster = cluster;
    }
}
