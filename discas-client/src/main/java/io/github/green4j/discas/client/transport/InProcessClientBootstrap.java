/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.transport;

import io.github.green4j.discas.common.identity.NodeId;

import io.github.green4j.discas.client.ClientObserver;

import java.util.ArrayList;
import java.util.List;

/**
 * What the client factory needs to build an in-process client: the nodes it may talk to, and
 * nothing else. There is no transport budget here, because {@link InProcessClientTransport} has no
 * sockets, frames or queues to spend one on.
 */
public final class InProcessClientBootstrap {
    public final List<NodeId> peers;
    /**
     * Observability seam; defaults to the silent {@link ClientObserver#NONE}, exactly as
     * {@link TcpClientBootstrap} does.
     */
    public final ClientObserver observer;

    public InProcessClientBootstrap(final List<NodeId> peers) {
        this(peers, ClientObserver.NONE);
    }

    public InProcessClientBootstrap(final List<NodeId> peers, final ClientObserver observer) {
        if (peers == null || peers.isEmpty()) {
            throw new IllegalArgumentException("peers are required");
        }
        this.peers = new ArrayList<>(peers);
        this.observer = observer == null ? ClientObserver.NONE : observer;
    }
}
