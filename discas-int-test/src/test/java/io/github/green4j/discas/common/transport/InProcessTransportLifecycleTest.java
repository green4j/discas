/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import io.github.green4j.discas.node.membership.InMemoryMembers;

import io.github.green4j.discas.node.transport.InProcessPeerTransport;

import io.github.green4j.discas.node.PeerMessage;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.NodeId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("In-process transport lifecycle -- register / close / message flow")
class InProcessTransportLifecycleTest {
    private static NodeId nid(final int id) {
        return NodeId.of(Integer.toString(id));
    }

    @Test
    void peerCloseUnregistersEndpoint() {
        final EventLoop loop = new EventLoop("inproc-peer");
        final InProcessPeerTransport peer = new InProcessPeerTransport(nid(2), 2, loop,
                InMemoryMembers.ofNodes(List.of(nid(1), nid(2))));
        peer.register(message -> {
        });
        peer.close();

        final InProcessPeerTransport sender = new InProcessPeerTransport(nid(1), 2, loop,
                InMemoryMembers.ofNodes(List.of(nid(1), nid(2))));
        assertThrows(RuntimeException.class,
                () -> sender.send(nid(2), new PeerMessage.DigestReq(nid(1), 1L)));
    }

}
