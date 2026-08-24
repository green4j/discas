/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.client.ClientTransportConfig;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.membership.InMemoryMembers;
import io.github.green4j.discas.node.transport.TcpClientServerTransport;
import io.github.green4j.discas.node.transport.TcpPeerTransport;
import io.github.green4j.discas.node.transport.TcpTransportConfig;
import io.github.green4j.discas.common.transport.security.PlaintextPeerSecurity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Binding port 0 and reading the port back removes the probe-to-bind window entirely: there is
 * no interval in which the port is free and unclaimed, because the transport never lets go of it.
 */
@DisplayName("TCP transports -- bind :0 and report the port they got")
class BoundPortTest {

    @Test
    void clientServerBindsEphemeralAndReportsIt() {
        final EventLoop loop = new EventLoop("bound-port-client");
        loop.start();
        try (TcpClientServerTransport transport = new TcpClientServerTransport(
                loop, new InetSocketAddress("127.0.0.1", 0),
                ClientTransportConfig.defaults(), 1)) {
            assertNotEquals(0, transport.boundPort(), "Port 0 must resolve to a real port");
            assertTrue(transport.boundPort() > 0);
        } finally {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(5));
        }
    }

    /**
     * The peer transport binds {@code :0} happily -- but the members map it is constructed with
     * must already carry every member's real address (port 0 there is rejected outright). That is
     * the chicken-and-egg that keeps the peer mesh on pre-allocated ports: node 1 needs node 2's
     * address before node 2 exists. The client port has no such constraint, because clients
     * connect after the node is up.
     */
    @Test
    void peerBindsEphemeralAndReportsIt() {
        final EventLoop loop = new EventLoop("bound-port-peer");
        loop.start();
        final NodeId self = NodeId.of("1");
        try (TcpPeerTransport transport = new TcpPeerTransport(
                self, ClusterId.of("c"), 1, loop, new InetSocketAddress("127.0.0.1", 0),
                // a real address in the members map; the bind address below is still :0
                InMemoryMembers.ofTcp(Map.of(self, new InetSocketAddress("127.0.0.1", 1))),
                PlaintextPeerSecurity.PROVIDER, TcpTransportConfig.defaults())) {
            assertNotEquals(0, transport.boundPort(), "Port 0 must resolve to a real port");
            assertTrue(transport.boundPort() > 0);
        } finally {
            loop.shutdown();
            loop.awaitTermination(Duration.ofSeconds(5));
        }
    }
}
