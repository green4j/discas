/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.membership;

import io.github.green4j.discas.common.identity.NodeId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TcpMemberInfo} holds a peer address <em>unresolved</em> and resolves it per connection
 * attempt, so a peer that moves behind a stable DNS name (a rescheduled pod, say) is dialed at
 * its current endpoint with no config reload.
 */
@DisplayName("TcpMemberInfo -- address held unresolved, resolved per attempt")
class TcpMemberInfoTest {

    private static final NodeId N1 = NodeId.of("1");

    @Test
    @DisplayName("The stored address view is unresolved and never triggers DNS")
    void addressIsHeldUnresolved() {
        final TcpMemberInfo member = new TcpMemberInfo(N1, "example.invalid", 9001);

        final InetSocketAddress view = member.unresolvedAddress();
        assertTrue(view.isUnresolved(),
                "Holding a resolved address would pin a peer to the endpoint it had at config load");
        assertEquals("example.invalid", view.getHostString());
        assertEquals(9001, view.getPort());
    }

    @Test
    @DisplayName("resolve() returns a fresh address object on every call")
    void resolveIsPerAttempt() {
        final TcpMemberInfo member = new TcpMemberInfo(N1, "127.0.0.1", 9001);

        final InetSocketAddress first = member.resolve();
        final InetSocketAddress second = member.resolve();

        assertFalse(first.isUnresolved());
        // Distinct instances: nothing is cached between attempts, which is what lets a moved
        // peer be reached without a config reload.
        assertNotSame(first, second);
        assertEquals(first, second);
    }

    @Test
    @DisplayName("Constructing from an InetSocketAddress does not pin its resolved endpoint")
    void fromSocketAddressKeepsHostString() {
        // A resolved loopback address: the member must keep the host string, not the resolution.
        final TcpMemberInfo member = new TcpMemberInfo(N1, new InetSocketAddress("127.0.0.1", 9001));

        assertEquals("127.0.0.1", member.host());
        assertEquals(9001, member.port());
        assertTrue(member.unresolvedAddress().isUnresolved());
    }

    @Test
    @DisplayName("Equality is by (nodeId, host, port), not by resolved endpoint")
    void equalityIsByHostAndPort() {
        assertEquals(new TcpMemberInfo(N1, "h", 1), new TcpMemberInfo(N1, "h", 1));
        assertEquals(new TcpMemberInfo(N1, "h", 1).hashCode(),
                new TcpMemberInfo(N1, "h", 1).hashCode());
        assertNotEquals(new TcpMemberInfo(N1, "h", 1), new TcpMemberInfo(N1, "h", 2));
        assertNotEquals(new TcpMemberInfo(N1, "h", 1), new TcpMemberInfo(NodeId.of("2"), "h", 1));
    }

    @Test
    @DisplayName("A missing host or out-of-range port is rejected at construction")
    void invalidInputsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new TcpMemberInfo(N1, "", 9001));
        assertThrows(IllegalArgumentException.class, () -> new TcpMemberInfo(N1, null, 9001));
        assertThrows(IllegalArgumentException.class, () -> new TcpMemberInfo(N1, "h", 0));
        assertThrows(IllegalArgumentException.class, () -> new TcpMemberInfo(N1, "h", 65_536));
        assertThrows(IllegalArgumentException.class,
                () -> new TcpMemberInfo(N1, (InetSocketAddress) null));
    }
}
