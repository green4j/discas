/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.tls;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.PeerChannelSecurity;
import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic, cluster-free tests of {@link io.github.green4j.discas.common.transport.tls.TlsPeerSecurity}
 * by pumping handshake bytes between two engines in-process.
 */
@DisplayName("mTLS peer channel security -- SSLEngine handshake")
class TlsPeerHandshakeTest {

    private static final ClusterId CLUSTER = ClusterId.of("tls-cluster");

    @Test
    @DisplayName("Two peers with certs from the same cluster CA complete the handshake and expose cert identity")
    void sameCaHandshakeSucceeds(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(dir, CLUSTER);
        final PeerSecurityProvider server = ca.provider(NodeId.of("1"));
        final PeerSecurityProvider client = ca.provider(NodeId.of("2"));

        final PeerChannelSecurity srv = server.forInbound();
        final PeerChannelSecurity cli = client.forOutbound();

        pumpToCompletion(cli, srv);

        assertTrue(cli.handshakeFinished() && srv.handshakeFinished(), "Both sides finish");
        // Server sees the client's (node 2) cert; client sees the server's (node 1) cert.
        assertTrue(srv.peerCredential().authenticated());
        assertEquals(NodeId.of("2"), srv.peerCredential().nodeId());
        assertEquals(CLUSTER, srv.peerCredential().clusterId());
        assertTrue(cli.peerCredential().authenticated());
        assertEquals(NodeId.of("1"), cli.peerCredential().nodeId());
    }

    @Test
    @DisplayName("A peer whose cert is signed by a foreign CA is rejected during the handshake")
    void foreignCaHandshakeFails(@TempDir final Path dir) throws Exception {
        final Path aDir = Files.createDirectories(dir.resolve("a"));
        final Path bDir = Files.createDirectories(dir.resolve("b"));
        final TestCa clusterCa = new TestCa(aDir, CLUSTER);
        final TestCa foreignCa = new TestCa(bDir, ClusterId.of("foreign"));

        final PeerChannelSecurity srv = clusterCa.provider(NodeId.of("1")).forInbound();
        final PeerChannelSecurity cli = foreignCa.provider(NodeId.of("2")).forOutbound();

        // The server trusts only the cluster CA; the client presents a foreign-CA
        // cert, so the mutual handshake must fail rather than complete.
        assertThrows(RuntimeException.class, () -> pumpToCompletion(cli, srv));
    }

    /** Shuttle handshake records between two engines until both finish (or one throws). */
    private static void pumpToCompletion(final PeerChannelSecurity a, final PeerChannelSecurity b) {
        for (int i = 0; i < 100; i++) {
            deliver(a, b);
            deliver(b, a);
            if (a.handshakeFinished() && b.handshakeFinished()) {
                return;
            }
        }
        throw new IllegalStateException("Handshake did not converge");
    }

    private static void deliver(final PeerChannelSecurity from, final PeerChannelSecurity to) {
        ByteBuffer net;
        while ((net = from.pendingOutbound()) != null) {
            final ByteBuffer app = ByteBuffer.allocate(32 * 1024);
            to.unwrap(net, app);
        }
    }
}
