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
import io.github.green4j.discas.common.transport.tls.ReloadableTlsContext;
import io.github.green4j.discas.common.transport.tls.TlsConfig;
import io.github.green4j.discas.common.transport.tls.TlsPeerSecurityProvider;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Zero-disruption cert rotation via {@link ReloadableTlsContext}. */
@DisplayName("mTLS cert rotation -- reloadable context")
class ReloadableTlsRotationTest {

    private static final ClusterId CLUSTER = ClusterId.of("rot-cluster");
    private static final NodeId N1 = NodeId.of("1");
    private static final NodeId N2 = NodeId.of("2");

    @Test
    @DisplayName("An established session keeps flowing after both peers rotate their certs")
    void establishedSessionSurvivesReload(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final ReloadableTlsContext serverCtx = ReloadableTlsContext.create(ca.material(N1));
        final ReloadableTlsContext clientCtx = ReloadableTlsContext.create(ca.material(N2));
        final PeerSecurityProvider server = new TlsPeerSecurityProvider(TlsConfig.of(serverCtx.sslContext()));
        final PeerSecurityProvider client = new TlsPeerSecurityProvider(TlsConfig.of(clientCtx.sslContext()));

        final PeerChannelSecurity srv = server.forInbound();
        final PeerChannelSecurity cli = client.forOutbound();
        pumpToCompletion(cli, srv);
        assertTrue(cli.handshakeFinished() && srv.handshakeFinished());

        // Rotate BOTH peers' certs on the live contexts (renewed leaves, same CA).
        serverCtx.reload(ca.material(N1));
        clientCtx.reload(ca.material(N2));

        // The already-established session must keep carrying application data.
        assertEquals("hello-after-rotation", roundTrip(cli, srv, "hello-after-rotation"));
        assertEquals("reply-after-rotation", roundTrip(srv, cli, "reply-after-rotation"));
    }

    @Test
    @DisplayName("A new handshake after rotation succeeds with the renewed material")
    void newHandshakeAfterReloadSucceeds(@TempDir final Path dir) throws Exception {
        final TestCa ca = new TestCa(Files.createDirectories(dir.resolve("pki")), CLUSTER);
        final ReloadableTlsContext serverCtx = ReloadableTlsContext.create(ca.material(N1));
        final ReloadableTlsContext clientCtx = ReloadableTlsContext.create(ca.material(N2));

        serverCtx.reload(ca.material(N1));
        clientCtx.reload(ca.material(N2));

        final PeerChannelSecurity srv =
                new TlsPeerSecurityProvider(TlsConfig.of(serverCtx.sslContext())).forInbound();
        final PeerChannelSecurity cli =
                new TlsPeerSecurityProvider(TlsConfig.of(clientCtx.sslContext())).forOutbound();
        pumpToCompletion(cli, srv);

        assertTrue(cli.handshakeFinished() && srv.handshakeFinished());
        assertEquals(N2, srv.peerCredential().nodeId());
        assertEquals(N1, cli.peerCredential().nodeId());
    }

    // Note: CertRotationManager applying pushed material (and no-op on unchanged files) is covered
    // by the file-backed material source's own tests, and the near-expiry warning threshold by the
    // rotation cadence tests.

    private static String roundTrip(final PeerChannelSecurity from, final PeerChannelSecurity to,
                                    final String message) {
        final ByteBuffer app = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer net = from.wrap(app);
        final ByteBuffer out = ByteBuffer.allocate(16 * 1024);
        to.unwrap(net, out);
        out.flip();
        final byte[] bytes = new byte[out.remaining()];
        out.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

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
            to.unwrap(net, ByteBuffer.allocate(32 * 1024));
        }
    }
}
