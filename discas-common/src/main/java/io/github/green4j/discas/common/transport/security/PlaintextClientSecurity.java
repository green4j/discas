/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.security;

import io.github.green4j.discas.common.identity.ClientId;

import java.nio.ByteBuffer;

/**
 * Identity {@link ClientChannelSecurity}: no encryption, no handshake, no certificate.
 * Network and application bytes are the same, so the client transports behave exactly as
 * plain TCP. Default for development, tests, and trusted-network deployments -- the
 * client-side analogue of {@link PlaintextPeerSecurity}.
 */
public final class PlaintextClientSecurity implements ClientChannelSecurity {

    public static final PlaintextClientSecurity INSTANCE = new PlaintextClientSecurity();

    public static final ClientSecurityProvider PROVIDER = new ClientSecurityProvider() {
        @Override
        public ClientChannelSecurity forInbound() {
            return INSTANCE;
        }

        @Override
        public ClientChannelSecurity forOutbound() {
            return INSTANCE;
        }
    };

    private PlaintextClientSecurity() {
    }

    @Override
    public boolean handshakeFinished() {
        return true;
    }

    @Override
    public void unwrap(final ByteBuffer net, final ByteBuffer app) {
        final int n = Math.min(net.remaining(), app.remaining());
        if (n == 0) {
            return;
        }
        final int savedLimit = net.limit();
        net.limit(net.position() + n);
        app.put(net);
        net.limit(savedLimit);
    }

    @Override
    public ByteBuffer wrap(final ByteBuffer app) {
        return app;
    }

    @Override
    public ByteBuffer pendingOutbound() {
        return null;
    }

    @Override
    public ClientId peerClientId() {
        return null;
    }

    @Override
    public void close() {
        // stateless singleton -- nothing to release
    }
}
