/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.security;

import java.nio.ByteBuffer;

/**
 * Identity {@link PeerChannelSecurity}: no encryption, no handshake, no peer
 * credential. Network bytes and application bytes are the same, so the peer
 * transport behaves exactly as a plain TCP transport. This is the default for
 * development, tests, and trusted-network deployments.
 * <p>
 * Stateless and shareable -- {@link #INSTANCE} is used for every connection; the
 * matching {@link PeerSecurityProvider} hands it out for both roles.
 */
public final class PlaintextPeerSecurity implements PeerChannelSecurity {

    public static final PlaintextPeerSecurity INSTANCE = new PlaintextPeerSecurity();

    public static final PeerSecurityProvider PROVIDER = new PeerSecurityProvider() {
        @Override
        public PeerChannelSecurity forInbound() {
            return INSTANCE;
        }

        @Override
        public PeerChannelSecurity forOutbound() {
            return INSTANCE;
        }
    };

    private PlaintextPeerSecurity() {
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
    public PeerCredential peerCredential() {
        return PeerCredential.NONE;
    }

    @Override
    public void close() {
        // stateless singleton -- nothing to release
    }
}
