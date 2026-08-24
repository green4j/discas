/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.transport.security.PeerChannelSecurity;
import io.github.green4j.discas.common.transport.security.PeerSecurityProvider;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * {@link PeerSecurityProvider} that mints per-connection {@link TlsPeerSecurity}
 * contexts from a shared {@link TlsConfig}. Every engine -- inbound (server role)
 * and outbound (client role) -- requires a peer certificate ({@code
 * needClientAuth}); the full mesh is symmetric with no server-privilege
 * asymmetry.
 */
public final class TlsPeerSecurityProvider implements PeerSecurityProvider {

    private final TlsConfig config;

    public TlsPeerSecurityProvider(final TlsConfig config) {
        this.config = config;
    }

    @Override
    public PeerChannelSecurity forInbound() {
        return new TlsPeerSecurity(newEngine(false));
    }

    @Override
    public PeerChannelSecurity forOutbound() {
        return new TlsPeerSecurity(newEngine(true));
    }

    private SSLEngine newEngine(final boolean clientMode) {
        final SSLEngine engine = config.sslContext().createSSLEngine();
        engine.setUseClientMode(clientMode);
        final SSLParameters params = engine.getSSLParameters();
        if (!clientMode) {
            params.setNeedClientAuth(true);
        }
        if (config.protocols() != null) {
            params.setProtocols(config.protocols());
        }
        if (config.cipherSuites() != null) {
            params.setCipherSuites(config.cipherSuites());
        }
        engine.setSSLParameters(params);
        return engine;
    }
}
