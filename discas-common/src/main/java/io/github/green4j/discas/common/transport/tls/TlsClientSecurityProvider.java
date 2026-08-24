/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.transport.security.ClientChannelSecurity;
import io.github.green4j.discas.common.transport.security.ClientSecurityProvider;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * {@link ClientSecurityProvider} that mints per-connection {@link TlsClientSecurity}
 * contexts from a shared {@link TlsConfig} -- the client-side analogue of
 * {@code TlsPeerSecurityProvider}.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>mTLS</b> (default, {@link #TlsClientSecurityProvider(TlsConfig)}): the inbound
 *   (node client-server) side requires a client certificate ({@code needClientAuth}) so the
 *   CN-bound {@code ClientId} authenticates the connection; the outbound (client) side
 *   presents the client's own certificate.</li>
 *   <li><b>Server-authenticated TLS only</b> ({@link #serverAuthOnly(TlsConfig)}): the
 *   inbound side does <b>not</b> require a client certificate -- the channel is encrypted
 *   and the server is authenticated, but the client identity comes from the CLIENT_HELLO
 *   credential (e.g. a token) rather than a cert. The outbound client presents no cert (its
 *   {@code TlsConfig} is typically trust-only, see {@code TlsContexts.buildTrustOnly}).</li>
 * </ul>
 */
public final class TlsClientSecurityProvider implements ClientSecurityProvider {

    private final TlsConfig config;
    private final boolean requireClientAuth;

    /** mTLS: the inbound side requires a client certificate. */
    public TlsClientSecurityProvider(final TlsConfig config) {
        this(config, true);
    }

    private TlsClientSecurityProvider(final TlsConfig config, final boolean requireClientAuth) {
        this.config = config;
        this.requireClientAuth = requireClientAuth;
    }

    /**
     * Server-authenticated-TLS-only: the inbound side encrypts the channel and authenticates
     * the server but does not require a client certificate (identity comes from the hello
     * credential). Pair the server with a token {@code ClientAuthenticator}.
     */
    public static TlsClientSecurityProvider serverAuthOnly(final TlsConfig config) {
        return new TlsClientSecurityProvider(config, false);
    }

    @Override
    public ClientChannelSecurity forInbound() {
        return new TlsClientSecurity(newEngine(false));
    }

    @Override
    public ClientChannelSecurity forOutbound() {
        return new TlsClientSecurity(newEngine(true));
    }

    private SSLEngine newEngine(final boolean clientMode) {
        final SSLEngine engine = config.sslContext().createSSLEngine();
        engine.setUseClientMode(clientMode);
        final SSLParameters params = engine.getSSLParameters();
        if (!clientMode) {
            params.setNeedClientAuth(requireClientAuth);
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
