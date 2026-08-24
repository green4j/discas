/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import javax.net.ssl.SSLContext;

/**
 * Immutable mTLS settings for the peer transport: the {@link SSLContext} plus
 * the enabled protocols and (optional) cipher-suite allow-list. Mutual
 * authentication ({@code needClientAuth}) is enforced on every peer engine --
 * the full mesh is symmetric, so both roles require a client cert.
 */
public final class TlsConfig {

    private final SSLContext sslContext;
    private final String[] protocols;
    private final String[] cipherSuites;

    private TlsConfig(final SSLContext sslContext, final String[] protocols,
                      final String[] cipherSuites) {
        this.sslContext = sslContext;
        this.protocols = protocols;
        this.cipherSuites = cipherSuites;
    }

    /** Default config: TLS 1.3, JVM-default cipher suites. */
    public static TlsConfig of(final SSLContext sslContext) {
        return new TlsConfig(sslContext, new String[]{TlsContexts.PROTOCOL}, null);
    }

    public static TlsConfig of(final SSLContext sslContext, final String[] protocols,
                               final String[] cipherSuites) {
        return new TlsConfig(sslContext,
                protocols == null ? null : protocols.clone(),
                cipherSuites == null ? null : cipherSuites.clone());
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    /** Enabled protocols, or {@code null} for JVM default. */
    public String[] protocols() {
        return protocols == null ? null : protocols.clone();
    }

    /** Enabled cipher suites, or {@code null} for JVM default. */
    public String[] cipherSuites() {
        return cipherSuites == null ? null : cipherSuites.clone();
    }
}
