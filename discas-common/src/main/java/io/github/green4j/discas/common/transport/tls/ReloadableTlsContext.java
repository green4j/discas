/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * A single, long-lived {@link SSLContext} backed by
 * {@link ReloadableKeyManager}/{@link ReloadableTrustManager}. Building the
 * context once and hot-swapping the underlying managers is what makes cert/key
 * rotation non-disruptive: only new handshakes see rotated material; established
 * connections ride on their existing sessions.
 */
public final class ReloadableTlsContext {

    private final SSLContext sslContext;
    private final ReloadableKeyManager keyManager;
    private final ReloadableTrustManager trustManager;

    private ReloadableTlsContext(final SSLContext sslContext,
                                 final ReloadableKeyManager keyManager,
                                 final ReloadableTrustManager trustManager) {
        this.sslContext = sslContext;
        this.keyManager = keyManager;
        this.trustManager = trustManager;
    }

    public static ReloadableTlsContext create(final TlsMaterial material) {
        try {
            final ReloadableKeyManager km = new ReloadableKeyManager(keyManagerOf(material));
            final ReloadableTrustManager tm = new ReloadableTrustManager(trustManagerOf(material));
            final SSLContext context = SSLContext.getInstance(TlsContexts.PROTOCOL);
            context.init(new KeyManager[]{km}, new TrustManager[]{tm}, null);
            return new ReloadableTlsContext(context, km, tm);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to build reloadable SSLContext", e);
        }
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    /**
     * Atomically swap in rotated key/trust material. <b>Trust-first</b>: the new
     * trust set is installed before the new key, so this node accepts peers using
     * either the old or new CA before it starts presenting a cert that may chain
     * to the new CA. Affects only handshakes started after this call.
     */
    public void reload(final TlsMaterial material) {
        try {
            trustManager.setDelegate(trustManagerOf(material));
            keyManager.setDelegate(keyManagerOf(material));
        } catch (final Exception e) {
            throw new RuntimeException("Failed to reload TLS material", e);
        }
    }

    private static X509ExtendedKeyManager keyManagerOf(final TlsMaterial material) throws Exception {
        final KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(material.keyStore(), material.keyPassword());
        for (final KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509ExtendedKeyManager) {
                return (X509ExtendedKeyManager) km;
            }
        }
        throw new IllegalStateException("No X509ExtendedKeyManager from default factory");
    }

    private static X509ExtendedTrustManager trustManagerOf(final TlsMaterial material) throws Exception {
        final TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(material.trustStore());
        for (final TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509ExtendedTrustManager) {
                return (X509ExtendedTrustManager) tm;
            }
        }
        throw new IllegalStateException("No X509ExtendedTrustManager from default factory");
    }
}
