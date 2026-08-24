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
import java.security.KeyStore;

/**
 * Builds the single long-lived {@link SSLContext} that backs the mTLS peer
 * transport. The context is created once over the given key/trust material;
 * per-connection {@code SSLEngine}s are minted from it by the provider. For material that
 * rotates, {@link ReloadableTlsContext} swaps the managers underneath instead of rebuilding it.
 */
public final class TlsContexts {

    /** TLS protocol used for the peer mesh. */
    public static final String PROTOCOL = "TLSv1.3";

    private TlsContexts() {
    }

    /**
     * Build an {@link SSLContext} from a key store (this node's cert + key,
     * signed by the cluster CA) and a trust store (the cluster CA).
     */
    public static SSLContext build(final KeyStore keyStore, final char[] keyPassword,
                                   final KeyStore trustStore) {
        try {
            final KeyManagerFactory kmf =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keyPassword);
            final KeyManager[] keyManagers = kmf.getKeyManagers();

            final TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            final TrustManager[] trustManagers = tmf.getTrustManagers();

            final SSLContext context = SSLContext.getInstance(PROTOCOL);
            context.init(keyManagers, trustManagers, null);
            return context;
        } catch (final Exception e) {
            throw new RuntimeException("Failed to build mTLS SSLContext", e);
        }
    }

    /**
     * Build an {@link SSLContext} with a trust store but <b>no</b> key material -- for a
     * party that verifies the peer's certificate but presents none of its own (e.g. a
     * client connecting to a server-authenticated-TLS endpoint and authenticating with a
     * token instead of a client certificate).
     */
    public static SSLContext buildTrustOnly(final KeyStore trustStore) {
        try {
            final TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            final SSLContext context = SSLContext.getInstance(PROTOCOL);
            context.init(null, tmf.getTrustManagers(), null);
            return context;
        } catch (final Exception e) {
            throw new RuntimeException("Failed to build trust-only SSLContext", e);
        }
    }
}
