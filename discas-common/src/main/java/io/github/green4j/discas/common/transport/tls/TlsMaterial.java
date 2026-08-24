/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

/**
 * An immutable snapshot of a node's TLS key material: its key store (leaf cert +
 * private key, signed by the cluster CA) and the trust store (the cluster CA, or
 * during a CA rotation, the old and new CAs). Used both to build the initial
 * {@link ReloadableTlsContext} and to hot-swap rotated material into it.
 */
public final class TlsMaterial {

    private final KeyStore keyStore;
    private final char[] keyPassword;
    private final KeyStore trustStore;

    public TlsMaterial(final KeyStore keyStore, final char[] keyPassword, final KeyStore trustStore) {
        if (keyStore == null || trustStore == null) {
            throw new IllegalArgumentException("keyStore and trustStore are required");
        }
        this.keyStore = keyStore;
        this.keyPassword = keyPassword == null ? new char[0] : keyPassword.clone();
        this.trustStore = trustStore;
    }

    public KeyStore keyStore() {
        return keyStore;
    }

    public char[] keyPassword() {
        return keyPassword.clone();
    }

    public KeyStore trustStore() {
        return trustStore;
    }

    /**
     * The leaf certificate this node presents (the first key entry's cert), used
     * for the near-expiry warning deadline. Returns {@code null} if no key entry
     * is present.
     */
    public X509Certificate leafCertificate() {
        try {
            final Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                final String alias = aliases.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    final Certificate cert = keyStore.getCertificate(alias);
                    if (cert instanceof X509Certificate) {
                        return (X509Certificate) cert;
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Failed to read leaf certificate", e);
        }
        return null;
    }
}
