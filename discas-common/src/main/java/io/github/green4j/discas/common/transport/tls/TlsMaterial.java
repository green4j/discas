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

    /**
     * One line for the reload report: which certificate this node now presents, until when, and
     * how many CAs it will accept.
     *
     * <p>Everything here is on the wire in the first two packets of every handshake -- a leaf's
     * subject, issuer, serial and validity are what the other side is shown in order to check it.
     * The private key and its password are the material worth protecting, and neither is named
     * here. What the line is for is the rotation question: after writing new files, did this
     * process pick up the <em>new</em> leaf, and does it still trust the old CA?
     */
    public String summary() {
        final StringBuilder sb = new StringBuilder();
        final X509Certificate leaf = leafCertificate();
        if (leaf == null) {
            sb.append("no leaf certificate");
        } else {
            sb.append("leaf ").append(leaf.getSubjectX500Principal().getName())
                    .append(" (serial ").append(leaf.getSerialNumber().toString(16))
                    .append(", issued by ").append(leaf.getIssuerX500Principal().getName())
                    .append(", expires ").append(leaf.getNotAfter().toInstant()).append(')');
        }
        sb.append(", trusting ").append(trustAnchorCount()).append(" CA(s)");
        return sb.toString();
    }

    private int trustAnchorCount() {
        try {
            return trustStore.size();
        } catch (final Exception e) {
            return -1;
        }
    }
}
