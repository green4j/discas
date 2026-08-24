/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

/**
 * Encodes and extracts the discas cluster/node identity carried in a peer
 * certificate's Subject Alternative Name.
 * <p>
 * The identity is a single URI SAN of the form
 * {@code discas://<clusterId>/<nodeId>}. Certs are issued by the cluster CA
 * with this SAN, so a peer's {@link NodeId} is cryptographically bound to its key. The peer
 * transport cross-checks the extracted values against the PEER_HELLO fields.
 */
public final class CertIdentities {

    public static final String SCHEME = "discas";

    private CertIdentities() {
    }

    /** The SAN URI value to embed in a cert for {@code (clusterId, nodeId)}. */
    public static String sanUri(final ClusterId clusterId, final NodeId nodeId) {
        return SCHEME + "://" + clusterId.value() + "/" + nodeId.value();
    }

    /**
     * Extract the {@code (clusterId, nodeId)} identity from a peer certificate's
     * SAN, or {@code null} if the cert carries no discas identity URI.
     */
    public static Parsed parse(final X509Certificate cert) {
        final Collection<List<?>> sans;
        try {
            sans = cert.getSubjectAlternativeNames();
        } catch (final CertificateParsingException e) {
            return null;
        }
        if (sans == null) {
            return null;
        }
        for (final List<?> san : sans) {
            // [Integer type, Object value]; type 6 == uniformResourceIdentifier.
            if (san.size() < 2 || !(san.get(0) instanceof Integer) || (Integer) san.get(0) != 6) {
                continue;
            }
            final Object value = san.get(1);
            if (!(value instanceof String)) {
                continue;
            }
            final String uri = (String) value;
            final String prefix = SCHEME + "://";
            if (!uri.startsWith(prefix)) {
                continue;
            }
            final String rest = uri.substring(prefix.length());
            final int slash = rest.indexOf('/');
            if (slash <= 0 || slash == rest.length() - 1) {
                continue;
            }
            final String cluster = rest.substring(0, slash);
            final String node = rest.substring(slash + 1);
            if (cluster.isEmpty() || node.isEmpty()) {
                continue;
            }
            return new Parsed(ClusterId.of(cluster), NodeId.of(node));
        }
        return null;
    }

    /** A parsed cert identity. */
    public static final class Parsed {
        public final ClusterId clusterId;
        public final NodeId nodeId;

        Parsed(final ClusterId clusterId, final NodeId nodeId) {
            this.clusterId = clusterId;
            this.nodeId = nodeId;
        }
    }
}
