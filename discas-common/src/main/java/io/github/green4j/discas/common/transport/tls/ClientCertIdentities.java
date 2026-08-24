/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.identity.ClientId;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.x500.X500Principal;
import java.security.cert.X509Certificate;

/**
 * Encodes and extracts the {@link ClientId} carried in a client certificate's Subject
 * Common Name ({@code CN=<clientId>}).
 * <p>
 * The client-side analogue of {@link CertIdentities}, which binds a peer's node identity in a URI
 * SAN instead. A client identity lives in the subject CN, the common practice for user and service
 * client certs. The client server cross-checks the extracted id against the CLIENT_HELLO's claim.
 */
public final class ClientCertIdentities {

    private ClientCertIdentities() {
    }

    /** The certificate subject DN to issue for {@code clientId}. */
    public static String subjectDn(final ClientId clientId) {
        return "CN=" + clientId.value();
    }

    /**
     * Extract the {@link ClientId} from a verified client certificate's subject CN, or
     * {@code null} if the cert carries no usable CN.
     */
    public static ClientId parse(final X509Certificate cert) {
        final String cn = commonName(cert.getSubjectX500Principal());
        if (cn == null || cn.isEmpty()) {
            return null;
        }
        try {
            return ClientId.of(cn);
        } catch (final IllegalArgumentException e) {
            return null; // CN is not a valid client id
        }
    }

    private static String commonName(final X500Principal principal) {
        try {
            final LdapName dn = new LdapName(principal.getName());
            for (final Rdn rdn : dn.getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    return rdn.getValue().toString();
                }
            }
        } catch (final InvalidNameException e) {
            return null;
        }
        return null;
    }
}
