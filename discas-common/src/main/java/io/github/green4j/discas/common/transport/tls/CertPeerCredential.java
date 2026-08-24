/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.tls;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.security.PeerCredential;

import java.security.cert.X509Certificate;

/**
 * {@link PeerCredential} backed by a verified mTLS peer certificate. Carries the
 * cluster/node identity parsed from the cert SAN (see {@link CertIdentities}).
 */
public final class CertPeerCredential implements PeerCredential {

    private final NodeId nodeId;
    private final ClusterId clusterId;

    private CertPeerCredential(final NodeId nodeId, final ClusterId clusterId) {
        this.nodeId = nodeId;
        this.clusterId = clusterId;
    }

    /**
     * Build a credential from a verified peer cert, or {@link PeerCredential#NONE}
     * if the cert carries no discas identity SAN.
     */
    public static PeerCredential fromCert(final X509Certificate cert) {
        final CertIdentities.Parsed parsed = CertIdentities.parse(cert);
        if (parsed == null) {
            return PeerCredential.NONE;
        }
        return new CertPeerCredential(parsed.nodeId, parsed.clusterId);
    }

    @Override
    public boolean authenticated() {
        return true;
    }

    @Override
    public NodeId nodeId() {
        return nodeId;
    }

    @Override
    public ClusterId clusterId() {
        return clusterId;
    }

    @Override
    public String toString() {
        return "CertPeerCredential(" + clusterId + "," + nodeId + ")";
    }
}
