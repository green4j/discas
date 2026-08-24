/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.security;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;

/**
 * Peer identity established by a channel security handshake -- e.g. the
 * certificate identity (SAN {@code node_id} / {@code cluster_id}) extracted
 * from an mTLS {@code SSLSession}. The plaintext transport authenticates no
 * peer identity and reports {@link #NONE}.
 * <p>
 * The peer transport binds this cryptographic identity to the application-layer
 * PEER_HELLO: a mismatch between the cert SAN and the claimed/expected node or
 * cluster id is rejected (anti-impersonation).
 */
public interface PeerCredential {

    /** Sentinel for a channel with no cryptographic peer identity (plaintext). */
    PeerCredential NONE = new PeerCredential() {
        @Override
        public String toString() {
            return "PeerCredential.NONE";
        }
    };

    /** True when the handshake authenticated a peer certificate identity. */
    default boolean authenticated() {
        return false;
    }

    /** Node id from the peer certificate SAN, or {@code null} when unauthenticated. */
    default NodeId nodeId() {
        return null;
    }

    /** Cluster id from the peer certificate SAN, or {@code null} when unauthenticated. */
    default ClusterId clusterId() {
        return null;
    }
}
