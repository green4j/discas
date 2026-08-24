/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.security;

import java.nio.ByteBuffer;

/**
 * Per-connection security layer that sits <b>beneath</b> the frame codec on a
 * single peer connection. It transforms between the network byte stream (what
 * is read from / written to the socket) and the application byte stream (framed
 * peer messages), and drives any cryptographic handshake.
 * <p>
 * The peer transport routes bytes through this seam: {@link #wrap} on the way out (application to
 * network, at enqueue time) and {@link #unwrap} on the way in (network to application, at read
 * time). It is the single insertion point for mTLS; {@link PlaintextPeerSecurity} is an identity
 * passthrough that copies nothing.
 */
public interface PeerChannelSecurity extends AutoCloseable {

    /**
     * True once the security handshake (if any) has completed and application
     * frames may flow. The plaintext implementation is always finished.
     */
    boolean handshakeFinished();

    /**
     * Consume network bytes from {@code net} and append the resulting
     * application (plaintext) bytes to {@code app}. Should consume only as much
     * of {@code net} as fits in {@code app}, leaving the remainder in
     * {@code net} for the caller to retain until more room is available.
     */
    void unwrap(ByteBuffer net, ByteBuffer app);

    /**
     * Transform outbound application bytes {@code app} into the network bytes to
     * enqueue for writing. The plaintext implementation returns {@code app}
     * unchanged (no copy).
     */
    ByteBuffer wrap(ByteBuffer app);

    /**
     * Network bytes the handshake itself needs to send right now (e.g. TLS
     * handshake records), or {@code null} if none. The plaintext implementation
     * always returns {@code null}.
     */
    ByteBuffer pendingOutbound();

    /** Peer identity established by the handshake, or {@link PeerCredential#NONE}. */
    PeerCredential peerCredential();

    @Override
    void close();
}
