/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport.security;

import io.github.green4j.discas.common.identity.ClientId;

import java.nio.ByteBuffer;

/**
 * Per-connection security layer that sits <b>beneath</b> the frame codec on a single
 * client connection -- the client-side analogue of {@link PeerChannelSecurity}. It
 * transforms between the network byte stream and the application (framed client message)
 * byte stream and drives any cryptographic handshake.
 * <p>
 * Both client transports route bytes through this seam: {@link #wrap} on the way out and
 * {@link #unwrap} on the way in. {@link PlaintextClientSecurity} is an identity passthrough; an
 * {@code SSLEngine}-backed implementation adds mTLS.
 */
public interface ClientChannelSecurity extends AutoCloseable {

    /** True once the security handshake (if any) has completed and frames may flow. */
    boolean handshakeFinished();

    /**
     * Consume network bytes from {@code net} and append the resulting application
     * (plaintext) bytes to {@code app}, consuming only as much of {@code net} as fits.
     */
    void unwrap(ByteBuffer net, ByteBuffer app);

    /** Transform outbound application bytes into the network bytes to enqueue for writing. */
    ByteBuffer wrap(ByteBuffer app);

    /** Network bytes the handshake needs to send right now, or {@code null} if none. */
    ByteBuffer pendingOutbound();

    /**
     * The client identity from the peer's mTLS certificate CN, established by the
     * handshake, or {@code null} when the channel authenticated no certificate (plaintext,
     * or the server side of a client connection which presents no cert).
     */
    ClientId peerClientId();

    @Override
    void close();
}
