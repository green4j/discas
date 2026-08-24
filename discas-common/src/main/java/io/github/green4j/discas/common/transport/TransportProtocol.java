/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/**
 * Shared transport protocol constants exchanged on the wire and used by
 * both the client-side and peer-side transports.
 */
public final class TransportProtocol {

    /**
     * Wire protocol version, exchanged once per connection in the {@code TYPE_PEER_HELLO} and
     * {@code TYPE_CLIENT_HELLO} payloads and nowhere else. Neither the frame header nor any
     * message payload repeats it: a connection whose hello was accepted is a connection on
     * which every subsequent message is known to speak this version, so re-checking per
     * message would cost bytes on the hot path to re-establish what the handshake settled.
     * <p>
     * A mismatch on either side closes the connection with a HELLO_RESP(PROTOCOL_MISMATCH),
     * which is what lets the codecs resolve enums and optional fields strictly -- an
     * unrecognised byte cannot have come from a legitimate counterpart of another vintage.
     */
    public static final int PROTOCOL_VERSION = 1;

    /**
     * Idle-write timeout used by both servers to evict slow consumers
     * (peers that have not drained any queued bytes within this window).
     */
    public static final int SLOW_CONSUMER_TIMEOUT_MS = 5_000;

    private TransportProtocol() {
    }
}
