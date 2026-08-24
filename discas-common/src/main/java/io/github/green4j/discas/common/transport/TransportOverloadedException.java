/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/**
 * A transport limit would have been breached. The link is healthy -- it just cannot absorb more
 * right now -- so this is always {@linkplain #isTransient() transient}.
 */
public final class TransportOverloadedException extends TransportException {

    private static final long serialVersionUID = 1L;

    /** Which limit was reached. */
    public enum Limit {
        /** Bytes already queued for transmission on this connection. */
        QUEUED_OUT_BYTES,
        /** Bytes of partially reassembled inbound messages. */
        INFLIGHT_BYTES,
        /** Per-connection receive buffer. */
        RX_BUFFER_BYTES,
        /** Bytes attributed to the transport as a whole. */
        TRANSPORT_BYTE_BUDGET,
        /** Simultaneous connections. */
        CONNECTIONS
    }

    private final Limit limit;

    public TransportOverloadedException(final Limit limit, final String message) {
        super(message);
        this.limit = limit;
    }

    public Limit limit() {
        return limit;
    }

    @Override
    public boolean isTransient() {
        return true;
    }
}
