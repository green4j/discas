/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/** The link cannot carry a send at the moment. Always {@linkplain #isTransient() transient}. */
public final class TransportUnavailableException extends TransportException {

    private static final long serialVersionUID = 1L;

    /** Why the link is unavailable. */
    public enum Reason {
        /** The transport has been shut down. */
        CLOSED,
        /** No connection to the target could be established. */
        CONNECT_FAILED,
        /** The target is in reconnect backoff, so this send was refused rather than queued. */
        RECONNECT_BACKOFF
    }

    private final Reason reason;

    public TransportUnavailableException(final Reason reason, final String message) {
        super(message);
        this.reason = reason;
    }

    public TransportUnavailableException(final Reason reason, final String message,
                                         final Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    @Override
    public boolean isTransient() {
        return true;
    }
}
