/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/**
 * The request did not get an answer, but the client is still usable. Always
 * {@linkplain #isTransient() transient} -- every case here is about this attempt, not about the
 * request being wrong (that is {@link DisCasOperationException} with a caller-error code).
 */
public final class RequestFailedException extends DisCasClientException {

    private static final long serialVersionUID = 1L;

    /** How the attempt ran out. */
    public enum Cause {
        /** Every peer was tried and none answered within the per-attempt timeout. */
        TIMED_OUT,
        /** Every peer was tried and the send itself failed on each. */
        ALL_PEERS_EXHAUSTED,
        /** Fewer than a majority of nodes answered a scan, so the merge could not be trusted. */
        SCAN_NO_QUORUM
    }

    private final Cause cause;

    public RequestFailedException(final Cause cause, final String message) {
        super(message);
        this.cause = cause;
    }

    public RequestFailedException(final Cause cause, final String message,
                                  final Throwable throwable) {
        super(message, throwable);
        this.cause = cause;
    }

    public Cause cause() {
        return cause;
    }

    @Override
    public boolean isTransient() {
        return true;
    }
}
