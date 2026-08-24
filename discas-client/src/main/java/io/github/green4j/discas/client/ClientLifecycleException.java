/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

/**
 * The client is closing or already closed. Never {@linkplain #isTransient() transient} in the sense
 * that matters: this instance will not serve the call, now or later. Build a new client.
 */
public final class ClientLifecycleException extends DisCasClientException {

    private static final long serialVersionUID = 1L;

    /** Where in the lifecycle the call landed. */
    public enum Phase {
        /** The client was already closed when the call was made. */
        ALREADY_CLOSED,
        /** The client began closing while this request was outstanding. */
        CLOSING,
        /** Shutdown did not complete within its budget. */
        SHUTDOWN_TIMED_OUT,
        /** The thread waiting for shutdown was interrupted. */
        INTERRUPTED
    }

    private final Phase phase;

    public ClientLifecycleException(final Phase phase, final String message) {
        super(message);
        this.phase = phase;
    }

    public ClientLifecycleException(final Phase phase, final String message,
                                    final Throwable cause) {
        super(message, cause);
        this.phase = phase;
    }

    public Phase phase() {
        return phase;
    }

    /**
     * {@link Phase#CLOSING} is reported as transient: the request was in flight during an orderly
     * shutdown, which is a normal race rather than a defect, and the same call against a live
     * client would be fine. The other phases mean this client is finished.
     */
    @Override
    public boolean isTransient() {
        return phase == Phase.CLOSING;
    }
}
