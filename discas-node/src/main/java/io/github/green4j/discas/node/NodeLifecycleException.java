/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

/**
 * The node is shutting down, or its shutdown did not complete. The node counterpart of
 * {@code ClientLifecycleException}, with the same shape: a phase enum rather than a message to
 * branch on.
 */
public final class NodeLifecycleException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Where in the lifecycle the failure arose. */
    public enum Phase {
        /**
         * The node began closing while this operation was in flight. In-flight proposer rounds are
         * drained with this, so a client sees a failure rather than waiting out its timeout.
         */
        CLOSING,
        /** Pre-shutdown teardown did not finish within its budget. */
        PRE_SHUTDOWN_TIMED_OUT,
        /** The event loop did not stop within its budget. */
        SHUTDOWN_TIMED_OUT,
        /** The thread waiting for shutdown was interrupted. */
        INTERRUPTED
    }

    private final Phase phase;

    public NodeLifecycleException(final Phase phase, final String message) {
        super(message);
        this.phase = phase;
    }

    public NodeLifecycleException(final Phase phase, final String message, final Throwable cause) {
        super(message, cause);
        this.phase = phase;
    }

    public Phase phase() {
        return phase;
    }
}
