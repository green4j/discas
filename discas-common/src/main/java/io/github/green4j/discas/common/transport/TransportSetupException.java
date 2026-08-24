/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

/**
 * The transport is misconfigured or was asked for something outside its membership. Never
 * {@linkplain #isTransient() transient}: retrying cannot help until the cause is corrected.
 */
public final class TransportSetupException extends TransportException {

    private static final long serialVersionUID = 1L;

    /** What is wrong. */
    public enum Fault {
        /** The transport itself could not be created (bind or selector failure). */
        INITIALIZATION_FAILED,
        /** The target is not a node this transport has connection state for. */
        UNKNOWN_TARGET
    }

    private final Fault fault;

    public TransportSetupException(final Fault fault, final String message) {
        super(message);
        this.fault = fault;
    }

    public TransportSetupException(final Fault fault, final String message,
                                   final Throwable cause) {
        super(message, cause);
        this.fault = fault;
    }

    public Fault fault() {
        return fault;
    }

    @Override
    public boolean isTransient() {
        return false;
    }
}
