/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

/** A write-ahead log operation failed. */
public final class WalException extends StorageException {

    private static final long serialVersionUID = 1L;

    /** Which log operation failed. */
    public enum Fault {
        /**
         * The tail could not be replayed. Fatal to startup: this node cannot reconstruct the state
         * its previous incarnation had acknowledged.
         */
        REPLAY_FAILED,
        /** Segments could not be truncated or compacted after a snapshot; the log keeps growing. */
        TRUNCATION_FAILED,
        /** The log could not be closed cleanly. */
        CLOSE_FAILED
    }

    private final Fault fault;

    public WalException(final Fault fault, final String message, final Throwable cause) {
        super(message, cause);
        this.fault = fault;
    }

    public Fault fault() {
        return fault;
    }
}
