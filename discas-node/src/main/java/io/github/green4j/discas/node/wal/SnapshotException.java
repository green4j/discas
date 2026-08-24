/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

/**
 * A snapshot operation failed. Recoverable in the sense that the node keeps running: the snapshot
 * is abandoned and the WAL continues to carry the state until a later attempt succeeds.
 */
public final class SnapshotException extends StorageException {

    private static final long serialVersionUID = 1L;

    /** Which snapshot operation failed. */
    public enum Fault {
        /** An existing snapshot could not be opened for recovery. */
        OPEN_FAILED,
        /** A new snapshot could not be started. */
        BEGIN_FAILED,
        /** An entry could not be written into the snapshot in progress. */
        WRITE_FAILED,
        /** The snapshot could not be finalised, so it must not be treated as usable. */
        COMMIT_FAILED,
        /** An entry could not be read back from a snapshot. */
        READ_FAILED
    }

    private final Fault fault;

    public SnapshotException(final Fault fault, final String message, final Throwable cause) {
        super(message, cause);
        this.fault = fault;
    }

    public Fault fault() {
        return fault;
    }
}
