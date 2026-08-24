/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

/**
 * Base for durable-storage failures. The concrete subtype says which artefact failed, and each
 * carries its own fault enum naming the specific operation:
 * <ul>
 *   <li>{@link WalException} -- the write-ahead log itself.</li>
 *   <li>{@link SnapshotException} -- reading or writing a snapshot.</li>
 * </ul>
 * The distinction matters to a node: a snapshot fault costs a snapshot (the next attempt can
 * retry, the WAL simply keeps growing), whereas a WAL fault means the log this node's state is
 * derived from is unreadable or unwritable.
 * <p>
 * Note that a failed {@code append} is deliberately <em>not</em> an exception -- it degrades the
 * log and returns {@code false}, so callers can decline to apply the write to memory. See
 * {@link Wal#append}.
 */
public abstract class StorageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    StorageException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
