/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

/**
 * Where a node's record of what its clients did goes: sessions opened and refused, every operation
 * that arrived, and how each one ended.
 * <p>
 * <b>Runs on the audit drain thread, not on the event loop</b> -- which is what the ring in front
 * of it buys: an implementation may block, write to a file or allocate while the node keeps
 * serving. The opposite contract to {@code NodeObserver}, which fires on the loop and may do none
 * of those, and for a different question: rates and states there, who did what here.
 * <p>
 * The {@link AuditEvent} passed to {@link #record} is a view into the ring and dies with the call.
 * {@link #close()} runs after the drain has handed over the last record in the buffer.
 */
public interface AuditLog extends AutoCloseable {

    /** Records nothing. */
    AuditLog NONE = event -> { };

    void record(AuditEvent event);

    @Override
    default void close() {
    }
}
