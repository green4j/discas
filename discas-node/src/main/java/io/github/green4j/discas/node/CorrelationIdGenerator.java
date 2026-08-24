/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.NodeId;

/**
 * Single source of correlation IDs shared between Proposer
 * and AntiEntropy.
 * All callers are on the same EventLoop; no synchronization needed.
 * <p>
 * The initial sequence incorporates nodeId and nanoTime to avoid
 * collisions with IDs issued by a previous incarnation of this node
 * (e.g., after a restart). Peers may still hold in-flight responses
 * keyed on old correlation IDs; a fresh epoch prevents stale matches
 */
final class CorrelationIdGenerator {
    private long sequence;

    CorrelationIdGenerator(final NodeId nodeId) {
        this.sequence = ((long) (nodeId.hashCode() & 0xFFFF) << 48)
                | (System.nanoTime() & 0x0000FFFFFFFFFFFFL);
    }

    /**
     * Generate the next unique correlation ID.
     * Must be called from the event loop thread only
     */
    public long next() {
        return ++sequence;
    }
}
