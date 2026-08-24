/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.observability;

import io.github.green4j.discas.node.NodeState;

/**
 * The node state the health endpoint reports, exposed as a narrow read-only seam.
 * <p>
 * This exists so the probe can see whether the node has recovered and whether its WAL is healthy
 * without those becoming part of {@code DisCasNode}'s public surface. A node hands out one of these
 * and nothing else; the handler cannot reach the store, the transport, or the loop through it.
 * <p>
 * Implementations are read from an HTTP worker thread while the node mutates on its event loop, so
 * the underlying fields must be safely publishable.
 */
public interface HealthSource {

    /**
     * Where the node is in its start model. Both probes are derived from it: {@code /health} is
     * about the process being viable, {@code /ready} about it being a member of the quorum.
     */
    NodeState state();

    /**
     * The WAL has degraded and the node can no longer durably record consensus state. Once true it
     * stays true until a restart, so a probe seeing it can treat the node as unrecoverable in place.
     */
    boolean walDegraded();

    /** Why the WAL degraded, or {@code null} when it has not. */
    String walDegradedReason();
}
