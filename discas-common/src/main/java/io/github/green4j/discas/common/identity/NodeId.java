/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.identity;

import io.github.green4j.discas.common.KvLimits;

/**
 * Stable, human-readable identity of a cluster member (a {@code DisCasNode}).
 * <p>
 * Node ids are set manually / from configuration and are the CASPaxos
 * tie-breaker embedded in {@link io.github.green4j.discas.common.Ballot}: their
 * natural (lexicographic) ordering breaks ballots with equal counters, so the
 * ordering only needs to be a stable total order, which {@link String}
 * comparison provides.
 */
public final class NodeId implements Comparable<NodeId> {

    /**
     * Sentinel used only as the node component of {@link
     * io.github.green4j.discas.common.Ballot#ZERO}. It is the empty string, so
     * it sorts before every real node id and can never collide with one
     * (real ids are validated non-empty).
     */
    public static final NodeId NONE = new NodeId("");

    private final String value;

    private NodeId(final String value) {
        this.value = value;
    }

    public static NodeId of(final String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("nodeId must be non-empty");
        }
        final int bytes = KvLimits.utf8Length(value);
        if (bytes > KvLimits.MAX_NODE_ID_BYTES) {
            throw new IllegalArgumentException("nodeId too long: " + bytes
                    + " bytes exceeds maximum " + KvLimits.MAX_NODE_ID_BYTES);
        }
        return new NodeId(value);
    }

    public String value() {
        return value;
    }

    @Override
    public int compareTo(final NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return value.equals(((NodeId) o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
