/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.identity;

import io.github.green4j.discas.common.KvLimits;

/**
 * Identity of a discas cluster. Every node is configured with the cluster id
 * it belongs to; peers exchange it in the PEER_HELLO handshake and reject a
 * counterparty whose cluster id does not match the local configuration.
 * Set manually / from configuration.
 */
public final class ClusterId {

    private final String value;

    private ClusterId(final String value) {
        this.value = value;
    }

    public static ClusterId of(final String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("clusterId must be non-empty");
        }
        final int bytes = KvLimits.utf8Length(value);
        if (bytes > KvLimits.MAX_CLUSTER_ID_BYTES) {
            throw new IllegalArgumentException("clusterId too long: " + bytes
                    + " bytes exceeds maximum " + KvLimits.MAX_CLUSTER_ID_BYTES);
        }
        return new ClusterId(value);
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return value.equals(((ClusterId) o).value);
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
