/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.identity;

import io.github.green4j.discas.common.KvLimits;

/**
 * Stable, human-readable identity of a client connection.
 * <p>
 * Client ids are set explicitly by the caller when a {@code DisCasClient}
 * connection is established -- just like {@link NodeId} and {@link ClusterId},
 * which are set manually / from configuration. The id is carried as the sender
 * of client-to-node protocol messages. Its natural ({@link String}) ordering is
 * a stable total order.
 */
public final class ClientId implements Comparable<ClientId> {

    private final String value;

    private ClientId(final String value) {
        this.value = value;
    }

    public static ClientId of(final String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("clientId must be non-empty");
        }
        final int bytes = KvLimits.utf8Length(value);
        if (bytes > KvLimits.MAX_CLIENT_ID_BYTES) {
            throw new IllegalArgumentException("clientId too long: " + bytes
                    + " bytes exceeds maximum " + KvLimits.MAX_CLIENT_ID_BYTES);
        }
        return new ClientId(value);
    }

    public String value() {
        return value;
    }

    @Override
    public int compareTo(final ClientId other) {
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
        return value.equals(((ClientId) o).value);
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
