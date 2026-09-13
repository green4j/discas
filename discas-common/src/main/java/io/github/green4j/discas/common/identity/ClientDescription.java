/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.identity;

import io.github.green4j.discas.common.KvLimits;

/**
 * Free-form description of a client connection, presented beside its {@link ClientId} at
 * CLIENT_HELLO and printed with it in the audit log.
 * <p>
 * A {@link ClientId} is the short stable name grants are written against, so it is kept short and
 * is rarely enough to tell a reader which controller, deployment or role is behind a connection.
 * This carries that sentence. It is optional, and nothing keys off it: authorization sees the
 * {@link ClientId} alone, so a client is free to describe itself however it likes without that
 * description buying it anything.
 * <p>
 * Bounded at {@link KvLimits#MAX_CLIENT_DESCRIPTION_BYTES} UTF-8 bytes -- a length that fits in a
 * single byte, which is how an audit record prefixes it.
 */
public final class ClientDescription {

    private final String value;

    private ClientDescription(final String value) {
        this.value = value;
    }

    public static ClientDescription of(final String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("clientDescription must be non-empty");
        }
        final int bytes = KvLimits.utf8Length(value);
        if (bytes > KvLimits.MAX_CLIENT_DESCRIPTION_BYTES) {
            throw new IllegalArgumentException("clientDescription too long: " + bytes
                    + " bytes exceeds maximum " + KvLimits.MAX_CLIENT_DESCRIPTION_BYTES);
        }
        return new ClientDescription(value);
    }

    /** Null-tolerant {@link #of(String)}, for the optional field this is on the wire and in configs. */
    public static ClientDescription ofNullable(final String value) {
        return value == null || value.isEmpty() ? null : of(value);
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
        return value.equals(((ClientDescription) o).value);
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
