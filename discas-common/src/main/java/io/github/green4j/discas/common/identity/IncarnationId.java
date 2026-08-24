/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.identity;

import java.util.UUID;

/**
 * Identity of one <em>run of a node's durable storage</em> -- generated when the storage directory
 * is first initialized, and gone the moment that directory is.
 * <p>
 * Not {@link NodeId}, which answers <em>which member are you</em>: that one is supplied from
 * outside, appears in the member file and the certificate SAN, and is stable across restarts and
 * across hardware. This answers <em>which run of that member's storage are you</em>, and is
 * pointedly not stable across a wipe. A node that returns with the same {@link NodeId} and empty
 * storage is amnesiac, and nothing but a marker sharing the storage's fate can tell it apart from
 * a healthy restart.
 * <p>
 * Clients have no equivalent: a client holds no durable state, so it has nothing to forget.
 * {@link ClientId} and {@link NodeId} are principals; only a node has durable state whose identity
 * has to be separated out.
 */
public final class IncarnationId {

    private final String value;

    private IncarnationId(final String value) {
        this.value = value;
    }

    /** A fresh incarnation. Called once, when a storage directory is created. */
    public static IncarnationId generate() {
        return new IncarnationId(UUID.randomUUID().toString());
    }

    /** Parse one previously written to disk or read off the wire. */
    public static IncarnationId of(final String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("incarnationId must be non-empty");
        }
        try {
            // Round-tripped rather than merely length-checked: a truncated marker file would
            // otherwise become a valid-looking identity that no other incarnation can equal.
            return new IncarnationId(UUID.fromString(value).toString());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("incarnationId is not a UUID: " + value, e);
        }
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
        return value.equals(((IncarnationId) o).value);
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
