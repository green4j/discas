/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas;

import io.github.green4j.discas.node.HashedBytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * UTF-8 conversion for test keys and values.
 * <p>
 * This two-line pair was re-declared privately in twenty-five files under eight names --
 * {@code bb}, {@code buf}, {@code utf8}, {@code key}, {@code value}, {@code encodeString},
 * {@code encode} -- with four more for the reverse direction. Three of those files declared
 * {@code key} and {@code value} as byte-identical twins of each other.
 */
public final class TestBytes {

    private TestBytes() {
    }

    /** {@code value} as a UTF-8 buffer. */
    public static ByteBuffer utf8(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * {@code value} as UTF-8 {@link HashedBytes} -- for driving node storage directly. Anything
     * going through the client API, or through a prefix/cursor argument, wants {@link #utf8}.
     */
    public static HashedBytes hashed(final String value) {
        return new HashedBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    /** A buffer's remaining bytes as a UTF-8 string, leaving its position untouched. */
    public static String string(final ByteBuffer value) {
        if (value == null) {
            return null;
        }
        final ByteBuffer view = value.duplicate();
        final byte[] raw = new byte[view.remaining()];
        view.get(raw);
        return new String(raw, StandardCharsets.UTF_8);
    }

    /** {@link HashedBytes} as a UTF-8 string. */
    public static String string(final HashedBytes value) {
        return value == null ? null : string(value.toBuffer());
    }
}
