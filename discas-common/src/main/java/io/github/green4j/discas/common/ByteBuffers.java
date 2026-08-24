/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import java.nio.ByteBuffer;

/**
 * Null-tolerant {@link ByteBuffer} view and copy helpers.
 * <p>
 * The protocol carries keys and values as plain {@code ByteBuffer}s, which are mutable handles
 * over shared storage. Whether a given hand-off needs a copy or only a view is a decision worth
 * making explicitly at each call site, so the two are separate methods with names that say
 * which one happened.
 */
public final class ByteBuffers {

    /**
     * A shared empty buffer. Safe to share because it is read-only and every reader here works
     * through a duplicate, so nothing can consume it.
     */
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0).asReadOnlyBuffer();

    /** Bytes of content {@link #preview(ByteBuffer)} renders before truncating. */
    public static final int PREVIEW_BYTES = 16;

    private ByteBuffers() {
    }

    /**
     * A read-only view over {@code source}'s remaining bytes. No copy: the result still reflects
     * later writes to {@code source}'s content, and is only appropriate where the producer
     * guarantees it will not mutate them.
     */
    public static ByteBuffer readOnly(final ByteBuffer source) {
        return source == null ? null : source.duplicate().asReadOnlyBuffer();
    }

    /**
     * A private read-only copy of {@code source}'s remaining bytes, safe to retain however long
     * the caller likes. {@code source}'s position is not disturbed.
     */
    public static ByteBuffer copyReadOnly(final ByteBuffer source) {
        if (source == null) {
            return null;
        }
        final ByteBuffer view = source.duplicate();
        final ByteBuffer copy = ByteBuffer.allocate(view.remaining());
        copy.put(view);
        copy.flip();
        return copy.asReadOnlyBuffer();
    }

    /**
     * An independent position/limit over the same bytes, so a caller reading the result cannot
     * disturb whoever else holds it.
     */
    public static ByteBuffer duplicate(final ByteBuffer source) {
        return source == null ? null : source.duplicate();
    }

    /**
     * Renders {@code source} for a log line or an error message as {@code size:hex}, truncated
     * to {@link #PREVIEW_BYTES}.
     * <p>
     * Bounded because these render keys and values that reach observers and {@code toString()},
     * and a value may be up to {@code KvLimits.MAX_VALUE_BYTES}. {@code ByteBuffer}'s own
     * {@code toString()} shows only positions, which is never what the reader wanted.
     */
    public static String preview(final ByteBuffer source) {
        if (source == null) {
            return "null";
        }
        final ByteBuffer view = source.duplicate();
        final int size = view.remaining();
        if (size > PREVIEW_BYTES) {
            view.limit(view.position() + PREVIEW_BYTES);
            return size + ":" + Hex.encode(view) + "...";
        }
        return size + ":" + Hex.encode(view);
    }
}
