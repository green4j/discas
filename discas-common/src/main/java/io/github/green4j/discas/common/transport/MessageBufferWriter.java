/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import io.github.green4j.discas.common.Ballot;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Low-level growable {@link ByteBuffer} writer shared by the message and record codecs. Carries no
 * protocol knowledge -- just primitive {@code putX} helpers and grow-on-demand.
 * <p>
 * Every writer is capped at construction by the maximum encoded size of the framing it serves.
 * Without the cap the doubling loop runs past {@code 2^30}, wraps negative, and never terminates.
 */
public final class MessageBufferWriter {
    private final int maxCapacity;
    private ByteBuffer buf;

    public MessageBufferWriter(final int initialCapacity, final int maxCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive: " + initialCapacity);
        }
        if (maxCapacity < initialCapacity) {
            throw new IllegalArgumentException("maxCapacity " + maxCapacity
                    + " is below initialCapacity " + initialCapacity);
        }
        this.maxCapacity = maxCapacity;
        this.buf = ByteBuffer.allocate(initialCapacity).order(ByteOrder.BIG_ENDIAN);
    }

    public void writeByte(final byte b) {
        ensure(1);
        buf.put(b);
    }

    public void writeBoolean(final boolean b) {
        writeByte((byte) (b ? 1 : 0));
    }

    public void writeShort(final short v) {
        ensure(Short.BYTES);
        buf.putShort(v);
    }

    public void writeInt(final int v) {
        ensure(Integer.BYTES);
        buf.putInt(v);
    }

    public void writeLong(final long v) {
        ensure(Long.BYTES);
        buf.putLong(v);
    }

    public void writeBallot(final Ballot b) {
        writeLong(b.counter());
        writeString(b.nodeId().value());
    }

    public void writeString(final String value) {
        final ByteBuffer utf = StandardCharsets.UTF_8.encode(value);
        final int len = utf.remaining();
        ensure(Integer.BYTES + len);
        buf.putInt(len);
        buf.put(utf);
    }

    /**
     * Writes {@code bytes}' remaining content, length-prefixed.
     * <p>
     * Reads through a duplicate so the caller's position is left untouched: callers pass shared
     * immutable buffers (an empty-prefix constant, a stored key's view) that would be consumed
     * by the first write otherwise.
     */
    public void writeBytes(final ByteBuffer bytes) {
        final ByteBuffer view = bytes.duplicate();
        final int len = view.remaining();
        ensure(Integer.BYTES + len);
        buf.putInt(len);
        buf.put(view);
    }

    public void writeNullableBytes(final ByteBuffer bytes) {
        if (bytes == null) {
            writeInt(-1);
        } else {
            writeBytes(bytes);
        }
    }

    public void writeNullableString(final String value) {
        if (value == null) {
            writeInt(-1);
            return;
        }
        final ByteBuffer utf = StandardCharsets.UTF_8.encode(value);
        final int len = utf.remaining();
        ensure(Integer.BYTES + len);
        buf.putInt(len);
        buf.put(utf);
    }

    public ByteBuffer toByteBuffer() {
        buf.flip();
        return buf;
    }

    private void ensure(final int required) {
        if (buf.remaining() >= required) {
            return;
        }
        final long needed = (long) buf.position() + required;
        if (needed > maxCapacity) {
            throw new IllegalArgumentException("Encoded message of " + needed
                    + " bytes exceeds the maximum " + maxCapacity + " for this framing");
        }
        int newCapacity = buf.capacity();
        while (newCapacity < needed) {
            if (newCapacity > maxCapacity / 2) { // doubling would overflow the cap (and int)
                newCapacity = maxCapacity;
                break;
            }
            newCapacity *= 2;
        }
        final ByteBuffer grown = ByteBuffer.allocate(newCapacity).order(buf.order());
        buf.flip();
        grown.put(buf);
        buf = grown;
    }
}
