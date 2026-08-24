/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Low-level {@link ByteBuffer} reader shared by the message codecs. Carries no protocol knowledge
 * -- just primitive {@code readX} helpers that bounds-check each call.
 *
 * <p><b>Malformed input against a short read.</b> A length prefix the buffer cannot satisfy raises
 * {@link BufferUnderflowException}, the same condition {@link ByteBuffer} signals when a read runs
 * past the limit: a streaming caller refills its window and retries on that, while a framed caller,
 * whose buffer always holds a whole message, treats it as a decode failure.
 *
 * <p>A prefix that is <em>structurally</em> wrong -- a negative length, an element count larger
 * than the remaining bytes can hold -- is never a short read, and raises
 * {@link IllegalArgumentException} whatever the caller.
 */
public final class MessageBufferReader {
    private final ByteBuffer in;

    public MessageBufferReader(final ByteBuffer in) {
        // Stated rather than inherited: slice()/duplicate() views do not carry the source's
        // order, so a reader over one would silently fall back to the default.
        this.in = in.order(ByteOrder.BIG_ENDIAN);
    }

    public byte readByte() {
        return in.get();
    }

    public short readShort() {
        return in.getShort();
    }

    public int readInt() {
        return in.getInt();
    }

    /**
     * Read an element count and bound it against what is actually left in the buffer. A
     * decoder that sizes a collection straight from {@link #readInt()} lets a peer claim
     * {@code Integer.MAX_VALUE} elements and forces the allocation before the first element
     * read can fail on the underflow.
     *
     * @param minBytesPerElement smallest encoded size a single element can occupy
     */
    public int readCount(final int minBytesPerElement) {
        final int count = in.getInt();
        if (count < 0) {
            throw new IllegalArgumentException("Negative element count: " + count);
        }
        final long minBytes = (long) count * minBytesPerElement;
        if (minBytes > in.remaining()) {
            throw new IllegalArgumentException("Element count " + count + " needs at least "
                    + minBytes + " bytes, but only " + in.remaining() + " remain");
        }
        return count;
    }

    public long readLong() {
        return in.getLong();
    }

    public boolean readBoolean() {
        return in.get() != 0;
    }

    public Ballot readBallot() {
        final long counter = readLong();
        final String node = readString();
        return new Ballot(counter, node.isEmpty() ? NodeId.NONE : NodeId.of(node));
    }

    public String readString() {
        final int len = readInt();
        if (len < 0) {
            throw new IllegalArgumentException("Negative string length: " + len);
        }
        if (len > in.remaining()) {
            // Short read -- see the class javadoc on BufferUnderflowException.
            throw new BufferUnderflowException();
        }
        final byte[] raw = new byte[len];
        in.get(raw);
        return new String(raw, StandardCharsets.UTF_8);
    }

    /**
     * A read-only view over the next length-prefixed field, taken <b>zero-copy</b> over the
     * source buffer.
     * <p>
     * The view is only valid for as long as the source is -- for a framed caller, the duration
     * of one message dispatch, after which the frame is recycled. A caller that retains the
     * bytes past that must copy them.
     */
    public ByteBuffer readBytes() {
        final int len = readInt();
        if (len < 0) {
            throw new IllegalArgumentException("Negative bytes length: " + len);
        }
        return readSlice(len);
    }

    /** As {@link #readBytes()}, for a field encoded as nullable. */
    public ByteBuffer readNullableBytes() {
        final int len = readInt();
        if (len == -1) {
            return null;
        }
        if (len < 0) {
            throw new IllegalArgumentException("Negative nullable bytes length: " + len);
        }
        return readSlice(len);
    }

    private ByteBuffer readSlice(final int len) {
        if (len > in.remaining()) {
            // Short read -- see the class javadoc on BufferUnderflowException.
            throw new BufferUnderflowException();
        }
        final ByteBuffer slice = in.slice();
        slice.limit(len);
        in.position(in.position() + len);
        return slice.asReadOnlyBuffer();
    }

    public String readNullableString() {
        final int len = readInt();
        if (len == -1) {
            return null;
        }
        if (len < 0) {
            throw new IllegalArgumentException("Negative string length: " + len);
        }
        if (len > in.remaining()) {
            // Short read -- see the class javadoc on BufferUnderflowException.
            throw new BufferUnderflowException();
        }
        final byte[] raw = new byte[len];
        in.get(raw);
        return new String(raw, StandardCharsets.UTF_8);
    }

    public boolean hasRemaining() {
        return in.hasRemaining();
    }
}
