/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.KeyHash;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 * Binary data carried together with its cached content hash.
 * <p>
 * <b>Use this only where the cached hash -- or the defensive copy that comes with it -- is
 * actually required.</b> That is the peer's storage and consensus state: {@code LocalStore}'s
 * keyspace, the values it digests, the WAL records it replays. Everything else -- a scan
 * prefix, a paging cursor, anything merely compared or streamed -- is a {@link ByteBuffer},
 * passed zero-copy. The type lives in {@code discas-node} rather than {@code discas-common} so
 * that rule is enforced by the module graph: {@code discas-client} does not depend on this
 * module and cannot name this type.
 * <p>
 * The point of the class is {@link #hashCode()}: {@code LocalStore.states} is probed on every
 * promise and accept, and a cached hash makes that O(1) instead of rehashing the key's content
 * each time. The hash is computed lazily, so an instance used only as a comparison bound never
 * pays for one.
 * <p>
 * Immutable. The backing storage is never exposed and never mutated, and every read goes
 * through absolute {@code get(i)}, so instances are safe to share and to read concurrently.
 */
public final class HashedBytes implements Comparable<HashedBytes> {
    public static final HashedBytes EMPTY = new HashedBytes(ByteBuffer.allocate(0));

    private final ByteBuffer data;
    private final int size;

    /**
     * Cached {@link #hashCode()}, computed on first use. {@code 0} means "not computed yet";
     * content that genuinely hashes to zero is simply rehashed each time, which is correct.
     * Benign race, same as {@code String.hash}.
     */
    private int hash;

    /**
     * Copies {@code source}'s remaining bytes. The caller keeps ownership of its buffer and may
     * mutate or recycle it afterwards; its position is not disturbed.
     */
    public HashedBytes(final ByteBuffer source) {
        final ByteBuffer view = source.duplicate();
        size = view.remaining();
        final ByteBuffer copy = ByteBuffer.allocate(size);
        if (size > 0) {
            copy.put(view);
            copy.flip();
        }
        data = copy.asReadOnlyBuffer();
    }

    /**
     * Convenience constructor for byte[]. Equivalent to
     * {@code new HashedBytes(ByteBuffer.wrap(data))}; the array is copied into the
     * internal ByteBuffer and not retained.
     */
    public HashedBytes(final byte[] data) {
        this(ByteBuffer.wrap(data));
    }

    private HashedBytes(final ByteBuffer readOnlyView, final int size) {
        this.data = readOnlyView;
        this.size = size;
    }

    /**
     * Wraps {@code source} without copying, for use as a transient comparison bound -- seeking a
     * {@code NavigableSet} of keys with a cursor that arrived as a {@link ByteBuffer}, say.
     * <p>
     * <b>The result aliases the caller's buffer.</b> Never retain it and never put it in a
     * collection; use {@link #adopt(ByteBuffer)} when the bytes are stable, or the copying
     * constructor when they are not.
     */
    public static HashedBytes viewOf(final ByteBuffer source) {
        // slice(), not duplicate(): every read here is an absolute get(i), which is only
        // correct when the view's position is zero.
        return new HashedBytes(source.slice().asReadOnlyBuffer(), source.remaining());
    }

    /**
     * Wraps {@code source} without copying and is safe to retain.
     * <p>
     * <b>The caller must own {@code source} exclusively and never mutate it</b> -- typically it
     * is already a private read-only copy, such as a decoded message field. Where that does not
     * hold, use the copying constructor.
     * <p>
     * Null-tolerant, because the fields it adopts (a CAS {@code expected}, a value) are
     * themselves nullable on the wire.
     */
    public static HashedBytes adopt(final ByteBuffer source) {
        return source == null
                ? null
                : new HashedBytes(source.slice().asReadOnlyBuffer(), source.remaining());
    }

    /**
     * Returns a fresh read-only view over the contained bytes with independent
     * position/limit. The backing storage is never exposed and never mutated.
     */
    public ByteBuffer toBuffer() {
        return data.duplicate();
    }

    /** Null-tolerant {@link #toBuffer()}, for the nullable fields the codecs carry. */
    public static ByteBuffer toBuffer(final HashedBytes bytes) {
        return bytes == null ? null : bytes.toBuffer();
    }

    /** Null-tolerant copying constructor, for the nullable fields the codecs carry. */
    public static HashedBytes copyOf(final ByteBuffer source) {
        return source == null ? null : new HashedBytes(source);
    }

    public int size() {
        return size;
    }

    /**
     * Whether these bytes begin with {@code prefix}. An empty prefix is a prefix of everything.
     * <p>
     * Reads through absolute {@code get(i)} so neither buffer's position is disturbed, which is
     * what lets a scan test every key in a page against one caller-owned prefix buffer without
     * allocating.
     */
    public boolean startsWith(final ByteBuffer prefix) {
        final int prefixFrom = prefix.position();
        final int prefixSize = prefix.remaining();
        if (prefixSize > size) {
            return false;
        }
        for (int i = 0; i < prefixSize; i++) {
            if (data.get(i) != prefix.get(prefixFrom + i)) {
                return false;
            }
        }
        return true;
    }

    public HashedBytes sha256() {
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(data.duplicate());
            return new HashedBytes(ByteBuffer.wrap(md.digest()));
        } catch (final Exception e) {
            throw new AssertionError(e);
        }
    }

    /** The range these bytes fall into, of {@code numRanges}. See {@link KeyHash}. */
    public int rangeOf(final int numRanges) {
        return KeyHash.rangeOf(data, numRanges);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return data.equals(((HashedBytes) o).data);
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = data.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public int compareTo(final HashedBytes o) {
        return data.compareTo(o.data);
    }

    /**
     * Byte-order comparison against a raw buffer, so a stored key can be compared to a cursor
     * that arrived as a {@link ByteBuffer} without allocating a view per comparison.
     */
    public int compareTo(final ByteBuffer o) {
        return data.compareTo(o);
    }

    /**
     * A bounded hex preview plus the size. These reach observers and error messages, and a value
     * can be up to {@code KvLimits.MAX_VALUE_BYTES}, so the content is truncated rather than
     * rendered whole.
     */
    @Override
    public String toString() {
        return "HashedBytes[" + ByteBuffers.preview(data) + "]";
    }
}
