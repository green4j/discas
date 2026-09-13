/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The cluster's key-to-range partitioning hash: the one implementation both sides of the protocol
 * use, since a client routes a request to a peer by it and a peer assigns a key to an anti-entropy
 * range by it, and a second copy would mis-route reads and mis-compare ranges.
 * <p>
 * Independent of {@link Object#hashCode()}, so range partitioning is not coupled to the
 * {@code HashMap} contract.
 */
public final class KeyHash {

    private KeyHash() {
    }

    // xxHash-family multipliers.
    private static final long PRIME_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME_3 = 0x165667B19E3779F9L;
    private static final long PRIME_5 = 0x27D4EB2F165667C5L;

    /** A fixed byte order, whatever the caller's buffer is in, so architectures cannot disagree. */
    private static final VarHandle WORD =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /**
     * A word-at-a-time mix over the buffer's remaining bytes, avalanched down to an int. Reads are
     * absolute, so the caller's position is not disturbed.
     * <p>
     * <b>Every member and every client must compute the same value.</b> Members that disagree file
     * a key under different anti-entropy ranges and their digests never match; clients that
     * disagree send one key to two coordinators. Changing this is a change every process makes at
     * once.
     */
    public static int distributionHash(final ByteBuffer key) {
        final int from = key.position();
        final int size = key.remaining();
        long h = PRIME_5 + size * PRIME_2;
        int i = 0;
        for (; i + Long.BYTES <= size; i += Long.BYTES) {
            long word = (long) WORD.get(key, from + i);
            word *= PRIME_2;
            word = Long.rotateLeft(word, 31) * PRIME_1;
            h ^= word;
            h = Long.rotateLeft(h, 27) * PRIME_1 + PRIME_3;
        }
        for (; i < size; i++) {
            h ^= (key.get(from + i) & 0xffL) * PRIME_5;
            h = Long.rotateLeft(h, 11) * PRIME_1;
        }
        // Into the low bits, which is what both callers take a remainder of.
        h ^= h >>> 33;
        h *= PRIME_2;
        h ^= h >>> 29;
        h *= PRIME_3;
        h ^= h >>> 32;
        return (int) h;
    }

    /** The range {@code key} falls into, of {@code numRanges}. */
    public static int rangeOf(final ByteBuffer key, final int numRanges) {
        return Integer.remainderUnsigned(distributionHash(key), numRanges);
    }
}
