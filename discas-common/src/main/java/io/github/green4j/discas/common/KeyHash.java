/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import java.nio.ByteBuffer;

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

    /**
     * FNV-1a (32-bit) over the buffer's remaining bytes, with a murmur3-style avalanche
     * finalizer.
     * <p>
     * Reads through absolute {@code get(i)} so the caller's position is not disturbed.
     */
    public static int distributionHash(final ByteBuffer key) {
        final int from = key.position();
        final int size = key.remaining();
        int h = 0x811c9dc5; // FNV-1a offset basis
        for (int i = 0; i < size; i++) {
            h ^= (key.get(from + i) & 0xff);
            h *= 0x01000193; // FNV-1a prime
        }
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    /** The range {@code key} falls into, of {@code numRanges}. */
    public static int rangeOf(final ByteBuffer key, final int numRanges) {
        return Integer.remainderUnsigned(distributionHash(key), numRanges);
    }
}
