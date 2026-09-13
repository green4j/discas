/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The value is pinned because every member and client must agree on it, and the spread both uses
 * rest on -- over ranges and over the member list -- is measured rather than assumed.
 */
@DisplayName("KeyHash -- the value the cluster agrees on, and how it spreads")
class KeyHashTest {

    private static ByteBuffer utf8(final String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();
    }

    private static int hash(final String text) {
        return KeyHash.distributionHash(utf8(text));
    }

    /** Standard deviations of chi-square from its expectation, for {@code buckets} buckets. */
    private static double spread(final String[] keys, final int buckets) {
        final int[] counts = new int[buckets];
        for (final String key : keys) {
            counts[KeyHash.rangeOf(utf8(key), buckets)]++;
        }
        final double expected = keys.length / (double) buckets;
        double chi = 0;
        for (final int count : counts) {
            chi += (count - expected) * (count - expected) / expected;
        }
        return (chi - (buckets - 1)) / Math.sqrt(2.0 * (buckets - 1));
    }

    private static String[] keys(final String prefix, final int count) {
        final String[] keys = new String[count];
        for (int i = 0; i < count; i++) {
            keys[i] = prefix + i;
        }
        return keys;
    }

    @Test
    @DisplayName("The value is fixed: changing it is a change every process makes at once")
    void pinnedValues() {
        assertEquals(1373170073, hash(""));
        assertEquals(-731735362, hash("a"));
        assertEquals(-1255539065, hash("app/user/1"));
        assertEquals(-394921394, hash("lock/eod"));
        // Two whole words, and two plus a tail.
        assertEquals(-190004166, hash("0123456789abcdef"));
        assertEquals(-1657361145, hash("0123456789abcdefghij"));
    }

    @Test
    @DisplayName("Independent of the caller's byte order, so architectures cannot disagree")
    void ignoresTheBuffersOwnByteOrder() {
        final byte[] bytes = "app/user/12345xy".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer little = ByteBuffer.allocate(bytes.length).order(ByteOrder.LITTLE_ENDIAN);
        little.put(bytes).flip();

        assertEquals(KeyHash.distributionHash(ByteBuffer.wrap(bytes)),
                KeyHash.distributionHash(little));
    }

    @Test
    @DisplayName("Reads the remaining bytes only, and leaves the position where it found it")
    void respectsPositionAndLimit() {
        final ByteBuffer padded = utf8("##app/user/1##");
        padded.position(2).limit(12);

        assertEquals(hash("app/user/1"), KeyHash.distributionHash(padded));
        assertEquals(2, padded.position());
        assertEquals(12, padded.limit());
    }

    @Test
    @DisplayName("Every length from empty to past a word gets its own value")
    void tailLengthsAreDistinct() {
        final Set<Integer> seen = new HashSet<>();
        final StringBuilder key = new StringBuilder();
        for (int length = 0; length <= 17; length++) {
            seen.add(KeyHash.distributionHash(utf8(key.toString())));
            key.append('k');
        }
        assertEquals(18, seen.size());
    }

    @Test
    @DisplayName("A single bit of the key changes about half the bits of the hash")
    void avalanche() {
        for (final int keyLength : new int[] {1, 7, 8, 16, 40}) {
            final byte[] base = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) {
                base[i] = (byte) (i * 31 + 7);
            }
            final int reference = KeyHash.distributionHash(ByteBuffer.wrap(base));
            long flipped = 0;
            for (int bit = 0; bit < keyLength * 8; bit++) {
                final byte[] mutated = base.clone();
                mutated[bit / 8] ^= (byte) (1 << (bit % 8));
                flipped += Integer.bitCount(
                        KeyHash.distributionHash(ByteBuffer.wrap(mutated)) ^ reference);
            }
            final double mean = flipped / (double) (keyLength * 8);
            assertTrue(mean > 14.0 && mean < 18.0,
                    "keyLength " + keyLength + " flips " + mean + " of 32 bits");
        }
    }

    @Test
    @DisplayName("Keys spread evenly over the anti-entropy ranges, whatever shape they have")
    void spreadsOverRanges() {
        assertSpread(keys("app/user/", 200_000), 256);
        assertSpread(keys("lock/shard/", 200_000), 256);
        assertSpread(keys("k", 50_000), 256);
    }

    @Test
    @DisplayName("And over a member list, which is what picks a coordinator")
    void spreadsOverMembers() {
        for (final int members : new int[] {3, 5, 7}) {
            assertSpread(keys("app/user/", 200_000), members);
        }
    }

    @Test
    @DisplayName("Single-byte keys use the whole range, which a length-driven hash would not")
    void singleByteKeys() {
        final int[] counts = new int[256];
        for (int i = 0; i < 256; i++) {
            counts[KeyHash.rangeOf(ByteBuffer.wrap(new byte[] {(byte) i}), 256)]++;
        }
        int occupied = 0;
        for (final int count : counts) {
            if (count > 0) {
                occupied++;
            }
        }
        // 256 keys into 256 buckets fill about 1 - 1/e of them: more means an identity, fewer a
        // hash that collapses them.
        assertTrue(occupied > 140 && occupied < 200, "occupied " + occupied + " of 256");
    }

    @Test
    @DisplayName("Collisions stay at what 32 bits allow, not above")
    void collisionsAreWhatTheWidthAllows() {
        final Set<Integer> values = new HashSet<>();
        final int count = 200_000;
        for (int i = 0; i < count; i++) {
            values.add(hash("app/user/" + i));
        }
        // Birthday expectation for 200k values in 2^32 is about 4.7.
        assertTrue(count - values.size() < 25, "collisions " + (count - values.size()));
    }

    private static void assertSpread(final String[] keys, final int buckets) {
        final double deviations = spread(keys, buckets);
        assertTrue(Math.abs(deviations) < 4.0,
                buckets + " buckets: chi-square is " + deviations + " deviations out");
    }
}
