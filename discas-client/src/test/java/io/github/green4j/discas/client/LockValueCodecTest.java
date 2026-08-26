/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client;

import io.github.green4j.discas.client.lock.LockValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link LockValueCodec} -- the on-the-wire layout, pinned byte for byte.
 * <p>
 * Lock records are stored as ordinary CASPaxos values, so they outlive the process that wrote
 * them: a node reading a key written by an older build must still see the same lock. That makes
 * the layout a compatibility surface, and a round-trip test cannot protect it -- encoder and
 * decoder change together, so any consistent reshuffle round-trips perfectly while silently
 * orphaning every record already stored.
 * <p>
 * Hence the golden bytes below. They were what the codec produced before it was moved onto the
 * shared {@code MessageBufferReader}/{@code MessageBufferWriter}, and they must stay that way.
 */
@DisplayName("LockValueCodec -- wire layout")
class LockValueCodecTest {

    private static String hex(final ByteBuffer buffer) {
        final ByteBuffer view = buffer.duplicate();
        final StringBuilder sb = new StringBuilder(view.remaining() * 2);
        while (view.hasRemaining()) {
            sb.append(String.format("%02x", view.get()));
        }
        return sb.toString();
    }

    @Test
    @DisplayName("Encoding is byte-for-byte stable")
    void encodingIsStable() {
        final ByteBuffer encoded = LockValueCodec.encode(new LockValueCodec.LockRecord(
                "owner", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}), 1000L, 2000L, 1L));

        // magic "LOCK" | version 1 | len+"owner" | len+token | acquiredAt | leaseUntil | generation
        assertEquals("4c4f434b"
                        + "0001"
                        + "00000005" + "6f776e6572"
                        + "00000004" + "01020304"
                        + "00000000000003e8"
                        + "00000000000007d0"
                        + "0000000000000001",
                hex(encoded));
    }

    @Test
    @DisplayName("A record of another version is not a lock record")
    void unknownVersionDecodesToNull() {
        // Right magic, wrong version: null, exactly as for any other value that is not a lock
        // record. There is only one version, so this is corruption or an unrelated value that
        // happens to lead with "LOCK" -- never an older record to interpret leniently.
        final ByteBuffer other = ByteBuffer.allocate(39);
        other.putInt(LockValueCodec.MAGIC);
        other.putShort((short) (LockValueCodec.VERSION + 1));
        other.putInt(5).put("owner".getBytes(StandardCharsets.UTF_8));
        other.putInt(4).put(new byte[] {1, 2, 3, 4});
        other.putLong(1000L);
        other.putLong(2000L);
        other.flip();

        assertNull(LockValueCodec.decode(other));
    }

    @Test
    @DisplayName("Decoding leaves the caller's buffer untouched")
    void decodeDoesNotConsumeTheInput() {
        final ByteBuffer encoded = LockValueCodec.encode(new LockValueCodec.LockRecord(
                "o", ByteBuffer.wrap(new byte[] {9}), 1L, 2L, 3L));
        final int positionBefore = encoded.position();

        assertNotNull(LockValueCodec.decode(encoded));

        // Callers read the same value again (getLockInfo, then release's CAS on it), so a decoder
        // that advanced the position would hand the second reader an empty buffer.
        assertEquals(positionBefore, encoded.position());
    }

    @Test
    @DisplayName("A release marker keeps the tenancy it ended and zeroes only the lease")
    void releaseMarkerRemembersItsWriter() {
        final LockValueCodec.LockRecord held = new LockValueCodec.LockRecord(
                "worker-3", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}), 1000L, 2000L, 7L);

        final LockValueCodec.LockRecord marker = LockValueCodec.LockRecord.releasedMarker(held);

        assertTrue(marker.isReleased());
        assertEquals(0L, marker.leaseUntilEpochMs());
        // The three fields that make a retried release, and a later reader, able to say anything
        // at all about whose lock this key last was.
        assertEquals("worker-3", marker.ownerId());
        assertEquals(held.token(), marker.token());
        assertEquals(7L, marker.generation());

        final LockValueCodec.LockRecord roundTripped =
                LockValueCodec.decode(LockValueCodec.encode(marker));
        assertNotNull(roundTripped);
        assertTrue(roundTripped.isReleased());
        assertEquals("worker-3", roundTripped.ownerId());
        assertEquals(held.token(), roundTripped.token());
    }

    @Test
    @DisplayName("Markers of either shape read as free to a reader of either build")
    void releaseMarkersStayCompatibleAcrossBuilds() {
        // The layout version is deliberately unchanged, so a build that predates carrying the
        // owner and token on a marker still decodes one -- it just applies a stricter predicate,
        // reproduced here. What has to hold is that neither build ends up thinking the key is
        // taken, because that is what would block acquires across a rolling upgrade.
        final LockValueCodec.LockRecord blank = new LockValueCodec.LockRecord(
                "", ByteBuffer.allocate(0), 0L, 0L, 7L);
        final LockValueCodec.LockRecord carried = LockValueCodec.LockRecord.releasedMarker(
                new LockValueCodec.LockRecord(
                        "worker-3", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}), 1000L, 2000L, 7L));

        // Both shapes are released to this build.
        assertTrue(blank.isReleased());
        assertTrue(carried.isReleased());
        // The older, stricter predicate calls the carried one a lock -- and still sees a lease of
        // zero, which no clock is behind, so it reads the key as free and acquirable all the same.
        assertTrue(releasedByTheOlderPredicate(blank));
        assertFalse(releasedByTheOlderPredicate(carried));
        assertEquals(0L, carried.leaseUntilEpochMs());
    }

    private static boolean releasedByTheOlderPredicate(final LockValueCodec.LockRecord record) {
        return record.leaseUntilEpochMs() == 0L
                && record.ownerId().isEmpty()
                && record.token().remaining() == 0;
    }

    @Test
    @DisplayName("A plain value is not a lock record")
    void plainValueDecodesToNull() {
        assertNull(LockValueCodec.decode(ByteBuffer.wrap("just a value".getBytes(StandardCharsets.UTF_8))));
        assertNull(LockValueCodec.decode(ByteBuffer.allocate(0)));
        // Right magic, truncated body: still not a lock record, and must not throw.
        final ByteBuffer torn = ByteBuffer.allocate(6);
        torn.putInt(LockValueCodec.MAGIC).putShort(LockValueCodec.VERSION);
        torn.flip();
        assertNull(LockValueCodec.decode(torn));
    }
}
