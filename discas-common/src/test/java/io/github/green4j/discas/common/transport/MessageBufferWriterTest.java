/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("MessageBufferWriter -- grow-on-demand is bounded by the framing's maximum")
class MessageBufferWriterTest {

    @Test
    @DisplayName("Grows to hold a payload larger than the initial capacity")
    void growsWithinTheCap() {
        final MessageBufferWriter out = new MessageBufferWriter(8, 4096);
        final byte[] payload = new byte[1000];
        out.writeBytes(ByteBuffer.wrap(payload));

        final ByteBuffer encoded = out.toByteBuffer();
        assertEquals(Integer.BYTES + payload.length, encoded.remaining());
        assertEquals(payload.length, encoded.getInt());
    }

    @Test
    @DisplayName("A write past the cap is rejected rather than doubling forever")
    void rejectsAWritePastTheCap() {
        // Before the cap existed, the doubling loop ran past 2^30, wrapped negative, and never
        // terminated -- a hostile length prefix hung the encoding thread instead of failing it.
        final MessageBufferWriter out = new MessageBufferWriter(8, 64);

        assertThrows(IllegalArgumentException.class,
                () -> out.writeBytes(ByteBuffer.wrap(new byte[128])));
    }

    @Test
    @DisplayName("A cap below the initial capacity is a wiring error")
    void rejectsACapBelowTheInitialCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new MessageBufferWriter(64, 8));
        assertThrows(IllegalArgumentException.class, () -> new MessageBufferWriter(0, 8));
    }

    @Test
    @DisplayName("writeBytes does not consume its argument, so a shared buffer can be written twice")
    void writeBytesLeavesTheArgumentAlone() {
        // Callers pass shared immutable buffers -- an empty-prefix constant, a view of a stored
        // key -- which the first write would consume if it read the argument directly.
        final ByteBuffer payload = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final MessageBufferWriter out = new MessageBufferWriter(8, 4096);

        out.writeBytes(payload);
        out.writeBytes(payload);

        assertEquals(0, payload.position());
        final ByteBuffer encoded = out.toByteBuffer();
        assertEquals(3, encoded.getInt());
        final byte[] first = new byte[3];
        encoded.get(first);
        assertEquals(3, encoded.getInt());
        final byte[] second = new byte[3];
        encoded.get(second);
        assertArrayEquals(first, second);
    }

    @Test
    @DisplayName("Byte order stays big-endian across a grow")
    void staysBigEndianAcrossAGrow() {
        final MessageBufferWriter out = new MessageBufferWriter(4, 4096);
        out.writeInt(1);
        out.writeLong(0x0102030405060708L); // forces the grow

        final ByteBuffer encoded = out.toByteBuffer();
        assertEquals(ByteOrder.BIG_ENDIAN, encoded.order());
        assertEquals(1, encoded.getInt());
        assertEquals(0x0102030405060708L, encoded.getLong());
    }
}
