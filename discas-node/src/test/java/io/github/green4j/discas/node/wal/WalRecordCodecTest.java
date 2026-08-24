/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("WAL record codec -- framing + CRC")
class WalRecordCodecTest {

    @Test
    void encodeDecodeRoundtrip() {
        final byte[] payload = new byte[]{1, 2, 3, 4, 5};
        final int diskSize = WalRecordCodec.diskSize(payload.length);
        final ByteBuffer buffer = ByteBuffer.allocate(diskSize);
        buffer.order(ByteOrder.BIG_ENDIAN);

        WalRecordCodec.encode(buffer, 42L, 7, ByteBuffer.wrap(payload));
        buffer.flip();

        final WalRecordCodec.OffsetRecord record = WalRecordCodec.decodeToOffsets(buffer);
        assertNotNull(record);
        assertEquals(42L, record.lsn());
        assertEquals(7, record.recordType());
        assertEquals(5, record.payloadLength());
        assertEquals(diskSize, record.totalDiskBytes());
    }

    @Test
    void corruptCrcReturnsNull() {
        final byte[] payload = new byte[]{10, 20, 30};
        final int diskSize = WalRecordCodec.diskSize(payload.length);
        final ByteBuffer buffer = ByteBuffer.allocate(diskSize);
        buffer.order(ByteOrder.BIG_ENDIAN);

        WalRecordCodec.encode(buffer, 5L, 1, ByteBuffer.wrap(payload));
        buffer.put(16, (byte) (buffer.get(16) ^ 0xFF));
        buffer.flip();

        assertNull(WalRecordCodec.decodeToOffsets(buffer));
    }
}
