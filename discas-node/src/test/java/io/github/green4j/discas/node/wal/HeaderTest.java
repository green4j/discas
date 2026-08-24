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

import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("WalFileHeader -- a corrupt header decodes to null")
class HeaderTest {

    @Test
    void walHeaderCorrupt() {
        final ByteBuffer buffer = ByteBuffer.allocate(WalFileHeader.HEADER_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        new WalFileHeader(1).encode(buffer);

        buffer.put(5, (byte) (buffer.get(5) ^ 0xFF));
        buffer.flip();

        assertNull(WalFileHeader.decode(buffer));
    }
}
