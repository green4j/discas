/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;


import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("WalWriter -- write buffer holds a max-size record independent of walMaxFileBytes")
class WalWriterMaxRecordTest {

    @Test
    void appendsMaxSizeRecordUnderDefaultConfig(@TempDir final Path dir) throws Exception {
        // Default walMaxFileBytes = 64 MiB, whose /4 write-buffer (16 MiB) is smaller than a
        // worst-case key+ballot+value record (~17 MiB). The buffer must grow to fit it.
        final StorageConfig config = StorageConfig.builder()
                .baseDirectory(dir).build();
        Files.createDirectories(config.walDirectory());
        final WalWriter writer = new WalWriter(config, 0);

        final int maxPayload = WalRecordCodec.MAX_RECORD_DISK_BYTES - WalRecordCodec.FRAME_OVERHEAD;
        assertDoesNotThrow(() -> writer.append(1, 2, ByteBuffer.allocate(maxPayload)),
                "A max-size record must append even though the default write buffer is smaller");

        // One byte past the derived ceiling is rejected (node ingress should prevent this upstream).
        assertThrows(IOException.class,
                () -> writer.append(2, 2, ByteBuffer.allocate(maxPayload + 1)));

        writer.roll();
    }
}
