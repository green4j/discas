/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import io.github.green4j.discas.common.io.ReloadReport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("FileAuditConfig -- settings from a file, re-read on request")
class FileAuditConfigTest {

    @TempDir
    private Path dir;

    private Path write(final String contents) throws IOException {
        final Path file = dir.resolve("audit.properties");
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static ByteBuffer key(final String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void defaultsWhenTheFileIsEmpty() throws Exception {
        final AuditConfigSnapshot cfg = new FileAuditConfig(write("")).snapshot();

        assertEquals(AuditConfigSnapshot.SINK_LOG, cfg.sink());
        assertEquals(AuditOverflow.DROP, cfg.overflow());
        assertEquals(AuditHashAlgorithm.NONE, cfg.hashAlgorithm());
        assertEquals(0, cfg.hashSaltIdCode());
    }

    @Test
    void parsesPrefixesAndSalt() throws Exception {
        final AuditConfigSnapshot cfg = new FileAuditConfig(write(
                "audit.overflow = wait\n"
                        + "audit.hash.algorithm = sha-256\n"
                        + "audit.hash.salt = pepper\n"
                        + "audit.full.prefixes = lock/ ; meta/\n"
                        + "audit.text.prefixes = config/\n")).snapshot();

        assertEquals(AuditOverflow.WAIT, cfg.overflow());
        assertEquals(AuditHashAlgorithm.SHA_256, cfg.hashAlgorithm());
        assertNotEquals(0, cfg.hashSaltIdCode());
        assertTrue(cfg.rendersFull(key("lock/eod")));
        assertTrue(cfg.rendersFull(key("meta/x")));
        assertFalse(cfg.rendersFull(key("data/x")));
        assertTrue(cfg.rendersText(key("config/flag")));
        assertFalse(cfg.rendersText(key("lock/eod")));
    }

    @Test
    @DisplayName("The buffer size is rounded up to a power of two")
    void bufferSizeIsRoundedUp() throws Exception {
        assertEquals(8192, FileAuditConfig.bufferBytesOf(write("audit.buffer-bytes = 5000\n")));
        assertEquals(4096, FileAuditConfig.bufferBytesOf(write("audit.buffer-bytes = 4096\n")));
    }

    @Test
    void refusesAnUnknownSink() throws Exception {
        final Path file = write("audit.sink = syslog\n");
        assertThrows(RuntimeException.class, () -> new FileAuditConfig(file));
    }

    @Test
    @DisplayName("A reload may change what is recorded, not the ring the node was sized around")
    void reloadRefusesAChangedBuffer() throws Exception {
        final Path file = write("audit.buffer-bytes = 4096\naudit.hash.algorithm = none\n");
        final FileAuditConfig config = new FileAuditConfig(file);

        Files.write(file, "audit.buffer-bytes = 4096\naudit.hash.algorithm = sha-256\n"
                .getBytes(StandardCharsets.UTF_8));
        assertEquals(ReloadReport.Outcome.APPLIED, config.reloadNow().outcome());
        assertEquals(AuditHashAlgorithm.SHA_256, config.snapshot().hashAlgorithm());

        Files.write(file, "audit.buffer-bytes = 8192\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(ReloadReport.Outcome.FAILED, config.reloadNow().outcome());
        assertEquals(4096, config.snapshot().bufferBytes());
    }
}
