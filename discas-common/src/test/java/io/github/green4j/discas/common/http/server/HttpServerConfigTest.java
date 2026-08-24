/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.http.server;

import io.github.green4j.discas.common.http.server.HttpServer.HttpServerConfig;
import io.github.green4j.discas.common.http.server.HttpServer.HttpServerConfigBuilder;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link HttpServerConfig} is immutable to callers: a built config must be a snapshot, or a caller
 * holding the builder could mutate the config a running server is already using.
 */
@DisplayName("HttpServerConfig -- a built config is a snapshot, not a live view")
class HttpServerConfigTest {

    /** Every setter given a distinct non-default value, so a dropped field shows up as a mismatch. */
    private static HttpServerConfigBuilder fullyPopulated() {
        return HttpServer.builder()
                .bindAddress("10.1.2.3")
                .port(1234)
                .workerCount(3)
                .headerTimeoutMs(11)
                .bodyTimeoutMs(22)
                .requestTimeoutMs(33)
                .writeTimeoutMs(44)
                .keepAliveTimeoutMs(55)
                .maxConnectionsPerWorker(66)
                .gzipLevel(7)
                .maxRequestBodyBytes(88)
                .inputBufferBytes(4096)
                .outputBufferBytes(8192)
                .acceptBacklog(99)
                .gzipStagingBytes(2048)
                .gzipScratchBytes(1024)
                .gzipInitialOutBytes(512);
    }

    private static void assertFullyPopulated(final HttpServerConfig cfg) {
        assertEquals("10.1.2.3", cfg.bindAddress());
        assertEquals(1234, cfg.port());
        assertEquals(3, cfg.workerCount());
        assertEquals(11, cfg.headerTimeoutMs());
        assertEquals(22, cfg.bodyTimeoutMs());
        assertEquals(33, cfg.requestTimeoutMs());
        assertEquals(44, cfg.writeTimeoutMs());
        assertEquals(55, cfg.keepAliveTimeoutMs());
        assertEquals(66, cfg.maxConnectionsPerWorker());
        assertEquals(7, cfg.gzipLevel());
        assertEquals(88, cfg.maxRequestBodyBytes());
        assertEquals(4096, cfg.inputBufferBytes());
        assertEquals(8192, cfg.outputBufferBytes());
        assertEquals(99, cfg.acceptBacklog());
        assertEquals(2048, cfg.gzipStagingBytes());
        assertEquals(1024, cfg.gzipScratchBytes());
        assertEquals(512, cfg.gzipInitialOutBytes());
    }

    @Test
    @DisplayName("build() carries every tunable across -- a dropped field would surface here")
    void buildCopiesEveryField() {
        // Guards the copy constructor. Forgetting one field there would silently hand the server a
        // default instead of the configured value, which no other test would notice.
        assertFullyPopulated(fullyPopulated().build());
    }

    @Test
    @DisplayName("Mutating the builder afterwards does not touch an already-built config")
    void builderCannotMutateABuiltConfig() {
        final HttpServerConfigBuilder builder = fullyPopulated();
        final HttpServerConfig built = builder.build();

        // The scenario that mattered: a server is already running on `built`.
        builder.port(9999).workerCount(64).maxRequestBodyBytes(1);

        assertEquals(1234, built.port(), "A running server's config must not change under it");
        assertEquals(3, built.workerCount());
        assertEquals(88, built.maxRequestBodyBytes());
    }

    @Test
    @DisplayName("Defaults survive a build untouched")
    void defaultsAreCopied() {
        final HttpServerConfig cfg = HttpServer.builder().build();

        // Defaults live inline on the config's fields; the copy must preserve them rather than
        // re-deriving or zeroing them.
        assertEquals("127.0.0.1", cfg.bindAddress());
        assertEquals(8080, cfg.port());
        assertEquals(1, cfg.workerCount());
        assertEquals(16 * 1024, cfg.inputBufferBytes());
        assertEquals(32 * 1024, cfg.outputBufferBytes());
        assertEquals(256, cfg.gzipInitialOutBytes());
    }

    @Test
    @DisplayName("Size setters still fail fast on invalid values")
    void sizeSettersValidate() {
        assertThrows(IllegalArgumentException.class, () -> HttpServer.builder().workerCount(0));
        assertThrows(IllegalArgumentException.class, () -> HttpServer.builder().gzipInitialOutBytes(0));
    }
}
