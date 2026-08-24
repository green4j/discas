/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ClientTransportConfig -- defaults and validation")
class ClientTransportConfigTest {

    @Test
    @DisplayName("A default config is usable and its budgets clear the max message size")
    void defaultsAreUsable() {
        final ClientTransportConfig cfg = ClientTransportConfig.defaults();

        assertEquals(256 * 1024, cfg.maxFrameBytes());
        assertEquals(512 * 1024, cfg.maxRxBufferBytes());
        assertEquals(128, cfg.maxConnections());
        // The queue and inflight defaults are derived from what validate() demands of them, so
        // the two cannot drift apart.
        assertTrue(cfg.maxQueuedOutBytes() >= ClientMessageCodec.MAX_MESSAGE_BYTES);
        assertTrue(cfg.maxInflightBytes() >= ClientMessageCodec.MAX_MESSAGE_BYTES);
    }

    @Test
    void rejectsZeroFields() {
        assertThrows(IllegalArgumentException.class,
                () -> ClientTransportConfig.builder().maxFrameBytes(0));
        assertThrows(IllegalArgumentException.class,
                () -> ClientTransportConfig.builder().maxQueuedOutBytes(0));
        assertThrows(IllegalArgumentException.class,
                () -> ClientTransportConfig.builder().maxRxBufferBytes(0));
        assertThrows(IllegalArgumentException.class,
                () -> ClientTransportConfig.builder().maxInflightBytes(0));
        assertThrows(IllegalArgumentException.class,
                () -> ClientTransportConfig.builder().maxConnections(0));
    }

    @Test
    @DisplayName("Rejects maxInflightBytes / maxQueuedOutBytes below the max client message size")
    void rejectsBuffersBelowMaxMessage() {
        final int maxFrame = 1_048_576;
        final int rx = maxFrame + 8;
        final int ok = ClientMessageCodec.MAX_MESSAGE_BYTES;
        // maxInflightBytes below the floor -> cannot reassemble a max-size request.
        assertThrows(IllegalArgumentException.class, () -> ClientTransportConfig.builder()
                .maxFrameBytes(maxFrame).maxQueuedOutBytes(ok).maxRxBufferBytes(rx)
                .maxInflightBytes(ok - 1).maxConnections(64).build());
        // maxQueuedOutBytes below the floor -> cannot enqueue a max-size response.
        assertThrows(IllegalArgumentException.class, () -> ClientTransportConfig.builder()
                .maxFrameBytes(maxFrame).maxQueuedOutBytes(ok - 1).maxRxBufferBytes(rx)
                .maxInflightBytes(ok).maxConnections(64).build());
    }
}
