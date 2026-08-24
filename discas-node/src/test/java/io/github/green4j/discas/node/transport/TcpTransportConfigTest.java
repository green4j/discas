/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.node.PeerMessageCodec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("TcpTransportConfig -- validation")
class TcpTransportConfigTest {

    @Test
    void rejectsMaxFrameBytesTooSmallForChunking() {
        assertThrows(IllegalArgumentException.class, () -> TcpTransportConfig.builder()
                .maxFrameBytes(5).maxQueuedOutBytes(2048).maxRxBufferBytes(32)
                .maxInflightBytes(512).maxConnections(10).build());
    }

    @Test
    @DisplayName("Rejects maxInflightBytes / maxQueuedOutBytes below the max peer message size")
    void rejectsBuffersBelowMaxMessage() {
        final int maxFrame = 1_048_576;
        final int rx = maxFrame + 8;
        final int ok = PeerMessageCodec.MAX_MESSAGE_BYTES;
        // maxInflightBytes below the floor -> cannot reassemble a max-size accept.
        assertThrows(IllegalArgumentException.class, () -> TcpTransportConfig.builder()
                .maxFrameBytes(maxFrame).maxQueuedOutBytes(ok).maxRxBufferBytes(rx)
                .maxInflightBytes(ok - 1).maxConnections(64).build());
        // maxQueuedOutBytes below the floor -> cannot enqueue a max-size accept.
        assertThrows(IllegalArgumentException.class, () -> TcpTransportConfig.builder()
                .maxFrameBytes(maxFrame).maxQueuedOutBytes(ok - 1).maxRxBufferBytes(rx)
                .maxInflightBytes(ok).maxConnections(64).build());
    }
}
