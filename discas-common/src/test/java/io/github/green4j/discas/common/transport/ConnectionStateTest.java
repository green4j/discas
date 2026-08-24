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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("TCP transport connection state")
class ConnectionStateTest {
    private static final int MAX_FRAME_BYTES = 256;
    private static final int INITIAL_RX_BYTES = 260;
    private static final int MAX_INFLIGHT_BYTES = 512;
    private static final int CHUNK_PAYLOAD_BYTES =
            FrameCodec.maxPayloadBytesForFrame(MAX_FRAME_BYTES) - Long.BYTES - Integer.BYTES;

    @Test
    void queueAgeStartsOnlyAfterConnectionEstablished() {
        final ConnectionState state = new ConnectionState(null,
                INITIAL_RX_BYTES, MAX_FRAME_BYTES, CHUNK_PAYLOAD_BYTES, MAX_INFLIGHT_BYTES,
                false, null);

        state.enqueue(ByteBuffer.allocate(8));
        assertEquals(0L, state.oldestQueuedAtNanos);

        state.markConnected();
        assertTrue(state.oldestQueuedAtNanos > 0L);
    }

    @Test
    void estimatedBytesIncludesRxAndQueuedAndInbound() {
        final ConnectionState state = new ConnectionState(null,
                INITIAL_RX_BYTES, MAX_FRAME_BYTES, CHUNK_PAYLOAD_BYTES, MAX_INFLIGHT_BYTES,
                true, null);
        final long rxCap = state.rxBuffer.capacity();
        assertEquals(rxCap, state.estimatedBytes());

        state.enqueue(ByteBuffer.allocate(100));
        assertEquals(rxCap + 100, state.estimatedBytes());
    }

    @Test
    void pooledBufferUsedWhenProvided() {
        final HeapBufferPool pool = new HeapBufferPool(INITIAL_RX_BYTES, 4);
        pool.release(pool.acquire());

        final ConnectionState state = new ConnectionState(null,
                INITIAL_RX_BYTES, MAX_FRAME_BYTES, CHUNK_PAYLOAD_BYTES, MAX_INFLIGHT_BYTES,
                false, pool);
        assertEquals(0, pool.pooledCount());
        assertEquals(INITIAL_RX_BYTES, state.rxBuffer.capacity());
        assertFalse(state.rxBuffer.isDirect());
    }
}
