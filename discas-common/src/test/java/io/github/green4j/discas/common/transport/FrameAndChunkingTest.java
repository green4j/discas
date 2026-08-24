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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Frame codec -- chunking + boundary cases")
class FrameAndChunkingTest {
    @Test
    @DisplayName("Encode + decode roundtrips the payload bit-exactly")
    void encodeDecodeRoundtrip() {
        final FrameCodec codec = new FrameCodec(1024);
        final byte[] payload = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        final ByteBuffer encoded = codec.encode(FrameCodec.TYPE_PEER_MESSAGE, ByteBuffer.wrap(payload));

        final ByteBuffer rx = ByteBuffer.allocate(1024);
        rx.put(encoded);
        final List<FrameCodec.Frame> frames = codec.drain(rx);

        assertEquals(1, frames.size());
        assertEquals(FrameCodec.TYPE_PEER_MESSAGE, frames.get(0).type);
        assertEquals(ByteBuffer.wrap(payload), frames.get(0).payload);
    }

    @Test
    @DisplayName("A single-byte payload bit flip is rejected by the frame CRC")
    void flippedPayloadByteRejected() {
        final FrameCodec codec = new FrameCodec(1024);
        final byte[] payload = new byte[]{10, 20, 30, 40, 50, 60, 70, 80};
        final ByteBuffer encoded = codec.encode(FrameCodec.TYPE_PEER_MESSAGE, ByteBuffer.wrap(payload));

        // The first payload byte sits at offset:
        //   4 (frameLen) + 4 (crc) + 1 (frameType) = 9.
        final int payloadStart = FrameCodec.FRAME_LENGTH_PREFIX_BYTES
                + FrameCodec.FRAME_CHECKSUM_BYTES + FrameCodec.FRAME_TYPE_BYTES;
        encoded.put(payloadStart, (byte) (encoded.get(payloadStart) ^ 0x40));

        final ByteBuffer rx = ByteBuffer.allocate(1024);
        rx.put(encoded);
        assertThrows(IllegalArgumentException.class, () -> codec.drain(rx));
    }

    @Test
    void singleAssemblyRoundTrip() {
        final ChunkingEngine sender = engine(64, 256);
        final ChunkingEngine receiver = engine(64, 256);

        final byte[] payload = new byte[120];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xFF);
        }

        final List<FrameCodec.Frame> frames = sender.encodePayload(FrameCodec.TYPE_PEER_MESSAGE,
                ByteBuffer.wrap(payload));
        assertTrue(frames.size() > 2);

        ByteBuffer result = null;
        for (final FrameCodec.Frame frame : frames) {
            result = receiver.onFrame(frame);
        }
        assertEquals(ByteBuffer.wrap(payload), result);
        assertFalse(receiver.hasActiveAssembly());
        assertEquals(0, receiver.inboundBytes());
    }

    @Test
    void outOfOrderSeqRejected() {
        final ChunkingEngine engine = engine(64, 256);
        engine.onFrame(startFrame(1L, 20));
        engine.onFrame(partFrame(1L, 0, new byte[]{1, 2}));
        assertThrows(IllegalArgumentException.class,
                () -> engine.onFrame(partFrame(1L, 5, new byte[]{3})));
    }

    private static ChunkingEngine engine(final int maxFrameBytes, final int maxInflightBytes) {
        final int chunkPayloadBytes = FrameCodec.maxPayloadBytesForFrame(maxFrameBytes)
                - Long.BYTES - Integer.BYTES;
        return new ChunkingEngine(maxFrameBytes, chunkPayloadBytes, maxInflightBytes);
    }

    private static FrameCodec.Frame startFrame(final long streamId, final int totalSize) {
        final ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        payload.putLong(streamId);
        payload.putInt(totalSize);
        payload.flip();
        return new FrameCodec.Frame(FrameCodec.TYPE_CHUNK_START, payload);
    }

    private static FrameCodec.Frame partFrame(final long streamId, final int seq, final byte[] chunk) {
        final ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + chunk.length);
        payload.putLong(streamId);
        payload.putInt(seq);
        payload.put(chunk);
        payload.flip();
        return new FrameCodec.Frame(FrameCodec.TYPE_CHUNK_PART, payload);
    }
}
