/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Length-prefixed transport framing:
 * {@code [int32 frameLen][int32 frameCrc32][byte frameType][payload...]}, where {@code frameLen}
 * counts the CRC, the type and the payload. Every multi-byte field is big-endian, stated
 * explicitly at each allocation rather than left to the {@link ByteBuffer} default.
 * <p>
 * {@code frameCrc32} covers the type and the payload. A mismatch on decode throws
 * {@link IllegalArgumentException}, the same class an invalid length raises, and a transport
 * closes the connection on it.
 */
public final class FrameCodec {
    public static final byte TYPE_CHUNK_START = 2;
    public static final byte TYPE_CHUNK_PART = 3;
    public static final byte TYPE_CHUNK_END = 4;
    public static final byte TYPE_PEER_HELLO = 5;
    public static final byte TYPE_CLIENT_HELLO = 6;
    public static final byte TYPE_PEER_HELLO_RESP = 7;
    public static final byte TYPE_CLIENT_HELLO_RESP = 8;
    public static final byte TYPE_PEER_MESSAGE = 9;
    public static final byte TYPE_CLIENT_MESSAGE = 10;
    public static final int FRAME_CHECKSUM_BYTES = Integer.BYTES;
    public static final int FRAME_TYPE_BYTES = 1;
    public static final int FRAME_LENGTH_PREFIX_BYTES = Integer.BYTES;

    private final int maxFrameBytes;

    /**
     * Reused across calls, since {@link #drain} runs once per readable poll on every connection.
     * Safe because a codec belongs to one transport, which drains only from its own event-loop
     * thread and consumes each result before draining again. The codec's only mutable state.
     */
    private final List<Frame> drained = new ArrayList<>();

    public FrameCodec(final int maxFrameBytes) {
        this.maxFrameBytes = maxFrameBytes;
    }

    public ByteBuffer encode(final byte frameType, final ByteBuffer payload) {
        final int payloadLen = payload.remaining();
        final int frameLen = FRAME_CHECKSUM_BYTES + FRAME_TYPE_BYTES + payloadLen;
        if (frameLen > maxFrameBytes) {
            throw new IllegalArgumentException("Frame too large: " + frameLen);
        }
        final ByteBuffer out = ByteBuffer.allocate(FRAME_LENGTH_PREFIX_BYTES + frameLen)
                .order(ByteOrder.BIG_ENDIAN);
        out.putInt(frameLen);
        out.putInt((int) computeCrc(frameType, payload));
        out.put(frameType);
        out.put(payload.duplicate());
        out.flip();
        return out;
    }

    /**
     * Decode all complete frames currently present in {@code rxBuffer}.
     * rxBuffer is expected in write mode before call and remains in write mode
     * after call. Each decoded frame carries an independent heap ByteBuffer
     * payload (not aliased to the rx buffer).
     * <p>
     * The returned list is <b>reused</b> by the next call on this codec: consume it before
     * draining again, and do not retain it.
     */
    public List<Frame> drain(final ByteBuffer rxBuffer) {
        final List<Frame> frames = drained;
        frames.clear();
        rxBuffer.order(ByteOrder.BIG_ENDIAN).flip();
        while (rxBuffer.remaining() >= FRAME_LENGTH_PREFIX_BYTES) {
            rxBuffer.mark();
            final int frameLen = rxBuffer.getInt();
            if (frameLen <= FRAME_CHECKSUM_BYTES + FRAME_TYPE_BYTES
                    || frameLen > maxFrameBytes) {
                throw new IllegalArgumentException("Invalid frame length: " + frameLen);
            }
            if (rxBuffer.remaining() < frameLen) {
                rxBuffer.reset();
                break;
            }
            final int storedCrc = rxBuffer.getInt();
            final byte frameType = rxBuffer.get();
            final int payloadLen = frameLen - FRAME_CHECKSUM_BYTES - FRAME_TYPE_BYTES;
            final ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.BIG_ENDIAN);
            if (payloadLen > 0) {
                final ByteBuffer src = rxBuffer.slice();
                src.limit(payloadLen);
                payload.put(src);
                rxBuffer.position(rxBuffer.position() + payloadLen);
            }
            payload.flip();
            final int actualCrc = (int) computeCrc(frameType, payload);
            if (storedCrc != actualCrc) {
                throw new IllegalArgumentException("Frame checksum mismatch");
            }
            frames.add(new Frame(frameType, payload));
        }
        rxBuffer.compact();
        return frames;
    }

    private static long computeCrc(final byte frameType, final ByteBuffer payload) {
        final CRC32 crc = new CRC32();
        crc.update(frameType);
        if (payload.remaining() > 0) {
            crc.update(payload.duplicate());
        }
        return crc.getValue();
    }

    public static int maxPayloadBytesForFrame(final int maxFrameBytes) {
        final int payloadBytes = maxFrameBytes - FRAME_CHECKSUM_BYTES - FRAME_TYPE_BYTES;
        if (payloadBytes < 0) {
            throw new IllegalArgumentException("maxFrameBytes too small for frame overhead");
        }
        return payloadBytes;
    }

    public static final class Frame {
        public final byte type;
        public final ByteBuffer payload;

        public Frame(final byte type, final ByteBuffer payload) {
            this.type = type;
            this.payload = payload;
        }
    }
}
