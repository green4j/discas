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

/**
 * Single-active-message chunking/reassembly at transport-frame level
 * <p>
 * Only one chunked message may be in-flight per connection at a time
 * A new CHUNK_START while a previous assembly is active is a protocol violation
 */
public final class ChunkingEngine {
    private final int maxSingleFramePayloadBytes;
    private final int chunkPayloadBytes;
    private final int maxInflightBytes;

    private InboundAssembly active;
    private int inboundBytes = 0;
    private long nextStreamId = 1;
    private long activeStartedAtNanos = 0L;

    public ChunkingEngine(final int maxFrameBytes,
                          final int chunkPayloadBytes,
                          final int maxInflightBytes) {
        // Derived once: it is a function of maxFrameBytes, which does not change, and it is asked
        // for on every encode.
        this.maxSingleFramePayloadBytes = FrameCodec.maxPayloadBytesForFrame(maxFrameBytes);
        this.chunkPayloadBytes = chunkPayloadBytes;
        this.maxInflightBytes = maxInflightBytes;
    }

    /**
     * Encode {@code payload} as one or more transport frames. If the
     * payload fits in a single frame, a single {@code singleFrameType}
     * frame is emitted; otherwise a CHUNK_START / CHUNK_PART... / CHUNK_END
     * stream is emitted. The single-frame type byte is supplied by the
     * caller so this engine stays side-agnostic.
     */
    public List<FrameCodec.Frame> encodePayload(final byte singleFrameType,
                                                final ByteBuffer payload) {
        final List<FrameCodec.Frame> result = new ArrayList<>();
        final int payloadLen = payload.remaining();
        if (payloadLen <= maxSingleFramePayloadBytes) {
            result.add(new FrameCodec.Frame(singleFrameType, payload.duplicate()));
            return result;
        }

        final long streamId = nextStreamId++;
        final ByteBuffer start = ByteBuffer.allocate(Long.BYTES + Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        start.putLong(streamId);
        start.putInt(payloadLen);
        start.flip();
        result.add(new FrameCodec.Frame(FrameCodec.TYPE_CHUNK_START, start));

        final ByteBuffer src = payload.duplicate();
        int seq = 0;
        while (src.hasRemaining()) {
            final int chunkSize = Math.min(chunkPayloadBytes, src.remaining());
            final ByteBuffer part = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + chunkSize)
                    .order(ByteOrder.BIG_ENDIAN);
            part.putLong(streamId);
            part.putInt(seq++);
            final ByteBuffer chunk = src.slice();
            chunk.limit(chunkSize);
            part.put(chunk);
            src.position(src.position() + chunkSize);
            part.flip();
            result.add(new FrameCodec.Frame(FrameCodec.TYPE_CHUNK_PART, part));
        }

        final ByteBuffer end = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
        end.putLong(streamId);
        end.flip();
        result.add(new FrameCodec.Frame(FrameCodec.TYPE_CHUNK_END, end));
        return result;
    }

    /**
     * Feed a chunk-stream frame into the reassembler. Single-frame
     * application payloads ({@code TYPE_PEER_MESSAGE} /
     * {@code TYPE_CLIENT_MESSAGE}) are not the engine's concern -- the
     * transport handles those directly.
     *
     * @return the fully reassembled payload on CHUNK_END, otherwise null
     * @throws IllegalArgumentException on protocol violation (caller should close the connection)
     */
    public ByteBuffer onFrame(final FrameCodec.Frame frame) {
        switch (frame.type) {
            case FrameCodec.TYPE_CHUNK_START:
                return onChunkStart(frame);
            case FrameCodec.TYPE_CHUNK_PART:
                return onChunkPart(frame);
            case FrameCodec.TYPE_CHUNK_END:
                return onChunkEnd(frame);
            default:
                throw new IllegalArgumentException("Unknown frame type: " + frame.type);
        }
    }

    private ByteBuffer onChunkStart(final FrameCodec.Frame frame) {
        ensurePayloadSize(frame.payload, Long.BYTES + Integer.BYTES, "chunk start");
        if (active != null) {
            resetAssembly();
            throw new IllegalArgumentException("New chunk stream while assembly active");
        }
        final ByteBuffer in = frame.payload.duplicate();
        final long streamId = in.getLong();
        final int totalSize = in.getInt();
        if (totalSize <= 0 || totalSize > maxInflightBytes) {
            throw new IllegalArgumentException("Invalid chunk stream total size: " + totalSize);
        }
        active = new InboundAssembly(streamId, totalSize);
        inboundBytes += totalSize;
        activeStartedAtNanos = System.nanoTime();
        return null;
    }

    private ByteBuffer onChunkPart(final FrameCodec.Frame frame) {
        if (frame.payload.remaining() < Long.BYTES + Integer.BYTES) {
            throw new IllegalArgumentException("Invalid chunk part payload size");
        }
        final ByteBuffer in = frame.payload.duplicate();
        final long streamId = in.getLong();
        final int seq = in.getInt();
        if (active == null || active.streamId != streamId) {
            throw new IllegalArgumentException("Chunk part for unknown stream: " + streamId);
        }
        if (seq != active.nextSeq) {
            throw new IllegalArgumentException("Out-of-order chunk seq: " + seq);
        }
        final int chunkSize = in.remaining();
        if (active.data.position() + chunkSize > active.data.capacity()) {
            throw new IllegalArgumentException("Chunk overflow for stream: " + streamId);
        }
        if (chunkSize > chunkPayloadBytes) {
            throw new IllegalArgumentException("Chunk part too large: " + chunkSize);
        }
        active.data.put(in);
        active.nextSeq++;
        return null;
    }

    private ByteBuffer onChunkEnd(final FrameCodec.Frame frame) {
        ensurePayloadSize(frame.payload, Long.BYTES, "chunk end");
        final ByteBuffer in = frame.payload.duplicate();
        final long streamId = in.getLong();
        if (active == null || active.streamId != streamId) {
            throw new IllegalArgumentException("Chunk end for unknown stream: " + streamId);
        }
        if (active.data.position() != active.data.capacity()) {
            resetAssembly();
            throw new IllegalArgumentException("Incomplete chunk stream: " + streamId);
        }
        active.data.flip();
        final ByteBuffer data = active.data;
        resetAssembly();
        return data;
    }

    public int inboundBytes() {
        return inboundBytes;
    }

    /**
     * True when an inbound chunked stream has been mid-reassembly for
     * longer than {@code timeoutNanos}. Used by the transport to evict
     * peers that started sending a chunked payload and went silent.
     */
    public boolean isStalled(final long timeoutNanos) {
        if (active == null) {
            return false;
        }
        return System.nanoTime() - activeStartedAtNanos > timeoutNanos;
    }

    public boolean hasActiveAssembly() {
        return active != null;
    }

    private void resetAssembly() {
        if (active != null) {
            inboundBytes -= active.data.capacity();
            active = null;
            activeStartedAtNanos = 0L;
        }
    }

    private static void ensurePayloadSize(final ByteBuffer payload, final int expected, final String frameName) {
        if (payload.remaining() != expected) {
            throw new IllegalArgumentException("Invalid " + frameName + " payload size: " + payload.remaining());
        }
    }

    private static final class InboundAssembly {
        final long streamId;
        final ByteBuffer data;
        int nextSeq;

        InboundAssembly(final long streamId, final int totalSize) {
            this.streamId = streamId;
            this.data = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
            this.nextSeq = 0;
        }
    }
}
