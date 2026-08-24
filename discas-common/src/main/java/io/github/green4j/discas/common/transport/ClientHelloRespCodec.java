/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.transport;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Codec for the {@link FrameCodec#TYPE_CLIENT_HELLO_RESP} frame payload.
 * <p>
 * Layout: {@code [byte status][byte clusterSize][long coordinatorEpochMs]}. {@code clusterSize} is
 * the node's frozen N (1..255, carried unsigned) and is written on rejections too so the frame stays
 * fixed-size. Clients use it to derive the scan quorum instead of assuming their node list is the
 * whole cluster.
 * <p>
 * {@code coordinatorEpochMs} is the node's wall clock, read as the response is built. It is what
 * lets a client measure the offset between its own clock and the cluster's, which lock leases are
 * written and judged against -- a lease deadline crosses processes, so it has to be expressed on a
 * clock they can share. Sent on rejections too, for the same fixed-size reason.
 * <p>
 * One reading per connection, not per request: a clock offset moves on the scale of NTP discipline,
 * and a client detects a step in its <em>own</em> clock without asking anyone.
 */
public final class ClientHelloRespCodec {

    /** Exact encoded size -- the payload is fixed-width. */
    public static final int PAYLOAD_BYTES = 10;

    private ClientHelloRespCodec() {
    }

    /**
     * Encode a hello response (returned buffer is flipped, ready to frame).
     *
     * @param coordinatorEpochMs the responding node's wall clock, in epoch millis, read as close to
     *                           writing the frame as the caller can manage -- every millisecond
     *                           between the reading and the send is error in the client's offset
     */
    public static ByteBuffer encode(final ClientHelloRespStatus status, final int clusterSize,
                                    final long coordinatorEpochMs) {
        final ByteBuffer out = ByteBuffer.allocate(PAYLOAD_BYTES).order(ByteOrder.BIG_ENDIAN);
        out.put(status.code());
        out.put((byte) clusterSize);
        out.putLong(coordinatorEpochMs);
        out.flip();
        return out;
    }

    /**
     * Decode a hello response. Reads from a duplicate so the caller's buffer position is left
     * untouched.
     *
     * @throws IllegalArgumentException if the payload is not exactly {@link #PAYLOAD_BYTES}
     *                                  long or carries an unknown status
     */
    public static Decoded decode(final ByteBuffer payload) {
        if (payload.remaining() != PAYLOAD_BYTES) {
            throw new IllegalArgumentException("CLIENT_HELLO_RESP must be " + PAYLOAD_BYTES
                    + " bytes, got " + payload.remaining());
        }
        final ByteBuffer in = payload.duplicate().order(ByteOrder.BIG_ENDIAN);
        final ClientHelloRespStatus status = ClientHelloRespStatus.fromCode(in.get());
        return new Decoded(status, Byte.toUnsignedInt(in.get()), in.getLong());
    }

    /** A decoded CLIENT_HELLO_RESP. */
    public static final class Decoded {
        public final ClientHelloRespStatus status;
        public final int clusterSize;
        /** The responding node's wall clock when it built this response, in epoch millis. */
        public final long coordinatorEpochMs;

        Decoded(final ClientHelloRespStatus status, final int clusterSize,
                final long coordinatorEpochMs) {
            this.status = status;
            this.clusterSize = clusterSize;
            this.coordinatorEpochMs = coordinatorEpochMs;
        }
    }
}
