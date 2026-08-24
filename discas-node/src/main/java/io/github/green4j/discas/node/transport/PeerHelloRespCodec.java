/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;

import java.nio.ByteBuffer;

/**
 * Codec for the {@link FrameCodec#TYPE_PEER_HELLO_RESP} frame payload.
 * <p>
 * Layout:
 * {@code [byte status][string cause][byte clusterSize][string incarnationId][int64 promiseCeiling]}. {@code cause} disambiguates
 * the several {@link PeerHelloRespStatus#IDENTITY_MISMATCH} situations (see the
 * {@code CAUSE_*} constants); it is nullable and encoded as such. {@code clusterSize} is the
 * responder's frozen N (1..255, carried unsigned), written on rejections too so the initiator
 * can run the reconfiguration guard independently.
 */
final class PeerHelloRespCodec {

    /**
     * Maximum UTF-8 length of the diagnostic {@code cause}. The text is assembled locally from
     * a fixed set of phrases; the bound exists so the encoder has a cap like every other
     * framing, not because a caller can drive it.
     */
    private static final int MAX_CAUSE_BYTES = 1024;

    /** Upper bound on an encoded payload. */
    public static final int MAX_PAYLOAD_BYTES =
            1                                             // status code
                    + Integer.BYTES + MAX_CAUSE_BYTES     // writeNullableString(cause)
                    + 1                                   // cluster size
                    + Integer.BYTES + 36          // writeString(incarnationId)
                    + Long.BYTES;                         // promise ceiling

    private PeerHelloRespCodec() {
    }

    /** Encode a hello response (returned buffer is flipped, ready to frame). */
    public static ByteBuffer encode(final PeerHelloRespStatus status, final String cause,
                                    final int clusterSize, final IncarnationId incarnationId,
                                    final long promiseCeiling) {
        final MessageBufferWriter out = new MessageBufferWriter(32, MAX_PAYLOAD_BYTES);
        out.writeByte(status.code());
        out.writeNullableString(cause);
        out.writeByte((byte) clusterSize);
        out.writeString(incarnationId.value());
        out.writeLong(promiseCeiling);
        return out.toByteBuffer();
    }

    /**
     * Decode a hello response. Reads from a duplicate so the caller's buffer position is left
     * untouched.
     *
     * @throws IllegalArgumentException if the payload is malformed or carries an unknown status
     */
    public static Decoded decode(final ByteBuffer payload) {
        final MessageBufferReader in = new MessageBufferReader(payload.duplicate());
        final PeerHelloRespStatus status = PeerHelloRespStatus.fromCode(in.readByte());
        final String cause = in.readNullableString();
        final int clusterSize = Byte.toUnsignedInt(in.readByte());
        final IncarnationId incarnationId = IncarnationId.of(in.readString());
        final long promiseCeiling = in.readLong();
        if (in.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after PEER_HELLO_RESP decode");
        }
        return new Decoded(status, cause, clusterSize, incarnationId, promiseCeiling);
    }

    /** A decoded PEER_HELLO_RESP. */
    public static final class Decoded {
        public final PeerHelloRespStatus status;
        public final String cause;
        public final int clusterSize;
        /**
         * The responder's incarnation, carried for the same reason {@link #clusterSize} is: the
         * dialer sends the HELLO, so without this it would never learn who it just connected to --
         * only the acceptor would get to run the guard, and the check would be one-directional.
         */
        final IncarnationId incarnationId;
        /** The responder's forced promise ceiling, carried for the same reason as the two above. */
        public final long promiseCeiling;

        Decoded(final PeerHelloRespStatus status, final String cause, final int clusterSize,
                final IncarnationId incarnationId, final long promiseCeiling) {
            this.status = status;
            this.cause = cause;
            this.clusterSize = clusterSize;
            this.incarnationId = incarnationId;
            this.promiseCeiling = promiseCeiling;
        }
    }
}
