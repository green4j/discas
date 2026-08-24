/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.transport;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.FrameCodec;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;

import java.nio.ByteBuffer;

/**
 * Codec for the {@link FrameCodec#TYPE_PEER_HELLO} frame payload.
 * <p>
 * Layout:
 * {@code [int32 version][string clusterId][string nodeId][int64 nonce][int64 epochMs][byte clusterSize]}
 * {@code [string incarnationId][int64 promiseCeiling]}.
 * The leading {@code version} keeps the first position so a receiver can always read it and
 * reject a mismatch before parsing the rest -- see {@link #peekVersion}, which is the peer-side
 * analogue of the same split in {@code ClientHello}.
 */
final class PeerHelloCodec {

    /** A UUID's canonical text form; the field is length-prefixed, so this only bounds the frame. */
    private static final int MAX_INCARNATION_BYTES = 36;

    /** Upper bound on an encoded payload. */
    public static final int MAX_PAYLOAD_BYTES =
            Integer.BYTES                                             // version
                    + Integer.BYTES + KvLimits.MAX_CLUSTER_ID_BYTES   // writeString(clusterId)
                    + Integer.BYTES + KvLimits.MAX_NODE_ID_BYTES      // writeString(nodeId)
                    + Long.BYTES                                      // nonce
                    + Long.BYTES                                      // epoch millis
                    + 1                                               // cluster size
                    + Integer.BYTES + MAX_INCARNATION_BYTES    // writeString(incarnationId)
                    + Long.BYTES;                                     // promise ceiling

    private PeerHelloCodec() {
    }

    /** Encode a hello payload (returned buffer is flipped, ready to frame). */
    public static ByteBuffer encode(final int version, final ClusterId clusterId,
                                    final NodeId nodeId, final long nonce,
                                    final long epochMs, final int clusterSize,
                                    final IncarnationId incarnationId,
                                    final long promiseCeiling) {
        final MessageBufferWriter out = new MessageBufferWriter(96, MAX_PAYLOAD_BYTES);
        out.writeInt(version);
        out.writeString(clusterId.value());
        out.writeString(nodeId.value());
        out.writeLong(nonce);
        out.writeLong(epochMs);
        out.writeByte((byte) clusterSize);
        out.writeString(incarnationId.value());
        out.writeLong(promiseCeiling);
        return out.toByteBuffer();
    }

    /**
     * Read only the leading protocol version, leaving the caller's buffer untouched.
     * <p>
     * The rest of the payload's layout may differ across protocol versions, so a receiver that
     * has not yet confirmed the version must not decode past this field.
     *
     * @throws java.nio.BufferUnderflowException if the payload is too short to carry a version
     */
    public static int peekVersion(final ByteBuffer payload) {
        return new MessageBufferReader(payload.duplicate()).readInt();
    }

    /**
     * Decode a hello payload whose version has already been confirmed by {@link #peekVersion}.
     * Reads from a duplicate so the caller's buffer position is left untouched.
     *
     * @throws IllegalArgumentException if the payload is malformed
     */
    public static Decoded decode(final ByteBuffer payload) {
        final MessageBufferReader in = new MessageBufferReader(payload.duplicate());
        final int version = in.readInt();
        final ClusterId clusterId = ClusterId.of(in.readString());
        final NodeId nodeId = NodeId.of(in.readString());
        final long nonce = in.readLong();
        final long epochMs = in.readLong();
        final int clusterSize = Byte.toUnsignedInt(in.readByte());
        final IncarnationId incarnationId = IncarnationId.of(in.readString());
        final long promiseCeiling = in.readLong();
        if (in.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after PEER_HELLO decode");
        }
        return new Decoded(version, clusterId, nodeId, nonce, epochMs, clusterSize, incarnationId,
                promiseCeiling);
    }

    /** A decoded PEER_HELLO. */
    public static final class Decoded {
        public final int version;
        public final ClusterId clusterId;
        public final NodeId nodeId;
        /** Reserved for a future nonce cache; read so the layout stays explicit. */
        final long nonce;
        final long epochMs;
        public final int clusterSize;
        /**
         * Which run of the sender's storage this is. A known {@link #nodeId} arriving with a
         * different one has been wiped.
         */
        final IncarnationId incarnationId;
        /**
         * The highest promise ballot the sender has forced, or
         * {@link PromiseCeilingHistory#UNKNOWN} while it has not replayed its own log and cannot
         * say. Together with {@link #incarnationId} it states, as something two nodes can check,
         * that the same storage may never come back reporting less than it left with.
         */
        public final long promiseCeiling;

        Decoded(final int version, final ClusterId clusterId, final NodeId nodeId,
                final long nonce, final long epochMs, final int clusterSize,
                final IncarnationId incarnationId, final long promiseCeiling) {
            this.version = version;
            this.clusterId = clusterId;
            this.nodeId = nodeId;
            this.nonce = nonce;
            this.epochMs = epochMs;
            this.clusterSize = clusterSize;
            this.incarnationId = incarnationId;
            this.promiseCeiling = promiseCeiling;
        }
    }
}
