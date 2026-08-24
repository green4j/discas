/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Binary codec for the 13 {@link PeerMessage} subtypes. Type-byte
 * dispatch is internal to this class; the wire layout is independent of
 * {@link io.github.green4j.discas.common.client.ClientMessageCodec}.
 */
public final class PeerMessageCodec {

    /**
     * Upper bound on an encoded <b>value-carrying</b> peer message, derived from
     * {@link KvLimits}. The largest such message carries a key, up to two ballots, and a
     * value (an {@code AcceptReq} / {@code PrepareResp}); a transport must be able to
     * reassemble one this large (see {@code TcpTransportConfig.maxInflightBytes}).
     * <p>
     * Anti-entropy messages are not described by this limit -- they scale with the number of
     * keys, not the size of one value -- but both are bounded independently:
     * <ul>
     *   <li>{@code DigestResp} carries at most {@code LocalStore.NUM_RANGES} (256) fixed-size
     *       SHA-256 range digests, so it is inherently small (~10 KiB).</li>
     *   <li>{@code KeysResp} is <b>paged</b>: a range is compared one page at a time, capped by
     *       {@link KvLimits#MAX_ANTI_ENTROPY_KEYS_PER_PAGE} and
     *       {@link KvLimits#MAX_ANTI_ENTROPY_PAGE_BYTES}. Before paging it carried every key
     *       digest in a range, so a dense range could breach the transport's frame/inflight
     *       budgets and have the connection torn down by its own backpressure checks.</li>
     * </ul>
     */
    public static final int MAX_MESSAGE_BYTES =
            1                                                        // type byte
                    + Integer.BYTES + KvLimits.MAX_NODE_ID_BYTES     // putString(senderId)
                    + Long.BYTES                                     // correlationId
                    + Integer.BYTES + KvLimits.MAX_KEY_BYTES         // putBytes(key)
                    + 2 * KvLimits.MAX_BALLOT_BYTES                  // up to two ballots
                    + Integer.BYTES + KvLimits.MAX_VALUE_BYTES       // putNullableBytes(value)
                    + 2;                                             // ok + tombstone flags

    private static final byte PREPARE_REQ = 1;
    private static final byte PREPARE_RESP = 2;
    private static final byte ACCEPT_REQ = 3;
    private static final byte ACCEPT_RESP = 4;
    private static final byte DIGEST_REQ = 5;
    private static final byte DIGEST_RESP = 6;
    private static final byte KEYS_REQ = 7;
    private static final byte KEYS_RESP = 8;
    private static final byte CEILING_REQ = 9;
    private static final byte CEILING_RESP = 10;
    private static final byte PURGE_CHECK = 11;
    private static final byte PURGE_CHECK_RESP = 12;
    private static final byte PURGE = 13;

    private PeerMessageCodec() {
    }

    public static ByteBuffer encode(final PeerMessage message) {
        final MessageBufferWriter out = new MessageBufferWriter(256, MAX_MESSAGE_BYTES);
        if (message instanceof PeerMessage.PrepareReq) {
            final PeerMessage.PrepareReq m = (PeerMessage.PrepareReq) message;
            out.writeByte(PREPARE_REQ);
            writeHeader(out, m);
            out.writeBytes(m.key().toBuffer());
            out.writeBallot(m.ballot());
        } else if (message instanceof PeerMessage.PrepareResp) {
            final PeerMessage.PrepareResp m = (PeerMessage.PrepareResp) message;
            out.writeByte(PREPARE_RESP);
            writeHeader(out, m);
            out.writeBoolean(m.ok());
            out.writeBallot(m.promised());
            out.writeBallot(m.accepted());
            out.writeNullableBytes(HashedBytes.toBuffer(m.value()));
            out.writeBoolean(m.tombstone());
        } else if (message instanceof PeerMessage.AcceptReq) {
            final PeerMessage.AcceptReq m = (PeerMessage.AcceptReq) message;
            out.writeByte(ACCEPT_REQ);
            writeHeader(out, m);
            out.writeBytes(m.key().toBuffer());
            out.writeBallot(m.ballot());
            out.writeNullableBytes(HashedBytes.toBuffer(m.value()));
            out.writeBoolean(m.tombstone());
        } else if (message instanceof PeerMessage.AcceptResp) {
            final PeerMessage.AcceptResp m = (PeerMessage.AcceptResp) message;
            out.writeByte(ACCEPT_RESP);
            writeHeader(out, m);
            out.writeBoolean(m.ok());
            out.writeBallot(m.promised());
        } else if (message instanceof PeerMessage.DigestReq) {
            final PeerMessage.DigestReq m = (PeerMessage.DigestReq) message;
            out.writeByte(DIGEST_REQ);
            writeHeader(out, m);
        } else if (message instanceof PeerMessage.DigestResp) {
            final PeerMessage.DigestResp m = (PeerMessage.DigestResp) message;
            out.writeByte(DIGEST_RESP);
            writeHeader(out, m);
            out.writeInt(m.rangeDigests().size());
            for (final Map.Entry<Integer, HashedBytes> entry : m.rangeDigests().entrySet()) {
                out.writeInt(entry.getKey());
                out.writeBytes(entry.getValue().toBuffer());
            }
        } else if (message instanceof PeerMessage.KeysReq) {
            final PeerMessage.KeysReq m = (PeerMessage.KeysReq) message;
            out.writeByte(KEYS_REQ);
            writeHeader(out, m);
            out.writeInt(m.range());
            out.writeNullableBytes(m.startAfter());
            out.writeInt(m.limit());
        } else if (message instanceof PeerMessage.KeysResp) {
            final PeerMessage.KeysResp m = (PeerMessage.KeysResp) message;
            out.writeByte(KEYS_RESP);
            writeHeader(out, m);
            out.writeInt(m.range());
            out.writeInt(m.digests().size());
            final List<KeyDigest> digests = m.digests();
            for (int i = 0; i < digests.size(); i++) {
                final KeyDigest digest = digests.get(i);
                out.writeBytes(digest.key().toBuffer());
                out.writeBallot(digest.accepted());
                out.writeBytes(digest.valueHash().toBuffer());
                out.writeBoolean(digest.tombstone());
            }
            out.writeBoolean(m.hasMore());
        } else if (message instanceof PeerMessage.CeilingReq) {
            out.writeByte(CEILING_REQ);
            writeHeader(out, message);
        } else if (message instanceof PeerMessage.CeilingResp) {
            final PeerMessage.CeilingResp m = (PeerMessage.CeilingResp) message;
            out.writeByte(CEILING_RESP);
            writeHeader(out, m);
            out.writeLong(m.promisedUpTo());
            out.writeLong(m.reservedProposerBallot());
            out.writeByte(m.evidence().code());
        } else if (message instanceof PeerMessage.PurgeCheckReq) {
            final PeerMessage.PurgeCheckReq m = (PeerMessage.PurgeCheckReq) message;
            out.writeByte(PURGE_CHECK);
            writeHeader(out, m);
            out.writeBytes(m.key().toBuffer());
            out.writeBallot(m.tombstoneBallot());
        } else if (message instanceof PeerMessage.PurgeCheckResp) {
            final PeerMessage.PurgeCheckResp m = (PeerMessage.PurgeCheckResp) message;
            out.writeByte(PURGE_CHECK_RESP);
            writeHeader(out, m);
            out.writeByte(m.answer().code());
        } else if (message instanceof PeerMessage.PurgeReq) {
            final PeerMessage.PurgeReq m = (PeerMessage.PurgeReq) message;
            out.writeByte(PURGE);
            writeHeader(out, m);
            out.writeBytes(m.key().toBuffer());
            out.writeBallot(m.tombstoneBallot());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported peer message type: " + message.getClass());
        }
        return out.toByteBuffer();
    }

    public static PeerMessage decode(final ByteBuffer payload) {
        final MessageBufferReader in = new MessageBufferReader(payload);
        final byte type = in.readByte();
        final PeerMessage message;
        switch (type) {
            case PREPARE_REQ:
                message = new PeerMessage.PrepareReq(
                        NodeId.of(in.readString()), in.readLong(), new HashedBytes(in.readBytes()), in.readBallot());
                break;
            case PREPARE_RESP:
                message = new PeerMessage.PrepareResp(
                        NodeId.of(in.readString()), in.readLong(), in.readBoolean(), in.readBallot(),
                        in.readBallot(), HashedBytes.copyOf(in.readNullableBytes()), in.readBoolean());
                break;
            case ACCEPT_REQ:
                message = new PeerMessage.AcceptReq(
                        NodeId.of(in.readString()), in.readLong(), new HashedBytes(in.readBytes()),
                        in.readBallot(), HashedBytes.copyOf(in.readNullableBytes()), in.readBoolean());
                break;
            case ACCEPT_RESP:
                message = new PeerMessage.AcceptResp(
                        NodeId.of(in.readString()), in.readLong(), in.readBoolean(), in.readBallot());
                break;
            case DIGEST_REQ:
                message = new PeerMessage.DigestReq(NodeId.of(in.readString()), in.readLong());
                break;
            case DIGEST_RESP: {
                final NodeId senderId = NodeId.of(in.readString());
                final long correlationId = in.readLong();
                // Each entry is an int range number plus a length-prefixed digest.
                final int size = in.readCount(Integer.BYTES + Integer.BYTES);
                final Map<Integer, HashedBytes> digests = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    digests.put(in.readInt(), new HashedBytes(in.readBytes()));
                }
                message = new PeerMessage.DigestResp(senderId, correlationId, digests);
                break;
            }
            case KEYS_REQ:
                message = new PeerMessage.KeysReq(NodeId.of(in.readString()), in.readLong(),
                        in.readInt(), in.readNullableBytes(), in.readInt());
                break;
            case KEYS_RESP: {
                final NodeId senderId = NodeId.of(in.readString());
                final long correlationId = in.readLong();
                final int range = in.readInt();
                // Each entry is a length-prefixed key, a ballot (counter + length-prefixed
                // node id), a length-prefixed value hash, and the tombstone flag.
                final int count = in.readCount(
                        Integer.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES + 1);
                final List<KeyDigest> digests = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    digests.add(new KeyDigest(
                            new HashedBytes(in.readBytes()), in.readBallot(),
                            new HashedBytes(in.readBytes()), in.readBoolean()));
                }
                message = new PeerMessage.KeysResp(senderId, correlationId, range, digests,
                        in.readBoolean());
                break;
            }
            case CEILING_REQ:
                message = new PeerMessage.CeilingReq(NodeId.of(in.readString()), in.readLong());
                break;
            case CEILING_RESP:
                message = new PeerMessage.CeilingResp(NodeId.of(in.readString()), in.readLong(),
                        in.readLong(), in.readLong(), CeilingEvidence.fromCode(in.readByte()));
                break;
            case PURGE_CHECK:
                message = new PeerMessage.PurgeCheckReq(NodeId.of(in.readString()), in.readLong(),
                        new HashedBytes(in.readBytes()), in.readBallot());
                break;
            case PURGE_CHECK_RESP:
                message = new PeerMessage.PurgeCheckResp(NodeId.of(in.readString()), in.readLong(),
                        PurgeAnswer.fromCode(in.readByte()));
                break;
            case PURGE:
                message = new PeerMessage.PurgeReq(NodeId.of(in.readString()), in.readLong(),
                        new HashedBytes(in.readBytes()), in.readBallot());
                break;
            default:
                throw new IllegalArgumentException("Unknown peer message type: " + type);
        }
        if (in.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after peer message decode");
        }
        return message;
    }

    private static void writeHeader(final MessageBufferWriter out, final PeerMessage m) {
        out.writeString(m.senderId().value());
        out.writeLong(m.correlationId());
    }
}
