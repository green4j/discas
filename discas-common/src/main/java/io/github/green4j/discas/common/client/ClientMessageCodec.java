/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.client;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.identity.ClientId;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary codec for the {@link ClientMessage} subtypes. Type-byte dispatch is internal to this
 * class, and the wire layout is independent of the peer protocol's.
 */
public final class ClientMessageCodec {

    /**
     * Upper bound on an encoded client message, derived from {@link KvLimits}. The largest is
     * a CAS request: type, header (client id and correlation id), key, {@code expected} and
     * {@code desired} -- two values -- each length-prefixed. A transport's reassembly budget must
     * accommodate a message this large.
     */
    public static final int MAX_MESSAGE_BYTES =
            1                                                        // type byte
                    + Integer.BYTES + KvLimits.MAX_CLIENT_ID_BYTES   // putString(senderId)
                    + Long.BYTES                                     // correlationId
                    + Integer.BYTES + KvLimits.MAX_KEY_BYTES         // putBytes(key)
                    + Integer.BYTES + KvLimits.MAX_VALUE_BYTES       // putNullableBytes(expected)
                    + Integer.BYTES + KvLimits.MAX_VALUE_BYTES;      // putNullableBytes(desired)

    private static final byte CLIENT_GET_REQ = 1;
    private static final byte CLIENT_PUT_REQ = 2;
    private static final byte CLIENT_CAS_REQ = 3;
    private static final byte CLIENT_DELETE_REQ = 4;
    private static final byte CLIENT_SCAN_REQ = 5;
    private static final byte CLIENT_GET_RESP = 6;
    private static final byte CLIENT_PUT_RESP = 7;
    private static final byte CLIENT_CAS_RESP = 8;
    private static final byte CLIENT_DELETE_RESP = 9;
    private static final byte CLIENT_SCAN_RESP = 10;

    private ClientMessageCodec() {
    }

    public static ByteBuffer encode(final ClientMessage message) {
        final MessageBufferWriter out = new MessageBufferWriter(256, MAX_MESSAGE_BYTES);
        if (message instanceof ClientMessage.ClientGetReq) {
            final ClientMessage.ClientGetReq m = (ClientMessage.ClientGetReq) message;
            out.writeByte(CLIENT_GET_REQ);
            writeHeader(out, m);
            out.writeBytes(m.key());
            out.writeByte(m.consistency().code());
        } else if (message instanceof ClientMessage.ClientPutReq) {
            final ClientMessage.ClientPutReq m = (ClientMessage.ClientPutReq) message;
            out.writeByte(CLIENT_PUT_REQ);
            writeHeader(out, m);
            out.writeBytes(m.key());
            out.writeBytes(m.value());
        } else if (message instanceof ClientMessage.ClientCasReq) {
            final ClientMessage.ClientCasReq m =
                    (ClientMessage.ClientCasReq) message;
            out.writeByte(CLIENT_CAS_REQ);
            writeHeader(out, m);
            out.writeBytes(m.key());
            out.writeBallot(m.expectedVersion());
            out.writeNullableBytes(m.desired());
        } else if (message instanceof ClientMessage.ClientCasResp) {
            final ClientMessage.ClientCasResp m =
                    (ClientMessage.ClientCasResp) message;
            out.writeByte(CLIENT_CAS_RESP);
            writeHeader(out, m);
            out.writeBoolean(m.ok());
            out.writeBoolean(m.swapped());
            out.writeNullableBytes(m.value());
            out.writeBallot(m.version());
            out.writeNullableString(m.error());
            out.writeByte(m.errorCode().code());
        } else if (message instanceof ClientMessage.ClientDeleteReq) {
            final ClientMessage.ClientDeleteReq m = (ClientMessage.ClientDeleteReq) message;
            out.writeByte(CLIENT_DELETE_REQ);
            writeHeader(out, m);
            out.writeBytes(m.key());
        } else if (message instanceof ClientMessage.ClientScanReq) {
            final ClientMessage.ClientScanReq m = (ClientMessage.ClientScanReq) message;
            out.writeByte(CLIENT_SCAN_REQ);
            writeHeader(out, m);
            out.writeBytes(m.prefix());
            out.writeNullableBytes(m.startAfter());
            out.writeInt(m.limit());
        } else if (message instanceof ClientMessage.ClientGetResp) {
            final ClientMessage.ClientGetResp m = (ClientMessage.ClientGetResp) message;
            out.writeByte(CLIENT_GET_RESP);
            writeHeader(out, m);
            out.writeBoolean(m.ok());
            out.writeNullableBytes(m.value());
            out.writeNullableString(m.error());
            out.writeByte(m.errorCode().code());
            out.writeBallot(m.version());
        } else if (message instanceof ClientMessage.ClientPutResp) {
            final ClientMessage.ClientPutResp m = (ClientMessage.ClientPutResp) message;
            out.writeByte(CLIENT_PUT_RESP);
            writeHeader(out, m);
            out.writeBoolean(m.ok());
            out.writeNullableString(m.error());
            out.writeByte(m.errorCode().code());
            out.writeBallot(m.version());
        } else if (message instanceof ClientMessage.ClientDeleteResp) {
            final ClientMessage.ClientDeleteResp m = (ClientMessage.ClientDeleteResp) message;
            out.writeByte(CLIENT_DELETE_RESP);
            writeHeader(out, m);
            out.writeBoolean(m.ok());
            out.writeNullableString(m.error());
            out.writeByte(m.errorCode().code());
            out.writeBallot(m.version());
        } else if (message instanceof ClientMessage.ClientScanResp) {
            final ClientMessage.ClientScanResp m = (ClientMessage.ClientScanResp) message;
            out.writeByte(CLIENT_SCAN_RESP);
            writeHeader(out, m);
            out.writeInt(m.entries().size());
            final List<ClientMessage.ScanEntry> entrys = m.entries();
            for (int i = 0; i < entrys.size(); i++) {
                final ClientMessage.ScanEntry entry = entrys.get(i);
                out.writeBytes(entry.key());
                out.writeBallot(entry.version());
                out.writeBoolean(entry.tombstone());
            }
            out.writeBoolean(m.hasMore());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported client message type: " + message.getClass());
        }
        return out.toByteBuffer();
    }

    /**
     * The decoder copies every field out of {@code payload}: the frame is a transport buffer
     * that is recycled the moment dispatch returns, while a decoded message outlives it -- a
     * node's request survives a Paxos round, and a client's response is handed to a future the
     * caller resolves whenever it likes. This is the "producer copies" half of the ownership
     * rule on {@link ClientMessage}.
     */
    private static ByteBuffer copy(final MessageBufferReader in) {
        return ByteBuffers.copyReadOnly(in.readBytes());
    }

    private static ByteBuffer copyNullable(final MessageBufferReader in) {
        return ByteBuffers.copyReadOnly(in.readNullableBytes());
    }

    public static ClientMessage decode(final ByteBuffer payload) {
        final MessageBufferReader in = new MessageBufferReader(payload);
        final byte type = in.readByte();
        final ClientMessage message;
        switch (type) {
            case CLIENT_GET_REQ: {
                final String getSender = readClientSender(in);
                final long getCorrelation = in.readLong();
                final ByteBuffer getKey = copy(in);
                final ReadConsistency consistency = ReadConsistency.fromCode(in.readByte());
                message = new ClientMessage.ClientGetReq(
                        getSender, getCorrelation, getKey, consistency);
                break;
            }
            case CLIENT_PUT_REQ:
                message = new ClientMessage.ClientPutReq(
                        readClientSender(in), in.readLong(), copy(in), copy(in));
                break;
            case CLIENT_CAS_REQ: {
                final String casSender = readClientSender(in);
                final long casCorrelation = in.readLong();
                final ByteBuffer casKey = copy(in);
                final Ballot casExpected = in.readBallot();
                message = new ClientMessage.ClientCasReq(
                        casSender, casCorrelation, casKey, casExpected, copyNullable(in));
                break;
            }
            case CLIENT_CAS_RESP: {
                final String casSender = readNodeSender(in);
                final long casCorrelation = in.readLong();
                final boolean casOk = in.readBoolean();
                final boolean casSwapped = in.readBoolean();
                final ByteBuffer casValue = copyNullable(in);
                final Ballot casVersion = in.readBallot();
                final String casError = in.readNullableString();
                message = new ClientMessage.ClientCasResp(
                        casSender, casCorrelation, casOk, casSwapped, casValue, casVersion,
                        casError, ClientErrorCode.fromCode(in.readByte()));
                break;
            }
            case CLIENT_DELETE_REQ:
                message = new ClientMessage.ClientDeleteReq(
                        readClientSender(in), in.readLong(), copy(in));
                break;
            case CLIENT_SCAN_REQ:
                message = new ClientMessage.ClientScanReq(readClientSender(in), in.readLong(),
                        copy(in), copyNullable(in), in.readInt());
                break;
            case CLIENT_GET_RESP: {
                final String getSender = readNodeSender(in);
                final long getCorrelation = in.readLong();
                final boolean getOk = in.readBoolean();
                final ByteBuffer getValue = copyNullable(in);
                final String getError = in.readNullableString();
                final ClientErrorCode getCode = ClientErrorCode.fromCode(in.readByte());
                final Ballot version = in.readBallot();
                message = new ClientMessage.ClientGetResp(
                        getSender, getCorrelation, getOk, getValue, getError, getCode, version);
                break;
            }
            case CLIENT_PUT_RESP:
                message = new ClientMessage.ClientPutResp(
                        readNodeSender(in), in.readLong(), in.readBoolean(),
                        in.readNullableString(), ClientErrorCode.fromCode(in.readByte()),
                        in.readBallot());
                break;
            case CLIENT_DELETE_RESP:
                message = new ClientMessage.ClientDeleteResp(
                        readNodeSender(in), in.readLong(), in.readBoolean(),
                        in.readNullableString(), ClientErrorCode.fromCode(in.readByte()),
                        in.readBallot());
                break;
            case CLIENT_SCAN_RESP: {
                final String senderId = readNodeSender(in);
                final long correlationId = in.readLong();
                // Each entry is a length-prefixed key, a ballot (counter + length-prefixed
                // node id), and the tombstone flag.
                final int count = in.readCount(Integer.BYTES + Long.BYTES + Integer.BYTES + 1);
                final List<ClientMessage.ScanEntry> entries = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    entries.add(new ClientMessage.ScanEntry(
                            copy(in), in.readBallot(), in.readBoolean()));
                }
                message = new ClientMessage.ClientScanResp(
                        senderId, correlationId, entries, in.readBoolean());
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown client message type: " + type);
        }
        if (in.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after client message decode");
        }
        return message;
    }

    /**
     * Read a request's {@code senderId} and bound its length, so an unbounded identity cannot
     * reach the node from the wire. Left as a {@code String} because that is what
     * {@link ClientMessage} carries.
     */
    private static String readClientSender(final MessageBufferReader in) {
        return ClientId.of(in.readString()).value();
    }

    /** Read a response's {@code senderId} -- the answering node's id -- and bound it. */
    private static String readNodeSender(final MessageBufferReader in) {
        return NodeId.of(in.readString()).value();
    }

    private static void writeHeader(final MessageBufferWriter out, final ClientMessage m) {
        out.writeString(m.senderId());
        out.writeLong(m.correlationId());
    }
}
