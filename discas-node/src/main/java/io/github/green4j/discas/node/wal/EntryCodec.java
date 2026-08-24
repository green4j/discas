/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;
import io.github.green4j.discas.node.HashedBytes;

import java.nio.ByteBuffer;

/**
 * Converts between {@link Wal.Entry} / {@link Wal.SnapshotEntry} and binary.
 * <p>
 * Ballots are encoded with a length-prefixed UTF-8 node id (see
 * {@link MessageBufferWriter#writeBallot}); the on-disk layout is versioned via
 * {@code WalFileHeader}/{@code SnapshotFileHeader}.
 * <p>
 * Record types:
 * 1 = Promise
 * 2 = Accept
 * 3 = BallotBump
 * 4 = PromiseCeiling
 * 5 = Purge
 */
public final class EntryCodec {

    public static final int TYPE_PROMISE = 1;
    public static final int TYPE_ACCEPT = 2;
    public static final int TYPE_BALLOT_BUMP = 3;
    public static final int TYPE_PROMISE_CEILING = 4;
    public static final int TYPE_PURGE = 5;

    private EntryCodec() {
    }

    public static int recordType(final Wal.Entry entry) {
        if (entry instanceof Wal.Entry.Promise) {
            return TYPE_PROMISE;
        }
        if (entry instanceof Wal.Entry.Accept) {
            return TYPE_ACCEPT;
        }
        if (entry instanceof Wal.Entry.BallotBump) {
            return TYPE_BALLOT_BUMP;
        }
        if (entry instanceof Wal.Entry.PromiseCeiling) {
            return TYPE_PROMISE_CEILING;
        }
        if (entry instanceof Wal.Entry.Purge) {
            return TYPE_PURGE;
        }
        throw new IllegalArgumentException("Unknown entry type: " + entry.getClass());
    }

    public static ByteBuffer encode(final Wal.Entry entry) {
        if (entry instanceof Wal.Entry.Promise) {
            final Wal.Entry.Promise promise = (Wal.Entry.Promise) entry;
            return encodeKeyBallot(promise.key(), promise.ballot());
        }
        if (entry instanceof Wal.Entry.Purge) {
            final Wal.Entry.Purge purge = (Wal.Entry.Purge) entry;
            return encodeKeyBallot(purge.key(), purge.ballot());
        }
        if (entry instanceof Wal.Entry.Accept) {
            return encodeAccept((Wal.Entry.Accept) entry);
        }
        if (entry instanceof Wal.Entry.BallotBump) {
            return encodeBallotBump((Wal.Entry.BallotBump) entry);
        }
        if (entry instanceof Wal.Entry.PromiseCeiling) {
            return encodeCounter(((Wal.Entry.PromiseCeiling) entry).promisedUpTo());
        }
        throw new IllegalArgumentException("Unknown entry type: " + entry.getClass());
    }

    public static Wal.Entry decode(final int recordType, final ByteBuffer payload) {
        final MessageBufferReader in = new MessageBufferReader(payload);
        if (recordType == TYPE_PROMISE) {
            final HashedBytes key = new HashedBytes(in.readBytes());
            return new Wal.Entry.Promise(key, in.readBallot());
        }
        if (recordType == TYPE_ACCEPT) {
            final HashedBytes key = new HashedBytes(in.readBytes());
            final Ballot ballot = in.readBallot();
            final HashedBytes value = HashedBytes.copyOf(in.readNullableBytes());
            final boolean tombstone = in.readBoolean();
            return new Wal.Entry.Accept(key, ballot, value, tombstone);
        }
        if (recordType == TYPE_BALLOT_BUMP) {
            return new Wal.Entry.BallotBump(in.readLong());
        }
        if (recordType == TYPE_PROMISE_CEILING) {
            return new Wal.Entry.PromiseCeiling(in.readLong());
        }
        if (recordType == TYPE_PURGE) {
            final HashedBytes key = new HashedBytes(in.readBytes());
            return new Wal.Entry.Purge(key, in.readBallot());
        }
        throw new IllegalArgumentException("Unknown record type: " + recordType);
    }

    /** Both key-scoped records without a value are a key and a ballot, so they share one encoder. */
    private static ByteBuffer encodeKeyBallot(final HashedBytes key, final Ballot ballot) {
        final MessageBufferWriter out = new MessageBufferWriter(64, WalRecordCodec.MAX_RECORD_DISK_BYTES);
        out.writeBytes(key.toBuffer());
        out.writeBallot(ballot);
        return out.toByteBuffer();
    }

    private static ByteBuffer encodeAccept(final Wal.Entry.Accept entry) {
        final MessageBufferWriter out = new MessageBufferWriter(64, WalRecordCodec.MAX_RECORD_DISK_BYTES);
        out.writeBytes(entry.key().toBuffer());
        out.writeBallot(entry.ballot());
        out.writeNullableBytes(HashedBytes.toBuffer(entry.value()));
        out.writeBoolean(entry.tombstone());
        return out.toByteBuffer();
    }

    private static ByteBuffer encodeBallotBump(final Wal.Entry.BallotBump entry) {
        return encodeCounter(entry.reservedUpTo());
    }

    /** Both reservation records are a bare counter, so they share one encoder. */
    private static ByteBuffer encodeCounter(final long counter) {
        final MessageBufferWriter out = new MessageBufferWriter(16, WalRecordCodec.MAX_RECORD_DISK_BYTES);
        out.writeLong(counter);
        return out.toByteBuffer();
    }

    public static ByteBuffer encodeSnapshotEntry(final Wal.SnapshotEntry entry) {
        final MessageBufferWriter out = new MessageBufferWriter(64, WalRecordCodec.MAX_RECORD_DISK_BYTES);
        out.writeBytes(entry.key().toBuffer());
        out.writeBallot(entry.promised());
        out.writeBallot(entry.accepted());
        out.writeNullableBytes(HashedBytes.toBuffer(entry.value()));
        out.writeBoolean(entry.tombstone());
        return out.toByteBuffer();
    }

    public static Wal.SnapshotEntry decodeSnapshotEntry(final ByteBuffer payload) {
        final MessageBufferReader in = new MessageBufferReader(payload);
        final HashedBytes key = new HashedBytes(in.readBytes());
        final Ballot promised = in.readBallot();
        final Ballot accepted = in.readBallot();
        final HashedBytes value = HashedBytes.copyOf(in.readNullableBytes());
        final boolean tombstone = in.readBoolean();
        return new Wal.SnapshotEntry(key, promised, accepted, value, tombstone);
    }
}
