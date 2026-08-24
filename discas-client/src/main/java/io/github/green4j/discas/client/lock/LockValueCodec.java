/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.lock;

import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.transport.MessageBufferReader;
import io.github.green4j.discas.common.transport.MessageBufferWriter;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Binary codec for client-side lock values stored in CASPaxos keys.
 *
 * <p>Layout: magic + version + ownerLen + owner + tokenLen + token + acquiredMs + leaseUntilMs
 * + generation. {@code generation} monotonically increases on every acquire of the same key
 * and serves as the fencing token.
 */
public final class LockValueCodec {
    /** Leading sentinel that tells a lock record apart from any other value on a shared key. */
    public static final int MAGIC = 0x4C4F434B; // "LOCK"
    /** Layout version; a record written under a different one decodes to {@code null}. */
    public static final short VERSION = 1;

    private static final ByteBuffer EMPTY_TOKEN =
            ByteBuffer.allocate(0).asReadOnlyBuffer();

    private LockValueCodec() {
    }

    /** Encodes {@code record} into a fresh buffer ready to be stored as a KV value. */
    public static ByteBuffer encode(final LockRecord record) {
        final MessageBufferWriter out = new MessageBufferWriter(64, KvLimits.MAX_VALUE_BYTES);
        out.writeInt(MAGIC);
        out.writeShort(VERSION);
        out.writeString(record.ownerId());
        out.writeBytes(record.token());
        out.writeLong(record.acquiredAtEpochMs());
        out.writeLong(record.leaseUntilEpochMs());
        out.writeLong(record.generation());
        return out.toByteBuffer();
    }

    /**
     * Decodes a lock record, or {@code null} if {@code in} is not one.
     * <p>
     * {@code null} rather than an exception because a lock key holding something else is an
     * ordinary outcome, not a fault: keys are shared with the plain KV surface, so a value that is
     * not a lock record is exactly what {@code LockInfoStatus.NOT_LOCK_RECORD} reports. Truncated
     * and corrupt input take the same path -- the reader raises on any length that overruns the
     * buffer, and there is nothing more useful to say than "not a lock record".
     */
    public static LockRecord decode(final ByteBuffer in) {
        final MessageBufferReader reader = new MessageBufferReader(in.duplicate());
        try {
            if (reader.readInt() != MAGIC) {
                return null;
            }
            final short version = reader.readShort();
            if (version != VERSION) {
                return null;
            }
            final String ownerId = reader.readString();
            // Zero-copy: LockRecord copies it, and `in` is a decoded response value the
            // caller owns, not a transport frame that will be recycled underneath us.
            final ByteBuffer token = reader.readBytes();
            final long acquiredAt = reader.readLong();
            final long leaseUntil = reader.readLong();
            final long generation = reader.readLong();
            return new LockRecord(ownerId, token, acquiredAt, leaseUntil, generation);
        } catch (final IllegalArgumentException | BufferUnderflowException malformed) {
            // Exactly the two ways the reader reports input it cannot parse. Catching every
            // RuntimeException here also swallowed genuine defects -- an NPE or an assertion
            // failure inside the decode would have surfaced as "not a lock record".
            return null;
        }
    }

    /**
     * One decoded lock value. The token is copied on construction and only ever handed out as a
     * read-only duplicate, so a record cannot be altered after it has been decoded.
     */
    public static final class LockRecord {
        private final String ownerId;
        private final ByteBuffer token;
        private final long acquiredAtEpochMs;
        private final long leaseUntilEpochMs;
        private final long generation;

        public LockRecord(final String ownerId,
                          final ByteBuffer token,
                          final long acquiredAtEpochMs,
                          final long leaseUntilEpochMs,
                          final long generation) {
            this.ownerId = ownerId;
            final ByteBuffer src = token.duplicate();
            final ByteBuffer copy = ByteBuffer.allocate(src.remaining());
            if (copy.capacity() > 0) {
                copy.put(src);
                copy.flip();
            }
            this.token = copy.asReadOnlyBuffer();
            this.acquiredAtEpochMs = acquiredAtEpochMs;
            this.leaseUntilEpochMs = leaseUntilEpochMs;
            this.generation = generation;
        }

        /** The holder's client id; empty on a released marker. */
        public String ownerId() {
            return ownerId;
        }

        /** A read-only duplicate of the holder token; empty on a released marker. */
        public ByteBuffer token() {
            return token.duplicate();
        }

        /** Acquire time in epoch millis, on the cluster's clock as the acquirer read it. */
        public long acquiredAtEpochMs() {
            return acquiredAtEpochMs;
        }

        /** Lease end in epoch millis, on the cluster's clock; {@code 0} when released. */
        public long leaseUntilEpochMs() {
            return leaseUntilEpochMs;
        }

        /** The per-key fencing generation, bumped on every acquire and carried across releases. */
        public long generation() {
            return generation;
        }

        /**
         * A "released" marker keeps the per-key generation alive across
         * release/re-acquire cycles so fencing tokens stay strictly monotonic
         * cluster-wide. {@link io.github.green4j.discas.client.DisCasClient#release} writes one
         * of these instead of deleting the record outright.
         */
        public boolean isReleased() {
            return leaseUntilEpochMs == 0L && ownerId.isEmpty() && token.remaining() == 0;
        }

        /** The marker written on release: no owner, no token, no lease -- only the generation. */
        public static LockRecord releasedMarker(final long generation) {
            return new LockRecord("", EMPTY_TOKEN, 0L, 0L, generation);
        }
    }
}
