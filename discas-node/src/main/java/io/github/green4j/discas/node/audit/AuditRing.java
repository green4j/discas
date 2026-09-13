/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.audit;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * The binary circular buffer between the event loop and the audit drain: one producer, one
 * consumer, a fixed heap {@code byte[]}, no allocation per record. A record is copied in and
 * published with one volatile store; with no room for it the default is to lose the record rather
 * than the latency (see {@link AuditOverflow}).
 * <p>
 * A record that would straddle the end of the array is preceded by a padding record filling the
 * remainder, so every record is contiguous and the consumer decodes in place. Lengths are rounded
 * up to eight bytes, which is what keeps padding large enough for its own header.
 */
public final class AuditRing {

    private static final int ALIGNMENT = 8;

    /** {@code int32} length written by this class ahead of the encoded record. */
    static final int LENGTH_BYTES = Integer.BYTES;

    /** Consumed, never handed out. Not an {@link AuditEventKind} code. */
    private static final byte KIND_PADDING = 0;

    private final byte[] buffer;
    private final int capacity;
    private final int mask;

    // Published across the two threads: the producer's store releases the bytes it wrote, the
    // consumer's read acquires them.
    private final AtomicLong producerPosition = new AtomicLong();
    private final AtomicLong consumerPosition = new AtomicLong();
    private final AtomicLong droppedRecords = new AtomicLong();

    // Each side's own copy of the position it owns, plus the producer's last reading of the
    // consumer's. Plain fields: only the owning thread touches them, so the volatile reads are
    // spent on what the other thread moved, and only when the ring looks full.
    private long producerCursor;
    private long consumerCursor;
    private long consumerPositionCache;

    /**
     * @param capacityBytes a power of two: the position-to-offset step is a mask. Charged to the
     *                      node's heap data budget, so the store is sized with it taken out.
     */
    public AuditRing(final int capacityBytes) {
        if (capacityBytes < 1024 || Integer.bitCount(capacityBytes) != 1) {
            throw new IllegalArgumentException("Audit buffer must be a power of two of at least "
                    + "1024 bytes, got " + capacityBytes);
        }
        this.buffer = new byte[capacityBytes];
        this.capacity = capacityBytes;
        this.mask = capacityBytes - 1;
    }

    public int capacity() {
        return capacity;
    }

    /** Records lost to a full buffer since the node started. */
    public long droppedRecords() {
        return droppedRecords.get();
    }

    /** @return false when the buffer had no room and the record was dropped and counted */
    public boolean offer(final byte[] record, final int length) {
        if (write(record, length)) {
            return true;
        }
        droppedRecords.incrementAndGet();
        return false;
    }

    /**
     * As {@link #offer}, waiting for the drain instead of dropping -- the caller is the event loop,
     * so this stalls the node. A record larger than the buffer is dropped rather than waited on.
     *
     * @return false only for such a record
     */
    public boolean offerOrWait(final byte[] record, final int length) {
        if (required(length) > capacity) {
            droppedRecords.incrementAndGet();
            return false;
        }
        while (!write(record, length)) {
            LockSupport.parkNanos(1_000L);
        }
        return true;
    }

    /**
     * Hands records published since the last call to {@code handler}, up to {@code maxRecords}.
     * The handler reads out of the ring itself, and the space is released when this returns.
     *
     * @return how many records were handed over
     */
    public int drain(final RecordHandler handler, final int maxRecords) {
        final long producer = producerPosition.get();
        long consumer = consumerCursor;
        int handled = 0;
        while (consumer < producer && handled < maxRecords) {
            final int offset = (int) (consumer & mask);
            final int aligned = readInt(offset);
            consumer += aligned;
            if (buffer[offset + LENGTH_BYTES] != KIND_PADDING) {
                handler.onRecord(buffer, offset + LENGTH_BYTES, aligned - LENGTH_BYTES);
                handled++;
            }
        }
        consumerCursor = consumer;
        consumerPosition.set(consumer);
        return handled;
    }

    /** True while the consumer has nothing to read. */
    public boolean isEmpty() {
        return consumerPosition.get() >= producerPosition.get();
    }

    /** One record, in place. */
    @FunctionalInterface
    public interface RecordHandler {
        void onRecord(byte[] buffer, int offset, int length);
    }

    private boolean write(final byte[] record, final int length) {
        final int aligned = required(length);
        if (aligned > capacity) {
            return false;
        }
        final long producer = producerCursor;
        int offset = (int) (producer & mask);
        int padding = 0;
        if (offset + aligned > capacity) {
            padding = capacity - offset;
        }
        final long needed = (long) aligned + padding;
        if (capacity - (producer - consumerPositionCache) < needed) {
            // Only now is it worth reading what the drain has freed since the last look.
            consumerPositionCache = consumerPosition.get();
            if (capacity - (producer - consumerPositionCache) < needed) {
                return false;
            }
        }
        if (padding > 0) {
            writeInt(offset, padding);
            buffer[offset + LENGTH_BYTES] = KIND_PADDING;
            offset = 0;
        }
        writeInt(offset, aligned);
        System.arraycopy(record, 0, buffer, offset + LENGTH_BYTES, length);
        // Publishes every byte written above: the consumer's read of this position is what
        // orders it against them.
        producerCursor = producer + needed;
        producerPosition.set(producerCursor);
        return true;
    }

    private static int required(final int length) {
        final int total = LENGTH_BYTES + length;
        return (total + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    }

    private int readInt(final int offset) {
        return ((buffer[offset] & 0xff) << 24)
                | ((buffer[offset + 1] & 0xff) << 16)
                | ((buffer[offset + 2] & 0xff) << 8)
                | (buffer[offset + 3] & 0xff);
    }

    private void writeInt(final int offset, final int value) {
        buffer[offset] = (byte) (value >>> 24);
        buffer[offset + 1] = (byte) (value >>> 16);
        buffer[offset + 2] = (byte) (value >>> 8);
        buffer[offset + 3] = (byte) value;
    }
}
