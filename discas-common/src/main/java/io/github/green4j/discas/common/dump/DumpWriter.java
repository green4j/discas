/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.common.dump;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.zip.CRC32;

/**
 * Writes a dump one pair at a time to a channel the caller owns -- a file for a backup, an HTTP
 * response body for the agent's endpoint. Nothing is assembled in memory: one fixed staging
 * buffer, a CRC and a counter, whatever the size of the cluster.
 *
 * <p><b>An unfinished dump is not a dump.</b> {@link #commit()} alone writes the entry count and
 * the CRC, so a dump abandoned halfway never gets a trailer and {@link DumpReader} refuses it
 * outright rather than reading it short.
 *
 * <p>There is no close: the channel belongs to the caller, who opens it in a try-with-resources
 * and commits inside. Not thread-safe.
 */
public final class DumpWriter {

    /** Big enough that entries batch up rather than reaching the channel one at a time. */
    private static final int STAGING_BYTES = 256 * 1024;

    private final WritableByteChannel channel;
    private final ByteBuffer staging;
    private final CRC32 crc = new CRC32();

    private long entries;
    private long bytes;
    private boolean committed;

    /** Writes the magic, the format version and {@code header} straight away. */
    public DumpWriter(final WritableByteChannel channel, final DumpHeader header) throws IOException {
        this.channel = channel;
        this.staging = ByteBuffer.allocate(STAGING_BYTES).order(ByteOrder.BIG_ENDIAN);

        final byte[] headerBlock = DumpCodec.encodeHeader(header);
        put(ByteBuffer.wrap(DumpCodec.MAGIC));
        putInt(DumpCodec.FORMAT_VERSION);
        putInt(headerBlock.length);
        put(ByteBuffer.wrap(headerBlock));
    }

    /**
     * Appends one live pair. Neither buffer's position is disturbed, so a caller may hand the same
     * key buffer to the writer and to something else.
     */
    public void writeEntry(final ByteBuffer key, final ByteBuffer value) throws IOException {
        if (committed) {
            throw new IllegalStateException("Dump is already committed");
        }
        if (key == null || value == null) {
            // A dump carries live pairs and nothing else -- a tombstone is not one of them, and a
            // null value here would encode as an entry that restores as a key holding nothing.
            throw new IllegalArgumentException("A dump entry needs both a key and a value");
        }
        final ByteBuffer keyView = key.duplicate();
        final ByteBuffer valueView = value.duplicate();
        DumpCodec.checkKeyLength(keyView.remaining());
        DumpCodec.checkValueLength(valueView.remaining());

        putInt(keyView.remaining());
        put(keyView);
        putInt(valueView.remaining());
        put(valueView);
        entries++;
    }

    /** Pairs written so far. */
    public long entryCount() {
        return entries;
    }

    /**
     * Bytes handed to the channel so far, framing included -- what a caller showing progress has
     * to show, since a dump's size is not knowable before its last pair.
     * <p>
     * Counted as bytes reach the channel rather than as entries are staged, so it never claims
     * progress the channel has not accepted.
     */
    public long bytesWritten() {
        return bytes;
    }

    /**
     * Writes the end-of-entries sentinel and the trailer, and flushes everything staged. The file
     * is a dump from this point and was not one before it. Idempotent, so a caller may commit
     * explicitly and still commit from a {@code finally}.
     */
    public void commit() throws IOException {
        if (committed) {
            return;
        }
        putInt(DumpCodec.END_OF_ENTRIES);
        putLong(entries);
        flush();

        // The checksum covers every byte above it and not itself, so it goes to the channel
        // without passing through the accumulator.
        staging.clear();
        staging.putInt((int) crc.getValue());
        staging.flip();
        writeFullyToChannel(staging, channel);
        staging.clear();
        committed = true;
    }

    private void putInt(final int value) throws IOException {
        ensureStaging(Integer.BYTES);
        staging.putInt(value);
    }

    private void putLong(final long value) throws IOException {
        ensureStaging(Long.BYTES);
        staging.putLong(value);
    }

    private void put(final ByteBuffer source) throws IOException {
        if (source.remaining() > staging.capacity()) {
            // Larger than the staging buffer: write it straight through rather than in slices, as
            // FileSnapshotWriter does with an oversized entry. Order matters for the CRC, so the
            // staged bytes have to reach the channel first.
            flush();
            crc.update(source.duplicate());
            writeFullyToChannel(source, channel);
            return;
        }
        ensureStaging(source.remaining());
        staging.put(source);
    }

    private void ensureStaging(final int required) throws IOException {
        if (staging.remaining() < required) {
            flush();
        }
    }

    private void flush() throws IOException {
        if (staging.position() == 0) {
            return;
        }
        staging.flip();
        crc.update(staging.duplicate()); // bulk update, not a byte at a time
        writeFullyToChannel(staging, channel);
        staging.clear();
    }

    private void writeFullyToChannel(final ByteBuffer source,
                                     final WritableByteChannel channel) throws IOException {
        while (source.hasRemaining()) {
            bytes += channel.write(source);
        }
    }
}
