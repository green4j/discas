/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

/**
 * Reads a committed snapshot file. Validates header and payload CRC
 * on construction by streaming the payload through a fixed buffer
 * (no whole-file materialization, so >2 GB snapshots work). Entries
 * are decoded sequentially via hasNext/next, refilling a small
 * window from the channel on demand.
 */
final class FileSnapshotReader implements Wal.SnapshotReader {

    private static final int INITIAL_WINDOW_BYTES = 64 * 1024;
    private static final int VERIFY_CHUNK_BYTES = 64 * 1024;

    private final long totalEntries;
    private final Wal.Reservations reservations;
    private final long snapshotLsn;
    private final FileChannel channel;
    private final long fileSize;
    private long channelReadOffset;
    private ByteBuffer window;
    private long entriesRead;

    private FileSnapshotReader(final long totalEntries,
                               final Wal.Reservations reservations,
                               final long snapshotLsn,
                               final FileChannel channel,
                               final long fileSize,
                               final long channelReadOffset) {
        this.totalEntries = totalEntries;
        this.reservations = reservations;
        this.snapshotLsn = snapshotLsn;
        this.channel = channel;
        this.fileSize = fileSize;
        this.channelReadOffset = channelReadOffset;
        this.window = ByteBuffer.allocate(INITIAL_WINDOW_BYTES);
        this.window.order(ByteOrder.BIG_ENDIAN);
        this.window.limit(0);
    }

    /**
     * Open and validate a snapshot file. Returns null if the file is
     * corrupt (bad magic, bad header CRC, bad payload CRC, too small)
     */
    public static FileSnapshotReader open(final Path filePath) throws IOException {
        final FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
        boolean handedOff = false;
        try {
            final long fileSize = channel.size();
            if (fileSize < SnapshotFileHeader.HEADER_SIZE) {
                return null;
            }

            final ByteBuffer headerBuf = ByteBuffer.allocate(SnapshotFileHeader.HEADER_SIZE);
            headerBuf.order(ByteOrder.BIG_ENDIAN);
            channel.position(0);
            readFully(channel, headerBuf);
            headerBuf.flip();
            final SnapshotFileHeader header = SnapshotFileHeader.decode(headerBuf);
            if (header == null) {
                return null;
            }
            if (header.formatVersion() != StorageFormat.FORMAT_VERSION) {
                throw new IllegalStateException("Incompatible snapshot format version "
                        + header.formatVersion() + ", expected " + StorageFormat.FORMAT_VERSION
                        + ": " + filePath);
            }

            final long payloadStart = StorageFormat.SNAPSHOT_HEADER_BYTES;
            final long payloadLength = fileSize - payloadStart;
            if (payloadLength < StorageFormat.SNAPSHOT_META_BYTES) {
                return null;
            }

            // Verify the CRC by streaming the payload through a fixed-size buffer, so a snapshot
            // larger than the heap still opens.
            channel.position(payloadStart);
            final CRC32 payloadCrc = new CRC32();
            final ByteBuffer verifyBuf = ByteBuffer.allocate(VERIFY_CHUNK_BYTES);
            long remaining = payloadLength;
            while (remaining > 0) {
                verifyBuf.clear();
                if (verifyBuf.remaining() > remaining) {
                    verifyBuf.limit((int) remaining);
                }
                final int n = channel.read(verifyBuf);
                if (n < 0) {
                    return null;
                }
                verifyBuf.flip();
                payloadCrc.update(verifyBuf);
                remaining -= n;
            }
            if ((int) payloadCrc.getValue() != header.payloadCrc32()) {
                return null;
            }

            channel.position(payloadStart);
            final ByteBuffer metaBuf = ByteBuffer.allocate(StorageFormat.SNAPSHOT_META_BYTES);
            metaBuf.order(ByteOrder.BIG_ENDIAN);
            readFully(channel, metaBuf);
            metaBuf.flip();
            final long totalEntries = metaBuf.getLong();
            final long reservedProposerBallot = metaBuf.getLong();
            final long promiseCeiling = metaBuf.getLong();
            final long snapshotLsn = metaBuf.getLong();

            // Leave the channel positioned at the first entry.
            final long entriesStart = payloadStart + StorageFormat.SNAPSHOT_META_BYTES;
            channel.position(entriesStart);

            final FileSnapshotReader reader = new FileSnapshotReader(
                    totalEntries,
                    new Wal.Reservations(reservedProposerBallot, promiseCeiling), snapshotLsn,
                    channel, fileSize, entriesStart);
            handedOff = true;
            return reader;
        } finally {
            if (!handedOff) {
                channel.close();
            }
        }
    }

    @Override
    public long totalEntries() {
        return totalEntries;
    }

    @Override
    public boolean hasNext() {
        return entriesRead < totalEntries;
    }

    @Override
    public Wal.SnapshotEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            while (true) {
                window.mark();
                try {
                    final Wal.SnapshotEntry entry = EntryCodec.decodeSnapshotEntry(window);
                    entriesRead++;
                    return entry;
                } catch (final BufferUnderflowException underflow) {
                    window.reset();
                    if (!refill()) {
                        throw new IOException(
                                "Snapshot truncated mid-entry at file offset "
                                        + (channelReadOffset - window.remaining()));
                    }
                }
            }
        } catch (final IOException e) {
            throw new SnapshotException(SnapshotException.Fault.READ_FAILED,
                    "Snapshot read failed", e);
        }
    }

    @Override
    public Wal.Reservations reservations() {
        return reservations;
    }

    long snapshotLsn() {
        return snapshotLsn;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (final IOException ignored) {
        }
    }

    private boolean refill() throws IOException {
        // Move unread bytes to the front of the window.
        window.compact();
        if (!window.hasRemaining()) {
            // Current capacity is fully held by unread bytes; grow the window so
            // we can pull more from disk for an oversized entry.
            final int newCapacity = Math.max(
                    window.capacity() * 2,
                    window.position() + INITIAL_WINDOW_BYTES);
            final ByteBuffer bigger = ByteBuffer.allocate(newCapacity);
            bigger.order(ByteOrder.BIG_ENDIAN);
            window.flip();
            bigger.put(window);
            window = bigger;
        }
        boolean progress = false;
        while (channelReadOffset < fileSize && window.hasRemaining()) {
            final int n = channel.read(window);
            if (n < 0) {
                break;
            }
            channelReadOffset += n;
            progress = true;
        }
        window.flip();
        return progress;
    }

    private static void readFully(final FileChannel channel, final ByteBuffer buffer)
            throws IOException {
        while (buffer.hasRemaining()) {
            final int n = channel.read(buffer);
            if (n < 0) {
                throw new IOException("Unexpected EOF while reading snapshot");
            }
        }
    }
}
