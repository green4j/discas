/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.zip.CRC32;

/**
 * Writes a single snapshot file through the write/commit/abort lifecycle
 *
 * <p>File layout:
 * <pre>
 *   [SnapshotFileHeader - 64 bytes]
 *   [long totalEntries]
 *   [long reservedProposerBallot]
 *   [long promiseCeiling]
 *   [long snapshotLsn]
 *   [encoded entry 1]...[encoded entry N]
 * </pre>
 *
 * <p>Metadata (totalEntries, the two reservations, snapshotLsn) is written as
 * placeholder zeros initially and rewritten on commit with final values
 * The payloadCrc in the header covers everything after the header.</p>
 */
final class FileSnapshotWriter implements Wal.SnapshotWriter {
    private final StorageConfig config;
    private final long snapshotLsn;
    private final Path tmpFilePath;
    private final ByteBuffer headerBuffer;
    private final ByteBuffer metaBuffer;
    private final ByteBuffer entryWriteBuffer;

    private FileChannel channel;
    private long entryCount;
    private boolean committed;
    private boolean aborted;

    FileSnapshotWriter(final StorageConfig config, final long snapshotLsn) throws IOException {
        this.config = config;
        this.snapshotLsn = snapshotLsn;
        this.headerBuffer = ByteBuffer.allocate(StorageFormat.SNAPSHOT_HEADER_BYTES);
        this.headerBuffer.order(ByteOrder.BIG_ENDIAN);
        this.metaBuffer = ByteBuffer.allocate(StorageFormat.SNAPSHOT_META_BYTES);
        this.metaBuffer.order(ByteOrder.BIG_ENDIAN);
        this.entryWriteBuffer = ByteBuffer.allocate(256 * 1024);
        this.entryWriteBuffer.order(ByteOrder.BIG_ENDIAN);

        final SnapshotFileName tmpName = SnapshotFileName.tmp(StorageFormat.LAYOUT_VERSION, snapshotLsn);
        this.tmpFilePath = tmpName.toPath(config.snapshotDirectory());

        this.channel = FileChannel.open(tmpFilePath,
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));

        headerBuffer.clear();
        SnapshotFileHeader.encodePlaceholderTo(headerBuffer, StorageFormat.FORMAT_VERSION);
        headerBuffer.flip();
        writeFullyToChannel(headerBuffer, channel);

        metaBuffer.clear();
        metaBuffer.putLong(0L);
        metaBuffer.putLong(0L);
        metaBuffer.putLong(0L);
        metaBuffer.putLong(0L);
        metaBuffer.flip();
        writeFullyToChannel(metaBuffer, channel);
    }

    @Override
    public void write(final Wal.SnapshotEntry entry) {
        if (committed || aborted) {
            throw new IllegalStateException("Snapshot already finalized");
        }
        try {
            final ByteBuffer encoded = EntryCodec.encodeSnapshotEntry(entry);
            final int encodedLen = encoded.remaining();
            final ByteBuffer buf;
            if (encodedLen <= entryWriteBuffer.capacity()) {
                entryWriteBuffer.clear();
                entryWriteBuffer.put(encoded);
                entryWriteBuffer.flip();
                buf = entryWriteBuffer;
            } else {
                buf = encoded;
            }
            writeFullyToChannel(buf, channel);
            entryCount++;
        } catch (final IOException e) {
            throw new SnapshotException(SnapshotException.Fault.WRITE_FAILED, "Snapshot write failed", e);
        }
    }

    @Override
    public void commit(final Wal.Reservations reservations) {
        if (committed) {
            return;
        }
        if (aborted) {
            throw new IllegalStateException("Snapshot was aborted");
        }
        try {
            metaBuffer.clear();
            metaBuffer.putLong(entryCount);
            metaBuffer.putLong(reservations.proposerBallot());
            metaBuffer.putLong(reservations.promiseCeiling());
            metaBuffer.putLong(snapshotLsn);
            metaBuffer.flip();

            // The payload CRC can only be computed now, and only by reading the file back: it
            // covers [meta][entries], and the meta's real values are not known until this point,
            // while the entries were written long before. CRC32 has no combine operation, so a
            // running CRC over the entries cannot be prefixed with the final meta afterwards.
            channel.force(false);
            final long payloadStart = StorageFormat.SNAPSHOT_HEADER_BYTES;
            channel.close();

            // Reopen for read + write to rewrite the meta and checksum the payload.
            channel = FileChannel.open(tmpFilePath,
                    EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE));

            channel.position(payloadStart);
            writeFullyToChannel(metaBuffer, channel);
            channel.force(false);

            // Stream the payload in fixed-size chunks, as FileSnapshotReader does when it
            // verifies. One buffer the size of the whole payload would spike the heap on the
            // consensus thread and, with the size narrowed to int, silently compute a wrong length
            // once a snapshot exceeds 2 GiB.
            channel.position(payloadStart);
            final CRC32 payloadCrc = new CRC32();
            // entryWriteBuffer is idle by commit time, so reuse it rather than allocating again.
            final ByteBuffer chunk = entryWriteBuffer;
            while (true) {
                chunk.clear();
                if (channel.read(chunk) < 0) {
                    break;
                }
                chunk.flip();
                payloadCrc.update(chunk); // bulk update, not a byte at a time
            }

            // Rewrite header with correct CRC
            final SnapshotFileHeader finalHeader = new SnapshotFileHeader(
                    StorageFormat.FORMAT_VERSION, (int) payloadCrc.getValue());
            headerBuffer.clear();
            finalHeader.encode(headerBuffer);
            headerBuffer.flip();
            channel.position(0);
            writeFullyToChannel(headerBuffer, channel);

            channel.force(false);
            channel.close();
            channel = null;

            // Rename .tmp -> .snap, then fsync the parent directory so the
            // dirent itself is durable; otherwise on POSIX filesystems the
            // rename can be lost across a crash even though the data file
            // was forced.
            final SnapshotFileName finalName =
                    SnapshotFileName.finalSnapshot(StorageFormat.LAYOUT_VERSION, snapshotLsn);
            final Path finalPath = finalName.toPath(config.snapshotDirectory());
            Files.move(tmpFilePath, finalPath);
            Fsync.dir(config.snapshotDirectory());

            committed = true;
        } catch (final IOException e) {
            throw new SnapshotException(SnapshotException.Fault.COMMIT_FAILED, "Snapshot commit failed", e);
        }
    }

    @Override
    public void abort() {
        if (committed || aborted) {
            return;
        }
        aborted = true;
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (final IOException ignored) {
        }
        try {
            if (tmpFilePath != null && Files.deleteIfExists(tmpFilePath)) {
                Fsync.dir(config.snapshotDirectory());
            }
        } catch (final IOException ignored) {
        }
    }

    public boolean isCommitted() {
        return committed;
    }

    long snapshotLsn() {
        return snapshotLsn;
    }

    private static void writeFullyToChannel(final ByteBuffer source,
                                            final FileChannel channel) throws IOException {
        while (source.hasRemaining()) {
            channel.write(source);
        }
    }
}
