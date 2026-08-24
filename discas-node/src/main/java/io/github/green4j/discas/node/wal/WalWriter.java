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

/**
 * Manages the active WAL file on the EventLoop thread:
 * <ul>
 *   <li>Creates new active files via tmp -&gt; rename</li>
 *   <li>Appends WAL records encoded from byte[] payload</li>
 *   <li>Tracks first/last LSN in the current file</li>
 *   <li>Rolls when the size threshold is exceeded</li>
 *   <li>Seals (rename .open -&gt; .wal)</li>
 * </ul>
 */
public final class WalWriter {
    private final StorageConfig config;
    private final Path walDirectory;
    private ByteBuffer writeBuffer; // grown on demand up to WalRecordCodec.MAX_RECORD_DISK_BYTES
    private final ByteBuffer headerBuffer;

    private long currentSequence;
    private long firstLsnInFile;
    private long lastLsnInFile;
    private FileChannel activeChannel;
    private long activeFileSizeBytes;
    /**
     * Bytes of the active file that an {@code fsync} has actually returned for -- the boundary a
     * power cut would cut at, maintained by the code that performs the sync rather than inferred by
     * anyone watching it.
     * <p>
     * Exists for the crash tests: they model a lost machine by dropping everything past this, and
     * taking the caller's word for where it is would make them pass a WAL that never synced at all.
     */
    private long forcedFileSizeBytes;

    public WalWriter(final StorageConfig config,
                     final long startSequence) {
        this.config = config;
        this.walDirectory = config.walDirectory();
        this.currentSequence = startSequence;
        this.firstLsnInFile = -1;
        this.lastLsnInFile = -1;
        // Start modest (a hint from the roll threshold) but never larger than a single
        // max record; append() grows it on demand up to WalRecordCodec.MAX_RECORD_DISK_BYTES,
        // so buffer capacity is decoupled from walMaxFileBytes (which is only a roll threshold).
        this.writeBuffer = ByteBuffer.allocate(Math.min(
                WalRecordCodec.MAX_RECORD_DISK_BYTES,
                Math.max(config.walMaxFileBytes() / 4, 256 * 1024)));
        this.writeBuffer.order(ByteOrder.BIG_ENDIAN);
        this.headerBuffer = ByteBuffer.allocate(StorageFormat.WAL_HEADER_BYTES);
        this.headerBuffer.order(ByteOrder.BIG_ENDIAN);
    }

    private void ensureActive() throws IOException {
        if (activeChannel != null && activeChannel.isOpen()) {
            return;
        }
        openNewActiveFile();
    }

    public void append(final long lsn,
                       final int recordType,
                       final ByteBuffer payload) throws IOException {
        ensureActive();

        final int payloadLength = (payload != null) ? payload.remaining() : 0;
        final int diskBytes = WalRecordCodec.diskSize(payloadLength);
        if (diskBytes > WalRecordCodec.MAX_RECORD_DISK_BYTES) {
            // Exceeds the derived key+ballot+value ceiling -- node-side ingress enforcement
            // (KvLimits) should have rejected this upstream, so reaching here is a bug.
            throw new IOException("WAL record exceeds maximum record size: " + diskBytes
                    + " > " + WalRecordCodec.MAX_RECORD_DISK_BYTES);
        }
        if (writeBuffer.capacity() < diskBytes) {
            writeBuffer = ByteBuffer.allocate(diskBytes).order(ByteOrder.BIG_ENDIAN);
        }
        writeBuffer.clear();
        WalRecordCodec.encode(writeBuffer, lsn, recordType, payload);
        writeBuffer.flip();
        writeFullyToChannel(writeBuffer, activeChannel);
        activeFileSizeBytes += diskBytes;

        if (firstLsnInFile < 0) {
            firstLsnInFile = lsn;
        }
        lastLsnInFile = lsn;

        if (activeFileSizeBytes >= config.walMaxFileBytes()) {
            roll();
        }
    }

    /** Seal the current active file and prepare the next sequence. */
    public void roll() throws IOException {
        if (activeChannel == null) {
            return;
        }

        activeChannel.force(false);
        activeChannel.close();
        activeChannel = null;

        final Path openPath = WalFileName.open(StorageFormat.LAYOUT_VERSION, currentSequence)
                .toPath(walDirectory);

        if (firstLsnInFile >= 0) {
            final WalFileName sealedName = WalFileName.sealed(
                    StorageFormat.LAYOUT_VERSION, currentSequence, firstLsnInFile, lastLsnInFile);
            final Path sealedPath = sealedName.toPath(walDirectory);
            Files.move(openPath, sealedPath);
            Fsync.dir(walDirectory);
        } else {
            Files.deleteIfExists(openPath);
            Fsync.dir(walDirectory);
        }

        currentSequence++;
        firstLsnInFile = -1;
        lastLsnInFile = -1;
        activeFileSizeBytes = 0;
        forcedFileSizeBytes = 0;
    }

    public void force() throws IOException {
        if (activeChannel != null && activeChannel.isOpen()) {
            activeChannel.force(false);
            forcedFileSizeBytes = activeFileSizeBytes;
        }
    }

    /** @see #forcedFileSizeBytes */
    long forcedBytes() {
        return forcedFileSizeBytes;
    }

    private void openNewActiveFile() throws IOException {
        final WalFileName tmpName = WalFileName.openTmp(StorageFormat.LAYOUT_VERSION, currentSequence);
        final WalFileName openName = WalFileName.open(StorageFormat.LAYOUT_VERSION, currentSequence);
        final Path tmpPath = tmpName.toPath(walDirectory);
        final Path openPath = openName.toPath(walDirectory);

        try (FileChannel tmpChannel = FileChannel.open(tmpPath,
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            headerBuffer.clear();
            final WalFileHeader header = new WalFileHeader(StorageFormat.FORMAT_VERSION);
            header.encode(headerBuffer);
            headerBuffer.flip();
            writeFullyToChannel(headerBuffer, tmpChannel);
            tmpChannel.force(false);
        }

        Files.move(tmpPath, openPath);
        Fsync.dir(walDirectory);

        activeChannel = FileChannel.open(openPath,
                EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.APPEND));
        activeFileSizeBytes = StorageFormat.WAL_HEADER_BYTES;
        firstLsnInFile = -1;
        lastLsnInFile = -1;
    }

    private static void writeFullyToChannel(final ByteBuffer source,
                                            final FileChannel channel) throws IOException {
        while (source.hasRemaining()) {
            channel.write(source);
        }
    }
}
