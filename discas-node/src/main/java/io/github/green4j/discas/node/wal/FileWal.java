/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.identity.IncarnationId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * A {@link Wal} over segment files and snapshots on disk.
 * <p>
 * All I/O runs synchronously on the caller's thread, which is the node's event loop. Writes are
 * buffered by the OS and reach the platter on {@link #force()}; between syncs, durability rests on
 * the quorum.
 */
public final class FileWal implements Wal {
    private final StorageConfig config;
    private final CompactionManager compaction;

    private IncarnationId incarnation;
    /**
     * Continuity of the log, established by the replay that reads it. {@code replayExpectedLsn} is
     * the LSN the next record must carry; {@link Continuity} latches what the replay found.
     */
    private long replayExpectedLsn = -1;
    private Continuity continuity = Continuity.NOT_REPLAYED;

    /**
     * What the replay of this log established, in the order the states occur.
     * <p>
     * One field rather than a set of flags: only four of their combinations are reachable -- a gap
     * can only be found inside a segment, so "a hole, but no segment" is a state a reader would
     * have to prove impossible every time.
     */
    private enum Continuity {
        /** {@code replayTail} has not run. Nothing below has been established yet. */
        NOT_REPLAYED,
        /** Replay ran and found no sealed segment: this storage has recorded nothing. */
        NOTHING_RECORDED,
        /** Records run unbroken from the snapshot (or from the beginning) to the end. */
        CONTINUOUS,
        /** A record was missing where the chain said one must be; latched for the rest of replay. */
        BROKEN
    }

    private WalWriter walWriter;
    private long currentLsn;
    private long snapshotLsn = -1;
    private long pendingSnapshotLsn = -1;
    private static final long FIRST_WAL_SEQUENCE = 1;

    private long nextWalSequence = FIRST_WAL_SEQUENCE;
    private boolean initialized;
    private volatile boolean degraded;
    private volatile String degradedReason;

    public FileWal(final StorageConfig config) {
        this.config = config;
        this.compaction = new CompactionManager(config);
    }

    /**
     * Initialize the directory layout. Must be called before any other method.
     * <p>
     * Absence of state is <b>reported, not refused</b>. An empty data directory is either a node
     * that has never run or one whose state was deleted, nothing on disk tells them apart, and
     * nothing needs to: a node that starts without state recovers its promise floor from the cluster
     * before it serves anything (see {@link #ceilingIsProven()}). Requiring an operator to declare
     * the emptiness expected would carry no information about promises and could be checked against
     * nothing.
     * <p>
     * What still fails here is state that is <em>present and contradictory</em>: an unreadable
     * marker, or a WAL that will not replay. Those are evidence in conflict with itself, which is a
     * different class from evidence that is absent.
     */
    public void initialize() throws IOException {
        Files.createDirectories(config.baseDirectory());

        // The marker, not the presence of the directories, is what says this storage has run
        // before: a wiped node is usually handed back the same (now empty) mount, and the
        // directories are recreated the moment anything touches them.
        final boolean initializedBefore = IncarnationMarker.exists(config.baseDirectory());

        final boolean fresh = !Files.exists(config.walDirectory());
        Files.createDirectories(config.walDirectory());
        Files.createDirectories(config.snapshotDirectory());
        if (fresh) {
            Fsync.dir(config.baseDirectory());
        }
        if (!initializedBefore) {
            // Written last, so a crash midway through creating the layout leaves the directory
            // still looking uninitialized rather than looking like a node that has run.
            incarnation = IncarnationMarker.create(config.baseDirectory());
        } else {
            incarnation = IncarnationMarker.read(config.baseDirectory());
        }

        initialized = true;
    }

    /** The identity of this run of the storage. Available once {@link #initialize()} has returned. */
    @Override
    public IncarnationId incarnation() {
        ensureInitialized();
        return incarnation;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Answered by the replay itself, from the LSNs it reads: a log with at least one segment, whose
     * records run unbroken from the snapshot (or from the beginning) to the end, is one that
     * contains every ceiling raise this storage ever made -- because a raise is forced before it is
     * believed, so an intact log cannot be missing one. Anything else, including a storage that has
     * recorded nothing, cannot prove a ceiling and says so.
     */
    @Override
    public boolean ceilingIsProven() {
        ensureInitialized();
        if (continuity == Continuity.NOT_REPLAYED) {
            // A guard rather than a safe default, because a safe default here is a silent one: the
            // answer would be "cannot prove it", the node would go and ask the cluster, and the
            // missing replay would never surface.
            throw new IllegalStateException(
                    "ceilingIsProven() asked before replayTail(); continuity is established by the"
                            + " replay that reads the log");
        }
        return continuity == Continuity.CONTINUOUS;
    }

    @Override
    public boolean append(final Entry entry) {
        ensureInitialized();
        if (degraded) {
            return false;
        }
        try {
            final int recordType = EntryCodec.recordType(entry);
            final ByteBuffer payload = EntryCodec.encode(entry);
            currentLsn++;
            walWriter.append(currentLsn, recordType, payload);
            return true;
        } catch (final IOException e) {
            markDegraded("WAL append failed: " + e.getMessage());
            return false;
        }
    }

    @Override
    public SnapshotReader openSnapshot() {
        ensureInitialized();
        try {
            cleanupTempAndGcFiles(config.snapshotDirectory());

            final Path snapshotDirectory = config.snapshotDirectory();
            if (!Files.exists(snapshotDirectory)) {
                return null;
            }

            final List<SnapshotFileName> candidates = new ArrayList<>();
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(snapshotDirectory)) {
                for (final Path entry : entries) {
                    final SnapshotFileName parsed = SnapshotFileName.parse(
                            StorageFormat.LAYOUT_VERSION, entry.getFileName().toString());
                    if (parsed != null && parsed.type() == SnapshotFileName.Type.FINAL) {
                        candidates.add(parsed);
                    }
                }
            }

            if (candidates.isEmpty()) {
                return null;
            }

            candidates.sort(SnapshotFileName.BY_LSN_DESCENDING);

            for (final SnapshotFileName candidate : candidates) {
                final Path filePath = candidate.toPath(snapshotDirectory);
                final FileSnapshotReader reader = FileSnapshotReader.open(filePath);
                if (reader != null) {
                    snapshotLsn = reader.snapshotLsn();
                    currentLsn = Math.max(currentLsn, snapshotLsn);
                    compaction.updateSnapshotLsn(snapshotLsn);
                    return reader;
                }
            }

            return null;
        } catch (final IOException e) {
            throw new SnapshotException(SnapshotException.Fault.OPEN_FAILED,
                    "Failed to open snapshot", e);
        }
    }

    @Override
    public void replayTail(final Consumer<Entry> consumer) {
        ensureInitialized();
        // A replay that does not finish establishes nothing: the field is left where it started, so
        // ceilingIsProven() still refuses to answer rather than answering from a partial read.
        boolean completed = false;
        try {
            final Path walDirectory = config.walDirectory();

            cleanupTempAndGcFiles(walDirectory);
            normalizeOpenWalFiles(walDirectory);

            if (!Files.exists(walDirectory)) {
                continuity = Continuity.NOTHING_RECORDED;
                initializeWalWriter();
                completed = true;
                return;
            }

            final List<WalFileName> sealedFiles = new ArrayList<>();
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(walDirectory)) {
                for (final Path entry : entries) {
                    final WalFileName parsed = WalFileName.parse(
                            StorageFormat.LAYOUT_VERSION, entry.getFileName().toString());
                    if (parsed != null && parsed.type() == WalFileName.Type.SEALED) {
                        sealedFiles.add(parsed);
                    }
                }
            }

            Collections.sort(sealedFiles, WalFileName.BY_SEQUENCE);
            continuity = sealedFiles.isEmpty()
                    ? Continuity.NOTHING_RECORDED
                    : Continuity.CONTINUOUS;   // until trackContinuity finds otherwise

            for (final WalFileName walFile : sealedFiles) {
                replayOneWalFile(walDirectory, walFile, consumer);
                nextWalSequence = Math.max(nextWalSequence, walFile.sequence() + 1);
            }

            initializeWalWriter();
            completed = true;
        } catch (final IOException e) {
            throw new WalException(WalException.Fault.REPLAY_FAILED, "WAL replay failed", e);
        } finally {
            if (!completed) {
                continuity = Continuity.NOT_REPLAYED;
            }
        }
    }

    @Override
    public SnapshotWriter beginSnapshot() {
        ensureInitialized();
        try {
            // Record the LSN handed to the writer. This is the fuzzy-snapshot
            // horizon: every entry with lsn <= pendingSnapshotLsn is captured
            // by the snapshot, so once it commits, sealed WAL files up to that
            // LSN can be reclaimed by truncateBeforeSnapshot().
            pendingSnapshotLsn = currentLsn;
            return new FileSnapshotWriter(config, currentLsn);
        } catch (final IOException e) {
            throw new SnapshotException(SnapshotException.Fault.BEGIN_FAILED,
                    "Failed to begin snapshot", e);
        }
    }

    @Override
    public void truncateBeforeSnapshot() {
        ensureInitialized();
        try {
            // Advance the compaction horizon to the snapshot just committed.
            // Without this, latestSnapshotLsn only ever reflects the snapshot
            // read at startup (openSnapshot), so a long-running node -- and any
            // node that started with no snapshot -- never reclaims sealed WAL
            // files, growing the WAL without bound.
            if (pendingSnapshotLsn >= 0) {
                compaction.updateSnapshotLsn(pendingSnapshotLsn);
            }
            compaction.runSnapshotRetention();
            compaction.runWalCompaction();
            compaction.deleteGcFiles();
        } catch (final IOException e) {
            throw new WalException(WalException.Fault.TRUNCATION_FAILED,
                    "Truncation/compaction failed", e);
        }
    }

    /**
     * Flush the active WAL file to disk (fdatasync)
     * Intended to be called periodically by an EventLoop timer
     */
    public void force() {
        if (walWriter == null || degraded) {
            return;
        }
        try {
            walWriter.force();
        } catch (final IOException e) {
            markDegraded("WAL force failed: " + e.getMessage());
        }
    }

    /**
     * Bytes of the active segment that an {@code fsync} has returned for. Everything past it is what
     * a power cut takes, and it is maintained by the sync itself: a crash test that asked the caller
     * instead would happily pass a log that never synced.
     */
    public long forcedBytes() {
        ensureInitialized();
        return walWriter == null ? 0L : walWriter.forcedBytes();
    }

    @Override
    public boolean isDegraded() {
        return degraded;
    }

    @Override
    public String degradedReason() {
        return degradedReason;
    }

    private void markDegraded(final String reason) {
        if (!degraded) {
            degraded = true;
            degradedReason = reason;
            // Deliberately silent: LocalStore reports this once through observer.walDegraded(),
            // and StderrNodeObserver prints it. Printing here too both duplicated the message and
            // defeated the seam, since NodeObserver.NONE could not opt out of it.
        }
    }

    /**
     * Seal the active WAL file and close. Called on shutdown.
     */
    public void close() {
        if (walWriter == null) {
            return;
        }
        try {
            walWriter.roll(); // seals the active file and closes its channel
            walWriter = null;
        } catch (final IOException e) {
            throw new WalException(WalException.Fault.CLOSE_FAILED, "WAL close failed", e);
        }
    }

    public long currentLsn() {
        return currentLsn;
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("FileWal.initialize() must be called first");
        }
    }

    private void initializeWalWriter() {
        this.walWriter = new WalWriter(config, nextWalSequence);
    }

    private void cleanupTempAndGcFiles(final Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        boolean anyDeleted = false;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (final Path entry : entries) {
                final String name = entry.getFileName().toString();
                if (name.endsWith(".tmp") || name.endsWith(".gc")) {
                    if (Files.deleteIfExists(entry)) {
                        anyDeleted = true;
                    }
                }
            }
        }
        if (anyDeleted) {
            Fsync.dir(directory);
        }
    }

    private void normalizeOpenWalFiles(final Path walDirectory) throws IOException {
        if (!Files.exists(walDirectory)) {
            return;
        }

        final List<WalFileName> openFiles = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(walDirectory)) {
            for (final Path entry : entries) {
                final WalFileName parsed = WalFileName.parse(
                        StorageFormat.LAYOUT_VERSION, entry.getFileName().toString());
                if (parsed != null && parsed.type() == WalFileName.Type.OPEN) {
                    openFiles.add(parsed);
                }
            }
        }

        for (final WalFileName openFile : openFiles) {
            normalizeOneOpenWalFile(walDirectory, openFile);
        }
    }

    private void normalizeOneOpenWalFile(final Path walDirectory,
                                         final WalFileName openFile) throws IOException {
        final Path openPath = openFile.toPath(walDirectory);
        final long fileSize = Files.size(openPath);

        if (fileSize < StorageFormat.WAL_HEADER_BYTES) {
            Files.delete(openPath);
            Fsync.dir(walDirectory);
            return;
        }

        final ByteBuffer fileBuffer = readFileToBuffer(openPath);

        fileBuffer.position(0);
        final WalFileHeader header = WalFileHeader.decode(fileBuffer);
        if (header == null) {
            Files.delete(openPath);
            Fsync.dir(walDirectory);
            return;
        }
        checkFormatVersion(header.formatVersion(), openPath);

        fileBuffer.position(StorageFormat.WAL_HEADER_BYTES);
        long firstLsn = -1;
        long lastLsn = -1;
        int validEndPosition = StorageFormat.WAL_HEADER_BYTES;

        while (fileBuffer.position() < fileBuffer.limit()) {
            final WalRecordCodec.OffsetRecord record =
                    WalRecordCodec.decodeToOffsets(fileBuffer);
            if (record == null) {
                break;
            }
            if (firstLsn < 0) {
                firstLsn = record.lsn();
            }
            lastLsn = record.lsn();
            validEndPosition = fileBuffer.position();
        }

        if (firstLsn >= 0) {
            if (validEndPosition < fileSize) {
                try (FileChannel channel = FileChannel.open(openPath,
                        StandardOpenOption.WRITE)) {
                    channel.truncate(validEndPosition);
                    channel.force(false);
                }
            }

            final WalFileName sealedName = WalFileName.sealed(
                    StorageFormat.LAYOUT_VERSION, openFile.sequence(), firstLsn, lastLsn);
            Files.move(openPath, sealedName.toPath(walDirectory));
            Fsync.dir(walDirectory);

            nextWalSequence = Math.max(nextWalSequence, openFile.sequence() + 1);
        } else {
            Files.delete(openPath);
            Fsync.dir(walDirectory);
        }
    }

    /**
     * One record's worth of the continuity check, run on every record replay reads -- including the
     * ones the snapshot already covers, because it is their LSNs that show whether the log reaches
     * back to it.
     * <p>
     * The first record has to start where the evidence before it ends: at LSN 1 when there is no
     * snapshot, or anywhere at or before {@code snapshotLsn + 1} when there is one. After that each
     * record must be its predecessor's successor. A hole anywhere means records existed that this
     * replay never saw, and a ceiling derived from what is left may be short.
     */
    private void trackContinuity(final long lsn) {
        final boolean gap = replayExpectedLsn < 0
                ? lsn > Math.max(1L, snapshotLsn + 1)
                : lsn != replayExpectedLsn;
        if (gap) {
            continuity = Continuity.BROKEN;   // latched: nothing below sets it back
        }
        replayExpectedLsn = lsn + 1;
    }

    private void replayOneWalFile(final Path walDirectory,
                                  final WalFileName walFile,
                                  final Consumer<Entry> consumer) throws IOException {
        final Path filePath = walFile.toPath(walDirectory);
        final ByteBuffer fileBuffer = readFileToBuffer(filePath);

        if (fileBuffer.remaining() < StorageFormat.WAL_HEADER_BYTES) {
            return;
        }

        fileBuffer.position(0);
        final WalFileHeader header = WalFileHeader.decode(fileBuffer);
        if (header == null) {
            return;
        }
        checkFormatVersion(header.formatVersion(), filePath);

        fileBuffer.position(StorageFormat.WAL_HEADER_BYTES);

        while (fileBuffer.position() < fileBuffer.limit()) {
            final WalRecordCodec.OffsetRecord record =
                    WalRecordCodec.decodeToOffsets(fileBuffer);
            if (record == null) {
                break;
            }

            trackContinuity(record.lsn());

            if (record.lsn() > snapshotLsn) {
                final int savedPosition = fileBuffer.position();
                final int savedLimit = fileBuffer.limit();
                fileBuffer.position(record.payloadOffset());
                fileBuffer.limit(record.payloadOffset() + record.payloadLength());

                final Entry entry = EntryCodec.decode(record.recordType(), fileBuffer);
                consumer.accept(entry);

                fileBuffer.limit(savedLimit);
                fileBuffer.position(savedPosition);
            }

            currentLsn = Math.max(currentLsn, record.lsn());
        }
    }

    private static void checkFormatVersion(final int actual, final Path filePath) {
        if (actual != StorageFormat.FORMAT_VERSION) {
            throw new IllegalStateException("Incompatible WAL format version " + actual
                    + ", expected " + StorageFormat.FORMAT_VERSION + ": " + filePath);
        }
    }

    private static ByteBuffer readFileToBuffer(final Path filePath) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            final int size = (int) channel.size();
            final ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.order(ByteOrder.BIG_ENDIAN);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) {
                    break;
                }
            }
            buffer.flip();
            return buffer;
        }
    }
}
