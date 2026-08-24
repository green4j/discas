/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages snapshot retention and WAL compaction
 *
 * <ul>
 *   <li>Snapshot retention: keep latest N, GC older ones</li>
 *   <li>WAL compaction: delete sealed WAL files whose lastLsn &lt;= snapshotLsn</li>
 *   <li>Safe GC: rename to .gc then delete</li>
 * </ul>
 */
final class CompactionManager {

    private final StorageConfig config;
    private long latestSnapshotLsn = -1;

    CompactionManager(final StorageConfig config) {
        this.config = config;
    }

    void updateSnapshotLsn(final long lsn) {
        if (lsn > latestSnapshotLsn) {
            latestSnapshotLsn = lsn;
        }
    }

    void runSnapshotRetention() throws IOException {
        final Path snapshotDirectory = config.snapshotDirectory();
        if (!Files.exists(snapshotDirectory)) {
            return;
        }

        final List<SnapshotFileName> snapshots = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(snapshotDirectory)) {
            for (final Path entry : entries) {
                final SnapshotFileName parsed = SnapshotFileName.parse(
                        StorageFormat.LAYOUT_VERSION, entry.getFileName().toString());
                if (parsed != null && parsed.type() == SnapshotFileName.Type.FINAL) {
                    snapshots.add(parsed);
                }
            }
        }

        snapshots.sort(SnapshotFileName.BY_LSN_DESCENDING);

        final int retention = config.snapshotRetentionCount();
        for (int i = retention; i < snapshots.size(); i++) {
            final SnapshotFileName target = snapshots.get(i);
            final Path originalPath = target.toPath(snapshotDirectory);
            final Path gcPath = SnapshotFileName.gc(StorageFormat.LAYOUT_VERSION, target.lsn())
                    .toPath(snapshotDirectory);
            safeDelete(originalPath, gcPath);
        }
    }

    long runWalCompaction() throws IOException {
        if (latestSnapshotLsn < 0) {
            return -1;
        }

        final Path walDirectory = config.walDirectory();
        if (!Files.exists(walDirectory)) {
            return latestSnapshotLsn;
        }

        try (DirectoryStream<Path> entries = Files.newDirectoryStream(walDirectory)) {
            for (final Path entry : entries) {
                final WalFileName parsed = WalFileName.parse(
                        StorageFormat.LAYOUT_VERSION, entry.getFileName().toString());
                if (parsed != null
                        && parsed.type() == WalFileName.Type.SEALED
                        && parsed.lastLsn() <= latestSnapshotLsn) {
                    final WalFileName gcName = WalFileName.gc(
                            StorageFormat.LAYOUT_VERSION, parsed.sequence(),
                            parsed.firstLsn(), parsed.lastLsn());
                    safeDelete(entry, gcName.toPath(walDirectory));
                }
            }
        }

        return latestSnapshotLsn;
    }

    void deleteGcFiles() throws IOException {
        deleteGcFilesIn(config.walDirectory());
        deleteGcFilesIn(config.snapshotDirectory());
    }

    private void deleteGcFilesIn(final Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        boolean anyDeleted = false;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, "*.gc")) {
            for (final Path entry : entries) {
                if (Files.deleteIfExists(entry)) {
                    anyDeleted = true;
                }
            }
        }
        if (anyDeleted) {
            Fsync.dir(directory);
        }
    }

    private static void safeDelete(final Path original, final Path gcPath) throws IOException {
        final Path directory = original.getParent();
        boolean changed = false;
        if (Files.exists(original)) {
            Files.move(original, gcPath);
            changed = true;
        }
        if (Files.deleteIfExists(gcPath)) {
            changed = true;
        }
        if (changed && directory != null) {
            Fsync.dir(directory);
        }
    }
}
