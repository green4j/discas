/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every shape a data directory can be in when a node opens it, and whether the node may believe the
 * ceiling it finds there.
 *
 * <p>Three axes, because these are the three things that can differ about a directory that is not
 * simply corrupt:
 * <ul>
 *   <li><b>snapshot</b> -- none, or one committed;</li>
 *   <li><b>compacted</b> -- whether the log still starts at its first segment, or whether the
 *       segments a snapshot captured have been reclaimed;</li>
 *   <li><b>removed</b> -- nothing, the log, or the snapshots. A partial restore and an operator
 *       deleting the half that looked broken arrive here.</li>
 * </ul>
 *
 * <p>The answer in every row is one question: <b>can this storage account for its own history back
 * to the beginning?</b> It can when the log runs unbroken from the first segment, or from a snapshot
 * that explains where it starts. Anything else -- and it does not matter which half is missing -- is
 * a node that must get its floor from the cluster instead of trusting what it replayed.
 *
 * <p>Two rows in here were bugs before this matrix existed: a log whose snapshot was deleted from
 * under it (believed a ceiling that had gone backwards), and a snapshot whose log was deleted
 * (believed the footer's ceiling, which is only as new as the snapshot). Both passed the tests of
 * the time, which is the argument for enumerating a space rather than sampling it.
 */
@DisplayName("Recovery -- every shape a data directory can arrive in")
class StorageRecoveryMatrixTest {

    /** Small enough that a few hundred records roll several segments, so compaction has work. */
    private static final int SEGMENT_BYTES = 4096;
    private static final long CEILING = 5000L;
    private static final int VALUES = 300;

    @ParameterizedTest(name = "snapshot={0} compacted={1} removed={2} -> provable={3}")
    @CsvSource({
        // A log that starts where it always did needs nothing to explain it.
        "false, false, NOTHING, true",
        "true,  false, NOTHING, true",
        // Compaction moved the log's start, and the snapshot is what accounts for the difference.
        "true,  true,  NOTHING, true",
        // ... so removing that snapshot leaves a start nothing explains, even though the log is fine.
        "true,  true,  SNAPSHOTS, false",
        // Removing snapshots is harmless while the log still starts at the beginning: nothing was
        // ever reclaimed on their account.
        "true,  false, SNAPSHOTS, true",
        // The log carries every ceiling raise, so without it there is nothing to prove one with --
        // whatever the snapshot footer says, it is only as new as the snapshot.
        "true,  true,  LOG, false",
        "true,  false, LOG, false",
        "false, false, LOG, false",
        // Nothing left at all: the same answer, reached the shortest way.
        "false, false, EVERYTHING, false",
        "true,  true,  EVERYTHING, false",
    })
    void everyDirectoryShape(final boolean snapshot, final boolean compacted,
                             final Removed removed, final boolean provable,
                             @TempDir final Path dir) throws Exception {
        build(dir, snapshot, compacted);
        remove(dir, removed);

        final FileWal wal = new FileWal(config(dir));
        wal.initialize();
        final Wal.SnapshotReader reader = wal.openSnapshot();
        if (reader != null) {
            reader.close();
        }
        wal.replayTail(entry -> { });

        assertEquals(provable, wal.ceilingIsProven(),
                "snapshot=" + snapshot + " compacted=" + compacted + " removed=" + removed);
        wal.close();
    }

    @ParameterizedTest(name = "compacted={0} -> the replayed ceiling is {1}")
    @CsvSource({
        // The other half of the same question: when the storage can account for itself, what it
        // replays must be the ceiling it actually reserved -- a floor that came back lower would be
        // the amnesia the whole mechanism exists to prevent, arrived at without losing a single file.
        "false, 5000",
        "true,  5000",
    })
    void aProvableDirectoryReplaysTheCeilingItReserved(final boolean compacted,
                                                       final long expectedCeiling,
                                                       @TempDir final Path dir) throws Exception {
        build(dir, true, compacted);

        final FileWal wal = new FileWal(config(dir));
        wal.initialize();
        final Wal.SnapshotReader reader = wal.openSnapshot();
        final long fromSnapshot = reader == null ? 0L : reader.reservations().promiseCeiling();
        if (reader != null) {
            reader.close();
        }
        final long[] fromLog = {0L};
        wal.replayTail(entry -> {
            if (entry instanceof Wal.Entry.PromiseCeiling) {
                fromLog[0] = Math.max(fromLog[0], ((Wal.Entry.PromiseCeiling) entry).promisedUpTo());
            }
        });

        assertTrue(wal.ceilingIsProven(), "Precondition: this shape is provable");
        assertEquals(expectedCeiling, Math.max(fromSnapshot, fromLog[0]),
                "The ceiling must come back whole, from the log or the footer that stands in for it");
        wal.close();
    }

    /** Which half of the directory was taken away before the node opened it. */
    enum Removed {
        NOTHING,
        /** {@code wal/} -- a torn log deleted, or a restore that missed it. */
        LOG,
        /** {@code snap/} -- the tempting half to clear, since it looks like a cache. */
        SNAPSHOTS,
        EVERYTHING
    }

    private static StorageConfig config(final Path dir) {
        return StorageConfig.builder()
                .baseDirectory(dir)
                .walMaxFileBytes(SEGMENT_BYTES)
                .build();
    }

    /**
     * Leave a directory holding a reserved ceiling and {@link #VALUES} committed values, optionally
     * snapshotted, optionally with the segments that snapshot captured reclaimed.
     */
    private static void build(final Path dir, final boolean snapshot, final boolean compacted)
            throws IOException {
        final FileWal wal = new FileWal(config(dir));
        wal.initialize();
        wal.replayTail(entry -> { });

        wal.append(new Wal.Entry.PromiseCeiling(CEILING));
        wal.force();
        for (int i = 0; i < VALUES; i++) {
            wal.append(new Wal.Entry.BallotBump(i + 1));
        }
        wal.force();

        if (snapshot) {
            final Wal.SnapshotWriter writer = wal.beginSnapshot();
            writer.commit(new Wal.Reservations(VALUES, CEILING));
            if (compacted) {
                wal.truncateBeforeSnapshot();
            }
        }
        wal.close();

        if (compacted) {
            assertTrue(lowestSegmentSequence(dir) > 1,
                    "Precondition: compaction must actually have reclaimed a segment, or this row"
                            + " is not testing what it says");
        }
    }

    private static long lowestSegmentSequence(final Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir.resolve("wal"))) {
            return files.map(path -> WalFileName.parse(
                            StorageFormat.LAYOUT_VERSION, path.getFileName().toString()))
                    .filter(name -> name != null
                            && (name.type() == WalFileName.Type.SEALED
                                || name.type() == WalFileName.Type.OPEN))
                    .mapToLong(WalFileName::sequence)
                    .min()
                    .orElse(Long.MAX_VALUE);
        }
    }

    private static void remove(final Path dir, final Removed removed) throws IOException {
        switch (removed) {
            case NOTHING:
                return;
            case LOG:
                deleteRecursively(dir.resolve("wal"));
                return;
            case SNAPSHOTS:
                deleteRecursively(dir.resolve("snap"));
                return;
            case EVERYTHING:
                deleteRecursively(dir.resolve("wal"));
                deleteRecursively(dir.resolve("snap"));
                return;
            default:
                throw new IllegalArgumentException(removed.name());
        }
    }

    private static void deleteRecursively(final Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(entry -> {
                try {
                    Files.delete(entry);
                } catch (final IOException ignored) {
                    // a directory that will not go is reported by the assertion that follows
                }
            });
        }
        assertFalse(Files.exists(path), "The half under test must actually be gone: " + path);
    }

}
