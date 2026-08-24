/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("FileWal -- durable WAL with snapshot + compaction")
class FileWalIntegrationTest {

    private static final NodeId N1 = NodeId.of("1");

    @TempDir
    Path tempDir;
    StorageConfig config;

    @BeforeEach
    void setup() {
        config = StorageConfig.builder()
                .baseDirectory(tempDir)
                .walMaxFileBytes(4096)
                .snapshotRetentionCount(2)
                .build();
    }

    @Test
    @DisplayName("Append entries, force, close, reopen and replay (no snapshot)")
    void appendAndReplayWithoutSnapshot() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();

        assertNull(wal.openSnapshot());

        final List<Wal.Entry> replayed = new ArrayList<>();
        wal.replayTail(replayed::add);
        assertTrue(replayed.isEmpty());
        assertEquals(0, wal.currentLsn(), "An empty WAL starts at LSN 0");

        final HashedBytes key = new HashedBytes(new byte[]{1, 2, 3});
        wal.append(new Wal.Entry.Promise(key, new Ballot(1, N1)));
        wal.append(new Wal.Entry.Accept(key, new Ballot(1, N1),
                new HashedBytes(new byte[]{10}), false));
        wal.append(new Wal.Entry.BallotBump(1024));

        wal.force();
        wal.close();

        final FileWal wal2 = new FileWal(config);
        wal2.initialize();
        assertNull(wal2.openSnapshot());

        final List<Wal.Entry> recovered = new ArrayList<>();
        wal2.replayTail(recovered::add);

        assertEquals(3, recovered.size());
        assertInstanceOf(Wal.Entry.Promise.class, recovered.get(0));
        assertInstanceOf(Wal.Entry.Accept.class, recovered.get(1));
        assertInstanceOf(Wal.Entry.BallotBump.class, recovered.get(2));

        final Wal.Entry.Promise promise = (Wal.Entry.Promise) recovered.get(0);
        assertEquals(key, promise.key());
        assertEquals(new Ballot(1, N1), promise.ballot());

        final Wal.Entry.Accept accept = (Wal.Entry.Accept) recovered.get(1);
        assertEquals(new HashedBytes(new byte[]{10}), accept.value());

        assertEquals(1024, ((Wal.Entry.BallotBump) recovered.get(2)).reservedUpTo());
        wal2.close();
    }

    @Test
    @DisplayName("Snapshot + WAL tail are both replayed on recovery")
    void snapshotAndPartialReplay() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();

        assertNull(wal.openSnapshot());
        wal.replayTail(e -> {
        });

        final HashedBytes key = new HashedBytes(new byte[]{1});
        for (int i = 1; i <= 5; i++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(i, N1),
                    new HashedBytes(new byte[]{(byte) i}), false));
        }

        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        writer.write(new Wal.SnapshotEntry(key, new Ballot(5, N1), new Ballot(5, N1),
                new HashedBytes(new byte[]{5}), false));
        writer.commit(new Wal.Reservations(0, 0L));
        wal.truncateBeforeSnapshot();

        for (int i = 6; i <= 10; i++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(i, N1),
                    new HashedBytes(new byte[]{(byte) i}), false));
        }
        wal.force();
        wal.close();

        final FileWal wal2 = new FileWal(config);
        wal2.initialize();

        final Wal.SnapshotReader reader = wal2.openSnapshot();
        assertNotNull(reader);
        assertEquals(1, reader.totalEntries());

        final Wal.SnapshotEntry snapshotEntry = reader.next();
        assertEquals(key, snapshotEntry.key());
        assertEquals(new HashedBytes(new byte[]{5}), snapshotEntry.value());
        assertFalse(reader.hasNext());
        reader.close();

        final List<Wal.Entry> replayed = new ArrayList<>();
        wal2.replayTail(replayed::add);

        assertEquals(5, replayed.size());
        for (int i = 0; i < 5; i++) {
            final Wal.Entry.Accept accept = (Wal.Entry.Accept) replayed.get(i);
            assertEquals(new Ballot(6 + i, N1), accept.ballot());
        }
        wal2.close();
    }

    @Test
    @DisplayName("WAL rolls into multiple sealed files once the size threshold is crossed")
    void walRollingProducesMultipleSealedFiles() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        assertNull(wal.openSnapshot());
        wal.replayTail(e -> {
        });

        final HashedBytes key = new HashedBytes(new byte[100]);
        for (int i = 0; i < 100; i++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(i, N1),
                    new HashedBytes(new byte[100]), false));
        }
        wal.close();

        final long sealedCount;
        try (Stream<Path> files = Files.list(config.walDirectory())) {
            sealedCount = files
                    .filter(p -> p.getFileName().toString().endsWith(".wal"))
                    .count();
        }
        assertTrue(sealedCount > 1, "Expected multiple sealed WAL files, got " + sealedCount);

        final FileWal wal2 = new FileWal(config);
        wal2.initialize();
        assertNull(wal2.openSnapshot());

        final List<Wal.Entry> recovered = new ArrayList<>();
        wal2.replayTail(recovered::add);
        assertEquals(100, recovered.size());
        wal2.close();
    }

    @Test
    @DisplayName("Compaction deletes WAL files whose last LSN is covered by a snapshot")
    void compactionDeletesOldWalFiles() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        assertNull(wal.openSnapshot());
        wal.replayTail(e -> {
        });

        // Write enough entries (100-byte values, 4 KB max file) to roll the WAL
        // into several sealed files. All share one key; the final state is the
        // last accept.
        final HashedBytes key = new HashedBytes(new byte[]{1});
        for (int i = 1; i <= 100; i++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(i, N1),
                    new HashedBytes(new byte[100]), false));
        }

        final long sealedBefore = countSealedWalFiles();
        assertTrue(sealedBefore > 1,
                "Expected multiple sealed WAL files before compaction, got " + sealedBefore);

        // Snapshot at the current LSN (covers every entry written so far), then compact. Every
        // sealed file's lastLsn is <= the snapshot LSN, so all are reclaimable -- the runtime path
        // that has to advance the compaction horizon, or the WAL grows without bound.
        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        writer.write(new Wal.SnapshotEntry(key, new Ballot(100, N1), new Ballot(100, N1),
                new HashedBytes(new byte[]{100}), false));
        writer.commit(new Wal.Reservations(0, 0L));
        wal.truncateBeforeSnapshot();

        final long sealedAfter = countSealedWalFiles();
        assertEquals(0, sealedAfter,
                "Compaction should delete every sealed WAL file covered by the snapshot, "
                        + "but " + sealedAfter + " remained");

        wal.close();

        // The compacted-away state must still recover from the snapshot alone.
        final FileWal reopened = new FileWal(config);
        reopened.initialize();
        final Wal.SnapshotReader reader = reopened.openSnapshot();
        assertNotNull(reader);
        assertEquals(1, reader.totalEntries());
        final Wal.SnapshotEntry recovered = reader.next();
        assertEquals(key, recovered.key());
        assertEquals(new HashedBytes(new byte[]{100}), recovered.value());
        assertEquals(new Ballot(100, N1), recovered.accepted());
        reader.close();
        reopened.close();
    }

    private long countSealedWalFiles() throws IOException {
        try (Stream<Path> files = Files.list(config.walDirectory())) {
            return files
                    .filter(p -> p.getFileName().toString().endsWith(".wal"))
                    .count();
        }
    }

    @Test
    @DisplayName("Snapshot retention keeps at most N snapshots on disk")
    void snapshotRetentionKeepsOnlyN() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        assertNull(wal.openSnapshot());
        wal.replayTail(e -> {
        });

        final HashedBytes key = new HashedBytes(new byte[]{1});
        for (int round = 0; round < 4; round++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(round + 1, N1),
                    new HashedBytes(new byte[]{(byte) round}), false));

            final Wal.SnapshotWriter writer = wal.beginSnapshot();
            writer.write(new Wal.SnapshotEntry(key, new Ballot(round + 1, N1),
                    new Ballot(round + 1, N1), new HashedBytes(new byte[]{(byte) round}), false));
            writer.commit(new Wal.Reservations(0, 0L));
            wal.truncateBeforeSnapshot();
        }

        final long snapCount;
        try (Stream<Path> files = Files.list(config.snapshotDirectory())) {
            snapCount = files
                    .filter(p -> p.getFileName().toString().endsWith(".snap"))
                    .count();
        }
        // Exactly the retained count: "at most" also passes when retention deleted every snapshot,
        // which is the failure this is here to catch.
        assertEquals(config.snapshotRetentionCount(), snapCount,
                "Expected exactly " + config.snapshotRetentionCount() + " snapshots, got " + snapCount);

        wal.close();
    }

    @Test
    @DisplayName("Crash recovery normalises the open WAL file and replays its content")
    void crashRecoveryNormalizesOpenWalFile() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        assertNull(wal.openSnapshot());
        wal.replayTail(e -> {
        });

        final HashedBytes key = new HashedBytes(new byte[]{1});
        for (int i = 1; i <= 3; i++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(i, N1),
                    new HashedBytes(new byte[]{(byte) i}), false));
        }
        wal.force();

        // The crash: close() is deliberately NOT called. close() rolls the writer, which seals
        // .open into .wal -- exactly the normalisation this test is supposed to be about, so a
        // test that closes first can never reach the recovery path. Abandoning the writer leaves
        // the file on disk in its unsealed state, which is what a killed process leaves behind.
        // (The channel stays open; POSIX rename over an open descriptor is what recovery does.)
        assertTrue(openWalFiles().findAny().isPresent(),
                "The crash state under test is an unsealed .open file");

        final FileWal wal2 = new FileWal(config);
        wal2.initialize();
        assertNull(wal2.openSnapshot());

        final List<Wal.Entry> recovered = new ArrayList<>();
        wal2.replayTail(recovered::add);

        // Normalised: the .open file was truncated to its last valid record and sealed, so the
        // records are replayed from a sealed segment like any other.
        assertFalse(openWalFiles().findAny().isPresent(),
                "Recovery must leave no .open file behind");
        assertEquals(3, recovered.size(), "Every forced record must survive the crash");
        wal2.close();
    }

    /** The unsealed WAL files currently on disk. */
    private Stream<Path> openWalFiles() throws IOException {
        return Files.list(config.walDirectory())
                .filter(p -> p.getFileName().toString().endsWith(".open"));
    }

    @Test
    @DisplayName("BallotBump entry persists and survives close/reopen via snapshot")
    void ballotBumpPersistedAndRecovered() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        assertNull(wal.openSnapshot());
        wal.replayTail(e -> {
        });

        wal.append(new Wal.Entry.BallotBump(5000));
        wal.append(new Wal.Entry.BallotBump(10000));

        final HashedBytes key = new HashedBytes(new byte[]{1});
        wal.append(new Wal.Entry.Accept(key, new Ballot(1, N1), new HashedBytes(new byte[]{1}), false));

        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        writer.write(new Wal.SnapshotEntry(key, new Ballot(1, N1), new Ballot(1, N1),
                new HashedBytes(new byte[]{1}), false));
        // Both reservations, with different values: they are the same primitive with opposite
        // meanings, so a test that leaves one at zero would not catch them being swapped on the way
        // to disk or back.
        writer.commit(new Wal.Reservations(10000, 7777));
        wal.close();

        final FileWal wal2 = new FileWal(config);
        wal2.initialize();

        final Wal.SnapshotReader reader = wal2.openSnapshot();
        assertNotNull(reader);
        assertEquals(10000, reader.reservations().proposerBallot(),
                "The proposer's ballot reservation must survive the file round trip");
        assertEquals(7777, reader.reservations().promiseCeiling(),
                "And so must the acceptor's promise ceiling -- losing it silently drops the "
                        + "post-recovery floor to zero");
        reader.close();
        wal2.close();
    }

    @Test
    @DisplayName("Mid-record byte flip is rejected by per-record CRC; replay halts at the corruption")
    void midRecordCorruptionDetectedByCrc() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        wal.replayTail(e -> {
        });

        final HashedBytes key = new HashedBytes(new byte[]{1});
        final int recordCount = 10;
        for (int i = 1; i <= recordCount; i++) {
            wal.append(new Wal.Entry.Accept(key, new Ballot(i, N1),
                    new HashedBytes(new byte[]{(byte) i}), false));
        }
        wal.force();
        wal.close();

        // Find the (single) WAL file and flip one byte well inside it -- after
        // the file header and a few records, so we corrupt a record that is
        // not the first and not the last.
        final Path walFile;
        try (Stream<Path> files = Files.list(config.walDirectory())) {
            walFile = files
                    .filter(p -> {
                        final String name = p.getFileName().toString();
                        return name.endsWith(".wal") || name.endsWith(".open");
                    })
                    .findFirst().orElseThrow();
        }
        final long fileSize = Files.size(walFile);
        // Pick a midpoint byte to flip -- guaranteed to be inside a record
        // payload or the CRC field of one of them.
        final long flipOffset = fileSize / 2;
        try (FileChannel ch = FileChannel.open(
                walFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final ByteBuffer one = ByteBuffer.allocate(1);
            ch.read(one, flipOffset);
            one.flip();
            final byte b = one.get();
            one.clear();
            one.put((byte) (b ^ 0x40)).flip();
            ch.write(one, flipOffset);
        }

        // Reopen and replay. We expect *some* prefix of records to be
        // accepted, the corrupt record (and everything after it) to be
        // skipped, and no exception to escape.
        final FileWal wal2 = new FileWal(config);
        wal2.initialize();
        final List<Wal.Entry> recovered = new ArrayList<>();
        wal2.replayTail(recovered::add);
        wal2.close();

        assertTrue(recovered.size() < recordCount,
                "Expected corruption to truncate replay; got all " + recovered.size() + " records");
        long lastBallot = 0;
        for (final Wal.Entry e : recovered) {
            final Wal.Entry.Accept acc = (Wal.Entry.Accept) e;
            assertTrue(acc.ballot().counter() > lastBallot,
                    "Recovered ballots must be strictly increasing");
            lastBallot = acc.ballot().counter();
        }
    }
}
