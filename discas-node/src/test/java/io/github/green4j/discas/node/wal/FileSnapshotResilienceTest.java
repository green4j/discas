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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What happens to a <em>file-backed</em> snapshot that is damaged, truncated or oversized.
 * <p>
 * The snapshot classes were effectively untested: every other snapshot test drives
 * {@code InMemoryWal}, which never encodes a header, never checksums anything and never reads a
 * byte back. {@code FileWalIntegrationTest} covers the happy path and the WAL's own corruption
 * handling, but not the snapshot file's. That mattered -- a defect in this path breaks node
 * restart, which is the one thing every deployment depends on.
 * <p>
 * In particular {@code FileWal.openSnapshot} is documented to walk candidates newest-first and
 * take the first that validates, so a corrupt newest snapshot silently falls back to an older
 * one. That resilience had never been exercised.
 */
@DisplayName("Snapshot files -- corruption, truncation and oversized entries")
class FileSnapshotResilienceTest {

    private static final NodeId N1 = NodeId.of("1");

    @TempDir
    Path tempDir;
    private StorageConfig config;

    @BeforeEach
    void setup() {
        config = StorageConfig.builder()
                .baseDirectory(tempDir)
                .walMaxFileBytes(16 * 1024 * 1024)
                .snapshotRetentionCount(5)
                .build();
    }

    private static HashedBytes key(final String s) {
        return new HashedBytes(s.getBytes());
    }

    private static HashedBytes val(final String s) {
        return new HashedBytes(s.getBytes());
    }

    /**
     * Open a WAL through its real recovery sequence: initialise, read any snapshot, replay the
     * tail. {@code walWriter} is created during the replay, so appends are only legal afterwards
     * -- the order {@code DisCasNode} always follows.
     */
    private FileWal openWal() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();
        final Wal.SnapshotReader reader = wal.openSnapshot();
        if (reader != null) {
            reader.close();
        }
        wal.replayTail(e -> { });
        return wal;
    }

    /** Write and commit a snapshot at {@code lsn} holding one entry per supplied key. */
    private void writeSnapshot(final long lsn, final long reservedBallot, final String... keys)
            throws IOException {
        final FileWal wal = openWal();
        // Drive the LSN forward so each snapshot lands at a distinct, increasing LSN.
        for (long i = 0; i < lsn; i++) {
            wal.append(new Wal.Entry.BallotBump(i + 1));
        }
        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        for (final String k : keys) {
            writer.write(new Wal.SnapshotEntry(
                    key(k), new Ballot(1, N1), new Ballot(1, N1), val("v-" + k), false));
        }
        writer.commit(new Wal.Reservations(reservedBallot, 0L));
        wal.close();
    }

    private List<Path> snapshotFiles() throws IOException {
        try (Stream<Path> files = Files.list(config.snapshotDirectory())) {
            return files.filter(p -> p.getFileName().toString().endsWith(".snap"))
                    .sorted(Comparator.comparing(Path::toString))
                    .collect(Collectors.toList());
        }
    }

    /** Flip one bit deep in the payload, well past the header. */
    private static void corruptPayload(final Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long at = StorageFormat.SNAPSHOT_HEADER_BYTES
                    + StorageFormat.SNAPSHOT_META_BYTES + 4;
            final ByteBuffer one = ByteBuffer.allocate(1);
            ch.position(at);
            ch.read(one);
            one.flip();
            final byte flipped = (byte) (one.get(0) ^ 0x01);
            ch.position(at);
            ch.write(ByteBuffer.wrap(new byte[] {flipped}));
        }
    }

    private List<String> readAllKeys(final Wal.SnapshotReader reader) {
        final List<String> keys = new ArrayList<>();
        while (reader.hasNext()) {
            final HashedBytes k = reader.next().key();
            final byte[] raw = new byte[k.size()];
            k.toBuffer().get(raw);
            keys.add(new String(raw));
        }
        return keys;
    }

    @Test
    @DisplayName("A corrupt newest snapshot falls back to the previous good one")
    void corruptNewestFallsBackToOlder() throws IOException {
        writeSnapshot(1, 10L, "old-a", "old-b");
        writeSnapshot(5, 50L, "new-a", "new-b");

        final List<Path> files = snapshotFiles();
        assertEquals(2, files.size(), "Expected two retained snapshots");
        // Highest LSN sorts last by filename, and that is the one openSnapshot prefers.
        corruptPayload(files.get(files.size() - 1));

        final FileWal wal = new FileWal(config);
        wal.initialize();
        final Wal.SnapshotReader reader = wal.openSnapshot();

        assertNotNull(reader, "The older intact snapshot must still be usable");
        assertEquals(10L, reader.reservations().proposerBallot(),
                "Recovery must land on the older snapshot, not the damaged newest one");
        final List<String> keys = readAllKeys(reader);
        assertTrue(keys.contains("old-a") && keys.contains("old-b"), keys.toString());
        reader.close();
        wal.close();
    }

    /** The ways a snapshot file can be wrong, each caught by a different guard. */
    private enum Damage {
        /** A flipped payload bit: caught by the payload CRC. */
        FLIPPED_PAYLOAD_BIT {
            @Override
            void apply(final Path file) throws IOException {
                corruptPayload(file);
            }
        },
        /** A zeroed magic: caught before anything is read as a snapshot at all. */
        ZEROED_MAGIC {
            @Override
            void apply(final Path file) throws IOException {
                try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    ch.position(0);
                    ch.write(ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x00}));
                }
            }
        },
        /** A file cut inside the payload: the CRC covers the whole payload, so it fails at open. */
        TRUNCATED_PAYLOAD {
            @Override
            void apply(final Path file) throws IOException {
                final long fullSize = Files.size(file);
                try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    ch.truncate(fullSize - 8);
                }
            }
        },
        /** A file too short to hold even a header: rejected rather than crashed on. */
        SHORTER_THAN_A_HEADER {
            @Override
            void apply(final Path file) throws IOException {
                try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    ch.truncate(8);
                }
            }
        };

        abstract void apply(Path file) throws IOException;
    }

    @ParameterizedTest
    @EnumSource(Damage.class)
    @DisplayName("A damaged snapshot is rejected rather than served as valid state")
    void damagedSnapshotIsRejected(final Damage damage) throws IOException {
        writeSnapshot(1, 10L, "a", "b", "c");
        damage.apply(snapshotFiles().get(0));

        final FileWal wal = new FileWal(config);
        wal.initialize();
        // Silently recovering from a snapshot whose bytes changed would seed the node with state
        // it never agreed to.
        assertNull(wal.openSnapshot(), "A damaged snapshot must not be opened: " + damage);
        wal.close();
    }

    @Test
    @DisplayName("An entry larger than the reader's initial window is read back intact")
    void entryLargerThanReadWindowGrowsIt() throws IOException {
        // FileSnapshotReader starts with a 64 KiB window and grows it when a single entry cannot
        // fit. Nothing else exercises that growth branch: the large-payload test uses many small
        // entries, which only spans chunk boundaries.
        final int valueBytes = 300 * 1024;
        final byte[] big = new byte[valueBytes];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 31);
        }

        final FileWal wal = openWal();
        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        writer.write(new Wal.SnapshotEntry(
                key("huge"), new Ballot(1, N1), new Ballot(1, N1), new HashedBytes(big), false));
        writer.commit(new Wal.Reservations(99L, 0L));
        wal.close();

        final FileWal reopened = new FileWal(config);
        reopened.initialize();
        final Wal.SnapshotReader reader = reopened.openSnapshot();
        assertNotNull(reader);
        assertTrue(reader.hasNext());
        final Wal.SnapshotEntry entry = reader.next();
        assertEquals(new HashedBytes(big), entry.value(),
                "An entry bigger than the initial window must survive the window growing");
        reader.close();
        reopened.close();
    }

    @Test
    @DisplayName("An aborted snapshot leaves nothing behind to be recovered from")
    void abortedSnapshotLeavesNoFile() throws IOException {
        final FileWal wal = openWal();
        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        writer.write(new Wal.SnapshotEntry(
                key("a"), new Ballot(1, N1), new Ballot(1, N1), val("v"), false));
        writer.abort();
        wal.close();

        assertTrue(snapshotFiles().isEmpty(), "An aborted snapshot must not leave a .snap file");
        try (Stream<Path> files = Files.list(config.snapshotDirectory())) {
            assertTrue(files.noneMatch(p -> p.getFileName().toString().endsWith(".tmp")),
                    "The temporary file must be cleaned up too");
        }

        final FileWal reopened = new FileWal(config);
        reopened.initialize();
        assertNull(reopened.openSnapshot());
        reopened.close();
    }

    @Test
    @DisplayName("Writing to a finalised snapshot is refused")
    void writeAfterFinalizeIsRefused() throws IOException {
        final FileWal wal = openWal();
        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        writer.commit(new Wal.Reservations(1L, 0L));

        assertThrows(IllegalStateException.class, () -> writer.write(new Wal.SnapshotEntry(
                key("late"), new Ballot(1, N1), new Ballot(1, N1), val("v"), false)));
        wal.close();
    }
}
