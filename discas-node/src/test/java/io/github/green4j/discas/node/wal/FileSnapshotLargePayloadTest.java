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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Committing a snapshot whose payload exceeds one read chunk.
 * <p>
 * The commit checksum has to read the payload back -- it covers {@code [meta][entries]}, and the
 * meta's real values only exist at commit time while the entries were written long before, so a
 * running CRC cannot be prefixed with the meta afterwards (CRC32 has no combine). That read-back
 * used to allocate a single buffer the size of the whole payload, sized by an {@code int} cast, so
 * it spiked the heap on the consensus thread and silently computed a wrong length past 2 GiB --
 * while {@code FileSnapshotReader} had been carefully written to stream for exactly that reason.
 * <p>
 * A multi-chunk payload is what distinguishes a streaming implementation from a single-buffer one:
 * anything smaller passes either way.
 */
@DisplayName("FileSnapshotWriter -- payloads larger than one read chunk")
class FileSnapshotLargePayloadTest {

    private static final NodeId N1 = NodeId.of("1");

    /** Matches the writer's internal chunk; the payload must comfortably exceed it. */
    private static final int CHUNK_BYTES = 256 * 1024;
    private static final int VALUE_BYTES = 4 * 1024;
    private static final int ENTRIES = (CHUNK_BYTES / VALUE_BYTES) * 3; // ~3 chunks

    @TempDir
    Path tempDir;
    private StorageConfig config;

    @BeforeEach
    void setup() {
        config = StorageConfig.builder()
                .baseDirectory(tempDir)
                .walMaxFileBytes(16 * 1024 * 1024)
                .snapshotRetentionCount(2)
                .build();
    }

    private static HashedBytes key(final int i) {
        return new HashedBytes(String.format("key-%06d", i).getBytes());
    }

    private static HashedBytes value(final int i) {
        final byte[] bytes = new byte[VALUE_BYTES];
        // Vary the content so a checksum bug cannot be hidden by uniform bytes.
        for (int b = 0; b < bytes.length; b++) {
            bytes[b] = (byte) (i + b);
        }
        return new HashedBytes(bytes);
    }

    @Test
    @DisplayName("A multi-chunk snapshot commits, validates and reads back intact")
    void multiChunkSnapshotRoundTrips() throws IOException {
        final FileWal wal = new FileWal(config);
        wal.initialize();

        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        for (int i = 0; i < ENTRIES; i++) {
            writer.write(new Wal.SnapshotEntry(
                    key(i), new Ballot(i + 1, N1), new Ballot(i + 1, N1), value(i), false));
        }
        writer.commit(new Wal.Reservations(4242L, 0L));
        wal.close();

        final FileWal reopened = new FileWal(config);
        reopened.initialize();
        final Wal.SnapshotReader reader = reopened.openSnapshot();

        // openSnapshot returns null when the payload CRC does not match, so a wrong checksum from
        // the writer shows up here rather than as corrupt data later.
        assertNotNull(reader, "The committed snapshot must validate against its payload CRC");
        assertEquals(ENTRIES, reader.totalEntries());
        assertEquals(4242L, reader.reservations().proposerBallot());

        final List<HashedBytes> readKeys = new ArrayList<>();
        while (reader.hasNext()) {
            final Wal.SnapshotEntry entry = reader.next();
            readKeys.add(entry.key());
            assertEquals(value(indexOf(entry.key())), entry.value(),
                    "Entry content must survive a multi-chunk round trip");
        }
        reader.close();
        reopened.close();

        assertEquals(ENTRIES, readKeys.size());
        assertTrue(readKeys.contains(key(0)) && readKeys.contains(key(ENTRIES - 1)),
                "First and last entries must both be present");
    }

    @Test
    @DisplayName("An empty snapshot still commits and validates")
    void emptySnapshotRoundTrips() throws IOException {
        // The streaming loop must terminate correctly when the payload is only the meta block --
        // the first read returns the meta, the second returns -1.
        final FileWal wal = new FileWal(config);
        wal.initialize();

        final Wal.SnapshotWriter writer = wal.beginSnapshot();
        writer.commit(new Wal.Reservations(7L, 0L));
        wal.close();

        final FileWal reopened = new FileWal(config);
        reopened.initialize();
        final Wal.SnapshotReader reader = reopened.openSnapshot();

        assertNotNull(reader, "An entry-less snapshot is still a valid snapshot");
        assertEquals(0, reader.totalEntries());
        assertEquals(7L, reader.reservations().proposerBallot());
        reader.close();
        reopened.close();
    }

    private static int indexOf(final HashedBytes key) {
        final byte[] raw = new byte[key.size()];
        key.toBuffer().get(raw);
        return Integer.parseInt(new String(raw).substring("key-".length()));
    }
}
