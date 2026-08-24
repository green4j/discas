/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.seed;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.dump.DumpReader;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.HashedBytes;
import io.github.green4j.discas.node.wal.EntryCodec;
import io.github.green4j.discas.node.wal.IncarnationMarker;
import io.github.green4j.discas.node.wal.StorageConfig;
import io.github.green4j.discas.node.wal.Wal;
import io.github.green4j.discas.node.wal.WalWriter;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes one member's data directory for a cluster that does not exist yet: an incarnation marker
 * and a WAL holding the dump's pairs, ready for a node to be started on.
 *
 * <p>A dump is not a snapshot file, so that no procedure ever reads <em>copy this into the data
 * directory</em> -- one short step from copying a whole directory, which is the one thing this
 * store cannot allow. The dangerous act is performed here instead: the directory is written from a
 * file that has been proven whole, with an incarnation minted here.
 *
 * <p>Every pair is written as one {@code ACCEPT} at {@link #SEED_BALLOT} -- a single fixed ballot,
 * identical on every member -- and nothing else is written: no promises above it, no ballot
 * reservation, no tombstones. So the members are byte-identical from their first second, and
 * anti-entropy has nothing to reconcile. The stated cost is that a {@code Version} a client held
 * against the old cluster does not fence against this one; that is true of any migration.
 *
 * <p>Refuses a directory that already holds state. Seeding is only ever for a member that does not
 * exist yet, and a directory that has been a member before is exactly the case the store forbids
 * bringing back.
 */
public final class MemberSeed {

    /**
     * The ballot every seeded pair is accepted at: counter 1, no node id.
     * <p>
     * Above {@link Ballot#ZERO}, so the value is a real acceptance rather than an empty slot, and
     * below every ballot the new cluster will issue -- a ballot compares by counter and then by node
     * id, and {@link NodeId#NONE} sorts before any real one, so the first proposer to touch the key
     * at counter 1 already outranks it without needing to know it was there.
     */
    public static final Ballot SEED_BALLOT = new Ballot(1L, NodeId.NONE);

    private MemberSeed() {
    }

    /**
     * Creates {@code baseDirectory} and writes a member's state into it.
     *
     * @param dump     the pairs to seed, or {@code null} for an empty member (a cluster with no
     *                 data, which is still a cluster somebody has to start)
     * @param progress told after each pair, with the dump's entry count; may be {@code null}
     * @return pairs written
     * @throws IOException if the directory already holds state, or the dump is not a whole one
     */
    public static long write(final Path baseDirectory,
                             final Path dump,
                             final SeedProgress progress) throws IOException {
        requireEmpty(baseDirectory);
        final SeedProgress report = progress == null ? SeedProgress.NONE : progress;

        final StorageConfig storage = StorageConfig.builder().baseDirectory(baseDirectory).build();
        Files.createDirectories(storage.walDirectory());
        Files.createDirectories(storage.snapshotDirectory());
        IncarnationMarker.create(baseDirectory);

        final WalWriter wal = new WalWriter(storage, 0L);
        final long[] written = {0};
        if (dump != null) {
            try (DumpReader reader = DumpReader.open(dump)) {
                final long total = reader.entryCount();
                reader.forEachEntry((key, value) -> {
                    written[0]++;
                    wal.append(written[0], EntryCodec.TYPE_ACCEPT, EntryCodec.encode(
                            new Wal.Entry.Accept(new HashedBytes(key), SEED_BALLOT,
                                    new HashedBytes(value), false)));
                    report.advanced(written[0], total);
                });
            }
        }
        // Seals the active segment and fsyncs the directory: the state has to be on the disk before
        // anyone is told to start a node on it, and an unsealed segment is one a node would have to
        // recover rather than simply read.
        wal.roll();
        return written[0];
    }

    private static void requireEmpty(final Path baseDirectory) throws IOException {
        if (!Files.exists(baseDirectory)) {
            return;
        }
        if (!Files.isDirectory(baseDirectory)) {
            throw new IOException(baseDirectory + " is not a directory");
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(baseDirectory)) {
            if (entries.iterator().hasNext()) {
                throw new IOException(baseDirectory + " is not empty; seeding writes a member that "
                        + "does not exist yet, and never into a directory that has been one");
            }
        }
    }
}
