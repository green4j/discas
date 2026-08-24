/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.dump;

import io.github.green4j.discas.client.DisCasClient;

import io.github.green4j.discas.client.ScanPage;
import io.github.green4j.discas.client.ScanResult;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.dump.DumpReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Writes a dump's pairs into a <b>running</b> cluster, overwriting whatever is already at those
 * keys, and -- if asked -- deletes what the dump did not bring.
 *
 * <p>This is the safe direction, and it is safe because it is unremarkable: every pair goes in as
 * an ordinary {@code put}, a full consensus round like any other write. The cluster is never told
 * anything about where the pairs came from, no ballot or promise from the source travels, and a
 * writer racing a load wins or loses by ballot order the way two writers always do. Nothing here
 * touches a data directory -- that is seeding a cluster that does not exist yet.
 *
 * <h2>Merge, or replace</h2>
 * {@link #load} is a <b>merge</b>: keys the dump does not mention are left alone.
 * {@link #loadAndCleanup} makes it a <b>replacement</b> of a named part of the key space -- after
 * every pair is in, it walks the prefixes it was given and deletes every key the dump did not
 * carry. The prefixes are what bounds the damage: with none, every key in the cluster that is not
 * in the dump is deleted.
 *
 * <h2>Blocking</h2>
 * The dump is read from a file, in order, and each write is awaited before the next pair is read.
 * That keeps memory flat whatever the size of the dump, stops a failure halfway from being followed
 * by thousands of writes already in flight, and makes the progress count mean what it says. Call it
 * from a thread that may block -- never from a client callback.
 */
public final class ClusterLoad {

    private ClusterLoad() {
    }

    /** What a finished load did. */
    public static final class LoadSummary {
        private final long written;
        private final long deleted;

        LoadSummary(final long written, final long deleted) {
            this.written = written;
            this.deleted = deleted;
        }

        /** Pairs written. */
        public long written() {
            return written;
        }

        /** Keys deleted by the cleanup pass; {@code 0} when there was none. */
        public long deleted() {
            return deleted;
        }
    }

    /**
     * Writes every pair of {@code dump} -- refusing the file outright unless it is whole -- leaving
     * keys it does not mention alone.
     *
     * @param progress told after each pair, with the total from the dump's trailer; may be null
     */
    public static LoadSummary load(final DisCasClient client,
                                   final Path dump,
                                   final LoadProgress progress) throws IOException {
        return new LoadSummary(write(client, dump, report(progress)), 0L);
    }

    /**
     * As {@link #load}, then deletes every key under {@code prefixes} that the dump did not carry,
     * making those prefixes hold exactly what the dump holds.
     *
     * <p>The cleanup needs to know which keys arrived, so it reads the dump a second time and holds
     * <b>its keys</b> in memory -- keys only, never values. That is the one place this operation is
     * not flat in the size of the dump, and it is the price of answering "was this key in it?" for
     * a key space that arrives in a different order.
     *
     * <p>Two things it is honest about. A key written by somebody else while the load was running
     * is not in the dump either, so it is deleted; and deletion is by key, unfenced, so it removes
     * whatever is there rather than what the scan saw. Cleanup is for a key space an operator has
     * taken charge of, not for one under live traffic.
     *
     * @param prefixes where to clean up; <b>empty means the whole key space</b>
     */
    public static LoadSummary loadAndCleanup(final DisCasClient client,
                                             final Path dump,
                                             final List<ByteBuffer> prefixes,
                                             final LoadProgress progress) throws IOException {
        final LoadProgress report = report(progress);
        final long written = write(client, dump, report);
        final long deleted = cleanup(client, dump, prefixes, report);
        return new LoadSummary(written, deleted);
    }

    private static long write(final DisCasClient client,
                              final Path dump,
                              final LoadProgress progress) throws IOException {
        try (DumpReader reader = DumpReader.open(dump)) {
            final long total = reader.entryCount();
            final long[] written = {0};
            reader.forEachEntry((key, value) -> {
                // The views are the reader's window and are valid only for this call -- which is
                // exactly as long as this write takes, because it is awaited here.
                await(client.put(key, value));
                written[0]++;
                progress.advanced(written[0], total);
            });
            return written[0];
        }
    }

    private static long cleanup(final DisCasClient client,
                                final Path dump,
                                final List<ByteBuffer> prefixes,
                                final LoadProgress progress) throws IOException {
        final Set<ByteBuffer> carried = keysOf(dump);
        final List<ByteBuffer> scopes = prefixes == null || prefixes.isEmpty()
                ? Collections.singletonList(ByteBuffers.EMPTY) : prefixes;

        long examined = 0;
        long deleted = 0;
        for (int i = 0; i < scopes.size(); i++) {
            ByteBuffer cursor = null;
            do {
                final ScanPage page =
                        await(client.scan(scopes.get(i), cursor, KvLimits.MAX_SCAN_LIMIT));
                final List<ScanResult> found = page.results();
                for (int k = 0; k < found.size(); k++) {
                    final ByteBuffer key = found.get(k).key();
                    examined++;
                    if (!carried.contains(ByteBuffers.copyReadOnly(key))) {
                        await(client.delete(key));
                        deleted++;
                    }
                    progress.cleaning(examined, deleted);
                }
                cursor = page.nextCursor();
            } while (cursor != null);
        }
        return deleted;
    }

    /** The dump's keys, copied: {@link ByteBuffer} compares by content, which is what is wanted. */
    private static Set<ByteBuffer> keysOf(final Path dump) throws IOException {
        try (DumpReader reader = DumpReader.open(dump)) {
            final Set<ByteBuffer> keys = new HashSet<>();
            reader.forEachEntry((key, value) -> keys.add(ByteBuffers.copyReadOnly(key)));
            return keys;
        }
    }

    private static LoadProgress report(final LoadProgress progress) {
        return progress == null ? LoadProgress.NONE : progress;
    }

    private static <T> T await(final CompletableFuture<T> operation) throws IOException {
        try {
            return operation.get();
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", interrupted);
        } catch (final ExecutionException failed) {
            // The cluster refused or could not answer. Reported as an IOException so a caller has
            // one thing to catch for "the dump could not be read" and "the cluster would not take
            // it", which are the same outcome from where they stand.
            throw new IOException("The cluster did not complete an operation of the load",
                    failed.getCause());
        }
    }
}
