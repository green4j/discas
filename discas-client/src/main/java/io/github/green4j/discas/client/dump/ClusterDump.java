/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.client.dump;

import io.github.green4j.discas.client.DisCasClient;

import io.github.green4j.discas.client.ScanResult;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.dump.DumpHeader;
import io.github.green4j.discas.common.dump.DumpWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Dumps a cluster's live pairs into a channel: scan the prefixes, read each key, encode.
 *
 * <p><b>Values are not read, only carried.</b> Nothing here inspects what a value is -- a lock
 * record is a value like any other. Only the application knows what lives where, which is exactly
 * why the dump space is named by prefixes: ask for a prefix that holds locks and the locks travel.
 * A rule that fished them out by recognising their bytes would be this code claiming to know the
 * key space better than the person who laid it out.
 *
 * <p>An operation over a {@link DisCasClient} rather than a feature of the agent, so an
 * application that already holds a client gets backups without standing up one, and there is a
 * single implementation of <em>what a dump is</em> rather than one per caller. The agent's endpoint
 * is one of its callers.
 *
 * <h2>What it costs, and what it cannot promise</h2>
 * A dump is a scan followed by a <b>linearizable read per key</b> -- one Paxos round each, because
 * the alternative is a backup that quietly carries a stale value from whichever member answered.
 * The key list is from one instant and each value from another, and a scan pages with a per-page
 * quorum with no global revision behind it, so a write racing the dump may land on either side of
 * it and two keys written together may not both be in it. For a migration this costs nothing --
 * step one there is to stop writes. For <em>back up a live cluster</em> it is the honest limit.
 *
 * @see io.github.green4j.discas.common.dump.DumpWriter
 */
public final class ClusterDump {

    private ClusterDump() {
    }

    /**
     * Streams every live pair under {@code prefixes} into {@code out}, in key order within each
     * prefix, and commits the dump. An empty or {@code null} prefix list dumps the whole key space.
     *
     * <p>The channel is the caller's: this never closes it, and leaves it holding an uncommitted --
     * therefore unreadable -- dump if the operation fails, which is what stops a half-finished
     * backup from reading as a whole one.
     */
    public static CompletableFuture<DumpSummary> dump(final DisCasClient client,
                                                      final List<ByteBuffer> prefixes,
                                                      final WritableByteChannel out) {
        return dump(client, prefixes, out, DumpProgress.NONE);
    }

    /** As {@link #dump}, telling {@code progress} how far it has got after every key. */
    public static CompletableFuture<DumpSummary> dump(final DisCasClient client,
                                                      final List<ByteBuffer> prefixes,
                                                      final WritableByteChannel out,
                                                      final DumpProgress progress) {
        final List<ByteBuffer> asked = copyOf(prefixes);
        final Run run;
        try {
            run = new Run(client, asked,
                    new DumpWriter(out, new DumpHeader(System.currentTimeMillis(), asked)),
                    progress == null ? DumpProgress.NONE : progress);
        } catch (final IOException failedBeforeItStarted) {
            return failed(failedBeforeItStarted);
        }
        return run.prefixFrom(0).thenCompose(ignored -> run.commit());
    }

    /** What a finished dump did, for the operator asking whether it carried what they expected. */
    public static final class DumpSummary {
        private final long entries;
        private final long keysVanished;

        DumpSummary(final long entries, final long keysVanished) {
            this.entries = entries;
            this.keysVanished = keysVanished;
        }

        /** Pairs written. */
        public long entries() {
            return entries;
        }

        /**
         * Keys the scan listed and the read did not find: deleted between the two, which is the
         * fuzziness of a live dump made countable rather than hidden.
         */
        public long keysVanished() {
            return keysVanished;
        }
    }

    /** One dump in progress. Touched only from the client's completion callbacks, in order. */
    private static final class Run {
        private final DisCasClient client;
        private final List<ByteBuffer> prefixes;
        private final DumpWriter writer;
        private final DumpProgress progress;

        private long keysVanished;

        private Run(final DisCasClient client,
                    final List<ByteBuffer> asked,
                    final DumpWriter writer,
                    final DumpProgress progress) {
            this.client = client;
            this.progress = progress;
            // An empty ask is the whole key space, which is one scan of the empty prefix. Said here
            // rather than branched on later: below this line a dump is always a list of prefixes.
            this.prefixes = asked.isEmpty()
                    ? Collections.singletonList(ByteBuffers.EMPTY) : asked;
            this.writer = writer;
        }

        private CompletableFuture<Void> prefixFrom(final int index) {
            if (index == prefixes.size()) {
                return CompletableFuture.completedFuture(null);
            }
            return pageFrom(index, null)
                    .thenComposeAsync(ignored -> prefixFrom(index + 1));
        }

        private CompletableFuture<Void> pageFrom(final int prefixIndex, final ByteBuffer cursor) {
            return client.scan(prefixes.get(prefixIndex), cursor, KvLimits.MAX_SCAN_LIMIT)
                    .thenComposeAsync(page -> entryFrom(page.results(), 0, prefixIndex)
                            .thenComposeAsync(ignored -> page.complete()
                                    ? CompletableFuture.<Void>completedFuture(null)
                                    : pageFrom(prefixIndex, page.nextCursor())));
        }

        /**
         * One key at a time, and the whole reason the walk is written as a chain rather than a
         * loop: a page holds keys, and holding a page's <em>values</em> would put an unbounded
         * slice of the cluster in the dumping process's heap.
         * <p>
         * Every step hops threads ({@code thenComposeAsync}) for two reasons: an in-process client
         * completes its futures inline, which would otherwise recurse once per key until the stack
         * ran out, and the channel writes must not land on the client's event loop.
         */
        private CompletableFuture<Void> entryFrom(final List<ScanResult> results,
                                                  final int index,
                                                  final int prefixIndex) {
            if (index == results.size()) {
                return CompletableFuture.completedFuture(null);
            }
            final ByteBuffer key = results.get(index).key();
            if (dumpedUnderAnEarlierPrefix(key, prefixIndex)) {
                return entryFrom(results, index + 1, prefixIndex);
            }
            return client.get(key).thenComposeAsync(read -> {
                try {
                    write(key, read.value());
                } catch (final IOException e) {
                    return ClusterDump.<Void>failed(e);
                }
                return entryFrom(results, index + 1, prefixIndex);
            });
        }

        private void write(final ByteBuffer key, final ByteBuffer value) throws IOException {
            if (value != null) {
                writer.writeEntry(key, value);
            } else {
                // Listed by the scan and gone by the read: deleted in between. A tombstone is not
                // a pair, and nothing further down would know what to do with one.
                keysVanished++;
            }
            // After every key, not only the written ones: a dump that spends a minute on a range
            // of keys that have all been deleted is still working.
            progress.advanced(writer.entryCount(), writer.bytesWritten());
        }

        /**
         * Overlapping prefixes are the operator's business, not ours -- but a key matching two of
         * them is dumped once, under the first, so the entry count does not overstate the key set.
         */
        private boolean dumpedUnderAnEarlierPrefix(final ByteBuffer key, final int prefixIndex) {
            for (int i = 0; i < prefixIndex; i++) {
                if (startsWith(key, prefixes.get(i))) {
                    return true;
                }
            }
            return false;
        }

        private CompletableFuture<DumpSummary> commit() {
            try {
                writer.commit();
            } catch (final IOException e) {
                return failed(e);
            }
            return CompletableFuture.completedFuture(
                    new DumpSummary(writer.entryCount(), keysVanished));
        }
    }

    private static boolean startsWith(final ByteBuffer key, final ByteBuffer prefix) {
        if (key.remaining() < prefix.remaining()) {
            return false;
        }
        for (int i = 0; i < prefix.remaining(); i++) {
            if (key.get(key.position() + i) != prefix.get(prefix.position() + i)) {
                return false;
            }
        }
        return true;
    }

    private static List<ByteBuffer> copyOf(final List<ByteBuffer> prefixes) {
        if (prefixes == null || prefixes.isEmpty()) {
            return Collections.emptyList();
        }
        // Copied because the walk outlives this call and re-reads every prefix per key.
        final List<ByteBuffer> copies = new ArrayList<>(prefixes.size());
        for (int i = 0; i < prefixes.size(); i++) {
            copies.add(ByteBuffers.copyReadOnly(prefixes.get(i)));
        }
        return copies;
    }

    private static <T> CompletableFuture<T> failed(final Throwable cause) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }
}
