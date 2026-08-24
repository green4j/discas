/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.identity.IncarnationId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * In-memory WAL for tests. No file I/O; deterministic and fast.
 */
public final class InMemoryWal implements Wal {
    private final List<Entry> entries = new ArrayList<>();
    private long reservedProposerBallot = 0;

    /**
     * How many entries have been forced. Everything past this is in the "buffered, acknowledged, not
     * yet on disk" state that an unclean shutdown loses -- see {@link #loseUnforcedTail}.
     */
    private int forcedUpTo = 0;

    private boolean failForce = false;
    private boolean degraded = false;
    private boolean continuityLost = false;

    /** A memory-backed log forgets everything on construction, so each one is a new incarnation. */
    private final IncarnationId incarnation = IncarnationId.generate();

    private List<SnapshotEntry> snapshot = null;
    private Reservations snapshotReservations = Reservations.NONE;
    private int snapshotCutoff = -1;

    @Override
    public boolean append(final Entry entry) {
        entries.add(entry);
        if (entry instanceof Entry.BallotBump) {
            final long upTo = ((Entry.BallotBump) entry).reservedUpTo();
            if (upTo > reservedProposerBallot) {
                reservedProposerBallot = upTo;
            }
        }
        return true;
    }

    @Override
    public SnapshotReader openSnapshot() {
        if (snapshot == null) {
            return null;
        }
        return new MemSnapshotReader(new ArrayList<>(snapshot), snapshotReservations);
    }

    @Override
    public void replayTail(final Consumer<Entry> consumer) {
        final int start = snapshotCutoff >= 0 ? snapshotCutoff : 0;
        for (int i = start; i < entries.size(); i++) {
            consumer.accept(entries.get(i));
        }
    }

    @Override
    public SnapshotWriter beginSnapshot() {
        return new MemSnapshotWriter();
    }

    @Override
    public void truncateBeforeSnapshot() {
        if (snapshotCutoff >= 0 && snapshotCutoff <= entries.size()) {
            entries.subList(0, snapshotCutoff).clear();
            snapshotCutoff = 0;
        }
    }

    @Override
    public IncarnationId incarnation() {
        return incarnation;
    }

    /**
     * Whether this log holds anything to derive a ceiling from.
     * <p>
     * A memory-backed log cannot have a hole in it -- there are no files to delete half of -- so
     * continuity is free and the only question left is whether it recorded anything. A test that
     * seeds entries and then builds a node around this log is modelling a <em>restart</em>, and such
     * a node proves its own ceiling and asks the cluster for nothing.
     */
    @Override
    public boolean ceilingIsProven() {
        return !continuityLost && (!entries.isEmpty() || snapshot != null);
    }

    /**
     * Model a directory that came back with half of itself: a log whose snapshot was deleted from
     * under it, or a snapshot whose log is gone.
     * <p>
     * There is no way to do that to a memory-backed log for real -- there are no files to remove
     * half of -- so what it produces is set directly: the entries are still here, and this log
     * cannot account for where they start. {@code StorageRecoveryMatrixTest} draws that conclusion
     * from real directories; this is for tests about what a <em>cluster</em> does with such a
     * member, which is a node that keeps its keys and has to take its promise floor from the
     * others.
     */
    public void loseContinuity() {
        continuityLost = true;
    }

    @Override
    public void force() {
        if (failForce) {
            // A failed writeback is how a real log finds out its pages never reached the platter,
            // and it degrades because of it -- so the caller learns from isDegraded(), not from a
            // return value force() does not have.
            degraded = true;
            return;
        }
        forcedUpTo = entries.size();
    }

    /**
     * Make every {@link #force()} from now on fail, degrading the log.
     * <p>
     * For the paths whose answer is a claim about the disk: they must be unable to make it when the
     * force behind it did not happen.
     */
    public void failForce() {
        failForce = true;
    }

    @Override
    public boolean isDegraded() {
        return degraded;
    }

    @Override
    public String degradedReason() {
        return degraded ? "test-induced force failure" : null;
    }

    /**
     * Simulate an unclean shutdown: drop every entry appended since the last {@link #force()}.
     * <p>
     * The fault promise durability is about, and the reason it is hard to notice in the wild: the
     * surviving log is perfectly well-formed and simply short, so
     * replay reports success and nothing distinguishes it from a complete log.
     */
    public void loseUnforcedTail() {
        while (entries.size() > forcedUpTo) {
            entries.remove(entries.size() - 1);
        }
    }

    /**
     * Model a hole: records this log no longer has and cannot account for the absence of.
     * <p>
     * Unlike {@link #loseUnforcedTail}, this can swallow a record that was <em>forced</em> -- a
     * reclaimed segment that never came back, a restore that landed short. That is the fault no
     * local check can see, which is why continuity goes with it: a log missing records from its
     * middle is a log that cannot prove where its history starts, and the node that opens it must
     * take its floor from the cluster.
     */
    public void swallow(final Predicate<Entry> lost) {
        entries.removeIf(lost);
        forcedUpTo = Math.min(forcedUpTo, entries.size());
        continuityLost = true;
    }

    /** How many entries of {@code type} the log currently holds. */
    public int countEntries(final Class<? extends Entry> type) {
        int found = 0;
        for (final Entry entry : entries) {
            if (type.isInstance(entry)) {
                found++;
            }
        }
        return found;
    }

    public List<Entry> entries() {
        return entries;
    }

    public long reservedProposerBallot() {
        return reservedProposerBallot;
    }

    private final class MemSnapshotWriter implements SnapshotWriter {
        private final List<SnapshotEntry> buffer = new ArrayList<>();
        private boolean committed = false;

        @Override
        public void write(final SnapshotEntry entry) {
            buffer.add(entry);
        }

        @Override
        public void commit(final Reservations reservations) {
            snapshot = new ArrayList<>(buffer);
            snapshotReservations = reservations;
            snapshotCutoff = entries.size();
            committed = true;
        }

        @Override
        public void abort() {
            if (!committed) {
                buffer.clear();
            }
        }
    }

    private static final class MemSnapshotReader implements SnapshotReader {
        private final List<SnapshotEntry> data;
        private final Reservations reservations;
        private int pos = 0;

        MemSnapshotReader(final List<SnapshotEntry> data, final Reservations reservations) {
            this.data = data;
            this.reservations = reservations;
        }

        @Override
        public long totalEntries() {
            return data.size();
        }

        @Override
        public boolean hasNext() {
            return pos < data.size();
        }

        @Override
        public SnapshotEntry next() {
            return data.get(pos++);
        }

        @Override
        public Reservations reservations() {
            return reservations;
        }

        @Override
        public void close() {
        }
    }
}
