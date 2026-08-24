/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node.wal;

import io.github.green4j.discas.common.identity.IncarnationId;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.node.HashedBytes;

import java.util.function.Consumer;

/**
 * Durable log of Paxos state changes, with snapshot compaction. Recovery loads the latest snapshot
 * through {@link #openSnapshot()}, then replays everything written after it through
 * {@link #replayTail}.
 * <p>
 * The log also carries the proposer's reserved ballot range ({@link Entry.BallotBump}) and the
 * acceptor's promise ceiling ({@link Entry.PromiseCeiling}), so that after a restart neither can
 * reissue or promise below a bound it already handed out.
 */
public interface Wal extends AutoCloseable {

    interface Entry {
        /** An acceptor promised a ballot for a key. */
        class Promise implements Entry {
            private final HashedBytes key;
            private final Ballot ballot;

            public Promise(final HashedBytes key,
                           final Ballot ballot) {
                this.key = key;
                this.ballot = ballot;
            }

            public HashedBytes key() {
                return key;
            }

            public Ballot ballot() {
                return ballot;
            }
        }

        /** An acceptor accepted a value for a key. */
        class Accept implements Entry {
            private final HashedBytes key;
            private final Ballot ballot;
            private final HashedBytes value;
            private final boolean tombstone;

            public Accept(final HashedBytes key,
                          final Ballot ballot,
                          final HashedBytes value,
                          final boolean tombstone) {
                this.key = key;
                this.ballot = ballot;
                this.value = value;
                this.tombstone = tombstone;
            }

            public HashedBytes key() {
                return key;
            }

            public Ballot ballot() {
                return ballot;
            }

            public HashedBytes value() {
                return value;
            }

            public boolean tombstone() {
                return tombstone;
            }
        }

        /**
         * A tombstone the cluster has agreed no replica can resurrect, so the key itself may go.
         * <p>
         * The one record in this log that <em>removes</em> state instead of raising a bound. What
         * makes that safe is that the decision was replicated before it was written: every member
         * either holds this tombstone durably or holds nothing for the key at all, so a key dropped
         * here cannot come back through anti-entropy -- absent compares equal to absent.
         * <p>
         * {@code ballot} is the ballot the tombstone was accepted at, and it is what makes the record
         * describe a state rather than a position: a member whose key was written again above that
         * ballot must keep the new value, and replaying the same log twice must reach the same state.
         */
        class Purge implements Entry {
            private final HashedBytes key;
            private final Ballot ballot;

            public Purge(final HashedBytes key,
                         final Ballot ballot) {
                this.key = key;
                this.ballot = ballot;
            }

            public HashedBytes key() {
                return key;
            }

            public Ballot ballot() {
                return ballot;
            }
        }

        /**
         * The proposer reserved ballot counters up to and including {@code reservedUpTo}. It must
         * not issue one above that without appending a new bump first, and recovery starts it at
         * {@code reservedUpTo + 1}.
         */
        class BallotBump implements Entry {
            private final long reservedUpTo;

            public BallotBump(final long reservedUpTo) {
                this.reservedUpTo = reservedUpTo;
            }

            public long reservedUpTo() {
                return reservedUpTo;
            }
        }

        /**
         * Acceptor reserved the right to promise ballot counters up to (and including)
         * {@code promisedUpTo}. The acceptor MUST NOT promise or accept a ballot whose counter is
         * {@code > promisedUpTo} without first persisting a new PromiseCeiling <b>and forcing it to
         * disk</b>.
         * <p>
         * The mirror of {@link BallotBump}, but <b>not</b> its exact image. A proposer
         * can reserve a floor on what it will issue because it controls what it issues; an acceptor
         * controls only what it <em>promises</em>, never what arrives. So this reserves a ceiling on
         * promising, and the floor on accepting is derived from it at recovery: every promise that
         * ever existed had a counter at or below the replayed {@code promisedUpTo}, so a promise
         * lost with an unforced WAL tail is still covered.
         * <p>
         * That derived floor is a <em>snapshot taken at startup</em>, not the live ceiling. Refusing
         * everything at or below the live ceiling would refuse the next legitimate ballot, turning
         * an amortized safety fix into a liveness bug.
         */
        class PromiseCeiling implements Entry {
            private final long promisedUpTo;

            public PromiseCeiling(final long promisedUpTo) {
                this.promisedUpTo = promisedUpTo;
            }

            public long promisedUpTo() {
                return promisedUpTo;
            }
        }
    }

    /**
     * Append an entry. Writes are buffered, so nothing is durable until {@link #force()}; between
     * syncs, durability rests on the quorum.
     * <p>
     * The return value reports whether the entry was <em>accepted into the log</em>, not whether
     * it reached disk. A caller that keeps an in-memory projection of the log must not apply the
     * entry when this returns {@code false}: doing so leaves memory holding a write the log will
     * never replay, so the node would serve state after a restart could not reproduce it.
     *
     * @return true if the entry was accepted; false if the log rejected it (already degraded, or
     *         the write failed and the log has just degraded)
     */
    boolean append(Entry entry);

    /** Flush buffered writes to durable storage. */
    default void force() {
    }

    /** Release resources. */
    @Override
    default void close() {
    }

    /**
     * True once the log has hit an unrecoverable I/O failure. The node stays up but must stop
     * accepting writes -- the acceptor NACKs -- so a quorum can still form among healthy peers.
     * One-way: nothing short of a restart clears it.
     */
    default boolean isDegraded() {
        return false;
    }

    /** Why {@link #isDegraded()} is true, or {@code null} when it is not. */
    default String degradedReason() {
        return null;
    }

    /** The latest valid snapshot, or {@code null} when there is none. */
    SnapshotReader openSnapshot();

    /** Replay the entries written after the latest snapshot, or all of them when there is none. */
    void replayTail(Consumer<Entry> consumer);

    /**
     * Identity of this run of the durable state: born when the storage is first initialized, gone
     * with it.
     * <p>
     * On the {@link Wal} because the incarnation identifies the state, and this is what owns the
     * state. A memory-backed implementation is a new incarnation on every construction, which is
     * not a degenerate answer but the true one -- it has forgotten everything.
     */
    IncarnationId incarnation();

    /**
     * Whether this storage can prove a promise ceiling at least as high as any it ever held.
     *
     * <p><b>Established by {@link #replayTail}, and only valid once it has run</b> -- the property is
     * continuity of the log, and continuity is a fact about the records, not about the files. A
     * ceiling raise is forced before it is believed, so a log that runs unbroken from the snapshot
     * (or from the beginning) to the end contains every raise this storage ever made. A log with a
     * hole in it, a log whose snapshot was deleted from under its compacted start, and a storage
     * that has recorded nothing at all are all the same answer: {@code false}. None of them is an
     * error -- a node that cannot prove a ceiling gets a floor from the cluster before it serves.
     *
     * @throws IllegalStateException if asked before the replay that answers it
     */
    boolean ceilingIsProven();

    SnapshotWriter beginSnapshot();

    void truncateBeforeSnapshot();

    interface SnapshotReader extends AutoCloseable {
        long totalEntries();

        boolean hasNext();

        SnapshotEntry next();

        /** The reservations in force when this snapshot was committed. */
        Reservations reservations();

        @Override
        void close();
    }

    interface SnapshotWriter {
        void write(SnapshotEntry entry);

        /**
         * Commit the snapshot, recording the {@link Reservations} in force, so recovery starts from
         * the correct bounds even when the entire WAL tail is truncated away.
         */
        void commit(Reservations reservations);

        /**
         * Abort an in-progress snapshot write
         * Must be a no-op if {@link #commit(Reservations)} has already succeeded
         */
        void abort();
    }

    /**
     * The durable counters a snapshot has to carry alongside the key states, because they are not
     * derivable from the keys and the WAL tail that held them is about to be truncated.
     * <p>
     * A value type rather than two {@code long} parameters on purpose: they are the same primitive
     * with opposite meanings -- one bounds what this node may <em>issue</em> as a proposer, the other
     * what it may <em>promise</em> as an acceptor -- and getting them the wrong way round would
     * silently lower a safety bound rather than fail to compile.
     *
     * @see Entry.BallotBump
     * @see Entry.PromiseCeiling
     */
    class Reservations {
        /** Neither bound set: a fresh store, or a snapshot written before either existed. */
        public static final Reservations NONE = new Reservations(0L, 0L);

        private final long proposerBallot;
        private final long promiseCeiling;

        public Reservations(final long proposerBallot, final long promiseCeiling) {
            this.proposerBallot = proposerBallot;
            this.promiseCeiling = promiseCeiling;
        }

        /** Highest ballot counter the local proposer had reserved the right to issue. */
        public long proposerBallot() {
            return proposerBallot;
        }

        /** Highest ballot counter the local acceptor had reserved the right to promise. */
        public long promiseCeiling() {
            return promiseCeiling;
        }

        @Override
        public String toString() {
            return "Reservations[proposerBallot=" + proposerBallot
                    + ", promiseCeiling=" + promiseCeiling + ']';
        }
    }

    class SnapshotEntry {
        private final HashedBytes key;
        private final Ballot promised;
        private final Ballot accepted;
        private final HashedBytes value;
        private final boolean tombstone;

        public SnapshotEntry(final HashedBytes key,
                             final Ballot promised,
                             final Ballot accepted,
                             final HashedBytes value,
                             final boolean tombstone) {
            this.key = key;
            this.promised = promised;
            this.accepted = accepted;
            this.value = value;
            this.tombstone = tombstone;
        }

        public HashedBytes key() {
            return key;
        }

        public Ballot promised() {
            return promised;
        }

        public Ballot accepted() {
            return accepted;
        }

        public HashedBytes value() {
            return value;
        }

        public boolean tombstone() {
            return tombstone;
        }
    }
}
