/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.ByteBuffers;
import io.github.green4j.discas.common.KvLimits;
import io.github.green4j.discas.common.client.ClientMessage;
import io.github.green4j.discas.node.wal.Wal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * The node's in-memory projection of its log: every key's promised and accepted ballots, the value
 * or tombstone behind them, and the indexes anti-entropy and scans read.
 *
 * <h2>A deleted key stays until the cluster agrees it may go</h2>
 * A delete writes a tombstone rather than removing the key, because the tombstone is what stops
 * anti-entropy from copying the old value back off a replica that has not seen the delete yet.
 * Removing one locally would let the key flap between absent and tombstoned forever.
 * <p>
 * {@link #purge} is the only way a key leaves this map, and it applies a decision the cluster has
 * already reached: every member holds the tombstone durably, or holds nothing for the key at all.
 * The per-key cost until then is ~280 bytes -- the map entry, the {@link HashedBytes} wrapper, the
 * {@link KeyState} and its ballots, and the secondary index references.
 */
public final class LocalStore {
    public static final int NUM_RANGES = 256;
    public static final long MIN_PROMISE_AGE_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final Map<HashedBytes, KeyState> states = new HashMap<>();
    private final Wal wal;
    private final NodeObserver observer;
    private boolean walDegradedReported = false;
    private final Set<HashedBytes> recentlyTouched = new HashSet<>();

    /**
     * Range number to the keys in that range, in ascending byte order. Maintained on every key
     * creation, which is what keeps the digest paths off an O(N) scan. Sorted rather than hashed
     * because anti-entropy pages through a range by key -- a range's key list is otherwise an
     * unbounded peer message -- and both digest paths need the keys in order anyway.
     */
    private final Map<Integer, NavigableSet<HashedBytes>> keysByRange = new HashMap<>();

    /**
     * All known keys in ascending byte order -- the index that makes a bounded, resumable
     * {@link #scanLocal} possible.
     * <p>
     * A secondary index rather than a sorted {@link #states}, which is on the consensus hot path
     * where a hash map's O(1) probe beats a tree's byte-by-byte comparisons. This set is touched
     * only when a key is created or evicted, so writes to an existing key pay nothing.
     */
    private final NavigableSet<HashedBytes> sortedKeys = new TreeSet<>();

    /**
     * Every key this store holds a tombstone for, in the order the tombstones were written -- so the
     * first element is the one that has been left alone longest, and is the sweeper's only candidate
     * ({@link #tombstoneCandidate}).
     * <p>
     * The index makes <em>nothing to collect</em> -- the common case, since most stores hold no
     * tombstones -- one lookup rather than a walk of every key. Age order comes free with the
     * insertion order a {@link LinkedHashSet} keeps, given the one rule {@link #trackTombstone}
     * follows: a tombstone written again goes to the back, because a key that was just written is
     * not a key that has been left alone.
     */
    private final Set<HashedBytes> tombstones = new LinkedHashSet<>();

    /** Range number to the SHA-256 of its sorted key digests; recomputed only when dirty. */
    private final Map<Integer, HashedBytes> rangeDigestCache = new HashMap<>();

    /** Ranges with a key added or modified since the last digest computation. */
    private final Set<Integer> dirtyRanges = new HashSet<>();

    /** Highest ballot counter seen in any promise or accept, acceptor-side. */
    private long maxBallotSeen = 0;

    /**
     * Highest ballot counter the local proposer has durably reserved, through a BallotBump entry
     * and recovered from the log on startup. It must not issue a ballot above this.
     */
    private long reservedProposerBallot = 0;

    /**
     * Highest ballot counter this *acceptor* has durably reserved the right to promise, via a
     * PromiseCeiling entry. Recovered from WAL and snapshot on startup. The acceptor must not
     * promise or accept a ballot with counter > this value.
     */
    private long promisedUpTo = 0;

    /**
     * {@link #promisedUpTo} as it stood when recovery finished -- the floor below which this
     * incarnation refuses to promise, because a promise in that range may have been made by the
     * previous incarnation and lost with an unforced tail.
     * <p>
     * Set once at the end of replay and never advanced by ordinary traffic. Two things advance it,
     * and both are cases of state deliberately not being there: {@link #adoptRecoveryFloor}, for a
     * node that replayed nothing and takes its floor from the cluster, and {@link #purge}, which
     * forgets a key's promise on purpose and has to leave the bound behind.
     */
    private long recoveryPromiseFloor = 0;

    /**
     * Whether {@link #recoveryPromiseFloor} came from the cluster rather than from this node's own
     * log -- which is the same fact as: this node's history has a hole in it, below that floor. Only
     * {@link #adoptRecoveryFloor} sets it, and nothing clears it: the hole does not heal, and state
     * written after it is above the floor anyway.
     */
    private boolean floorAdopted = false;

    /**
     * What one stored key costs beyond its own bytes, rounded up.
     * <p>
     * Payload alone is the wrong number to budget against: on small values the objects around a
     * pair cost several times the pair. Summed for a 64-bit JVM with compressed oops -- the map
     * node and its table slot, the two {@link HashedBytes} wrappers and their array headers, the
     * {@link KeyState} and its two {@link Ballot}s, and an entry in each secondary index -- that is
     * about 340.
     * <p>
     * <b>Held at 512, and the margin is the point.</b> Object padding, allocation rounding, the
     * tombstone index and the recently-touched set are all real and none are in that sum. Being
     * wrong high costs unused budget; being wrong low costs the JVM, which is the failure this
     * check exists to prevent. Charged for tombstones too, which are never refused, so overstating
     * them can only make the node cautious, never stuck.
     */
    static final long ENTRY_FOOTPRINT_BYTES = 512;

    /**
     * The heap this store is estimated to occupy, and the budget it may not pass.
     * <p>
     * An estimate rather than the JVM's live heap, which moves with the collector: the same write
     * would be refused before a GC and taken after one, and two replicas would disagree about one
     * pair. This is a function of what is stored, so every replica holding the same keys computes
     * the same answer. See {@link NodeConfig#storeHeapFraction()}.
     */
    private long storedBytes;
    private final long capacityBytes;

    /** Reported on the flip only, like quorum availability; nothing fires while it merely holds. */
    private boolean capacityAvailable = true;

    public LocalStore(final Wal wal) {
        this(wal, NodeObserver.NONE);
    }

    /** Unbounded: the embedding application has not said what this node's share of the heap is. */
    public LocalStore(final Wal wal, final NodeObserver observer) {
        this(wal, observer, Long.MAX_VALUE);
    }

    public LocalStore(final Wal wal, final NodeObserver observer, final long capacityBytes) {
        this.wal = wal;
        this.observer = observer == null ? NodeObserver.NONE : observer;
        this.capacityBytes = capacityBytes;
    }

    /** The heap this node's stored keys are estimated to occupy. */
    public long storedBytes() {
        return storedBytes;
    }

    /** The most it may hold before it refuses to grow. */
    public long capacityBytes() {
        return capacityBytes;
    }

    /**
     * Whether this node has room for {@code key} holding {@code value}, counting only what the entry
     * would <em>add</em> -- so overwriting a large value with a small one is never refused, and an
     * overwrite of an existing key costs no {@link #ENTRY_FOOTPRINT_BYTES}, because the objects
     * around it are already there.
     * <p>
     * <b>A tombstone always passes.</b> A store that cannot take a delete when it is full is a store
     * with no way back, and the tombstone that replaces a value is never larger than the value it
     * replaces.
     * <p>
     * Reports the flip through {@link NodeObserver#storeCapacity} rather than each call: an operator
     * needs to know that this node has started refusing and when it stopped, and the rate of
     * refusals is a counter's job.
     */
    public boolean hasCapacityFor(final HashedBytes key, final HashedBytes value,
                                  final boolean tombstone) {
        final boolean room = tombstone
                || storedBytes + delta(key, value) <= capacityBytes;
        if (room != capacityAvailable && !tombstone) {
            capacityAvailable = room;
            observer.storeCapacity(room, storedBytes, capacityBytes);
        }
        return room;
    }

    /**
     * Applies a change in stored size made by <b>replay</b>, and refuses to go past the budget.
     * <p>
     * The one place the store may throw over capacity rather than answer {@code false}. A serving
     * node has somewhere to put a refusal -- the client is told, the proposer is NACKed -- but replay
     * has no caller to refuse: reading the next record is not a request, it is this node recovering
     * what it already owns. Carrying on regardless is how a member gets killed by the JVM halfway
     * through starting, so it stops here and the node enters {@code FAILED} with a stated reason.
     *
     * @throws StoreCapacityExceededException if the log holds more than this node can carry
     */
    private void replayStoredBytes(final long delta) {
        storedBytes += delta;
        if (storedBytes > capacityBytes) {
            throw new StoreCapacityExceededException(storedBytes, capacityBytes);
        }
    }

    /** What writing {@code value} under {@code key} would add to {@link #storedBytes}. */
    private long delta(final HashedBytes key, final HashedBytes value) {
        return pairBytes(key, value) - pairBytes(key, states.get(key));
    }

    /**
     * A key's contribution, which is nothing until it has an accepted state: a key that exists only
     * as a promise holds no pair, and is evicted rather than stored.
     */
    private static long pairBytes(final HashedBytes key, final KeyState state) {
        if (state == null || state.accepted.isZero()) {
            return 0L;
        }
        return pairBytes(key, state.value);
    }

    private static long pairBytes(final HashedBytes key, final HashedBytes value) {
        return ENTRY_FOOTPRINT_BYTES + key.size() + (value == null ? 0L : value.size());
    }

    /**
     * Reports the WAL's transition into its degraded state through the observer
     * exactly once. Called from every write path (which no-ops when degraded), so an
     * operator learns of the failure through the observability seam rather than only
     * the WAL layer's own diagnostic.
     */
    private void reportWalDegradedOnce() {
        if (!walDegradedReported && wal.isDegraded()) {
            walDegradedReported = true;
            observer.walDegraded(wal.degradedReason());
        }
    }

    /**
     * The two sources a recovery reads, in the order it reads them. One field rather than a pair of
     * flags, whose fourth combination -- the tail read before the snapshot -- cannot occur.
     */
    private enum Phase { SNAPSHOT, TAIL, DONE }

    public ThrottledLoader beginRecovery() {
        return new ThrottledLoader();
    }

    public final class ThrottledLoader {
        private Wal.SnapshotReader snapshotReader;
        private Phase phase = Phase.SNAPSHOT;
        private long appliedEntries = 0;

        ThrottledLoader() {
            this.snapshotReader = wal.openSnapshot();
            if (snapshotReader == null) {
                phase = Phase.TAIL;
            } else {
                final Wal.Reservations reservations = snapshotReader.reservations();
                if (reservations.proposerBallot() > reservedProposerBallot) {
                    reservedProposerBallot = reservations.proposerBallot();
                }
                if (reservations.promiseCeiling() > promisedUpTo) {
                    promisedUpTo = reservations.promiseCeiling();
                }
            }
        }

        public long snapshotSize() {
            return snapshotReader != null ? snapshotReader.totalEntries() : 0;
        }

        /** Snapshot entries applied so far -- what a recovery in progress can report about itself. */
        public long appliedEntries() {
            return appliedEntries;
        }

        public boolean loadBatch(final int batchSize) {
            if (phase == Phase.SNAPSHOT) {
                try {
                    int count = 0;
                    while (count < batchSize && snapshotReader.hasNext()) {
                        final Wal.SnapshotEntry snapshotEntry = snapshotReader.next();
                        applySnapshot(snapshotEntry);
                        appliedEntries++;
                        count++;
                    }
                    if (!snapshotReader.hasNext()) {
                        closeSnapshotReader();
                        phase = Phase.TAIL;
                    }
                } catch (final RuntimeException e) {
                    closeSnapshotReader();
                    throw e;
                }
                return true;
            }

            if (phase == Phase.TAIL) {
                wal.replayTail(this::applyWALEntry);
                phase = Phase.DONE;
                // Everything durable has now been seen, so this is the highest counter any previous
                // incarnation could have promised. Fixed here and never advanced by traffic: it is a
                // statement about what was forgotten, not a running bound. See reservePromiseCeiling.
                //
                // A max rather than an assignment because purges replayed above have already put
                // their dropped promises here: the ceiling covers them (a promise is at or below the
                // ceiling forced for it), and the floor is not something that may go backwards.
                if (promisedUpTo > recoveryPromiseFloor) {
                    recoveryPromiseFloor = promisedUpTo;
                }
                return false;
            }

            return false;
        }

        private void closeSnapshotReader() {
            if (snapshotReader == null) {
                return;
            }
            try {
                snapshotReader.close();
            } finally {
                snapshotReader = null;
            }
        }

        public boolean isDone() {
            return phase == Phase.DONE;
        }

        private void applySnapshot(final Wal.SnapshotEntry snapshotEntry) {
            final KeyState keyState = new KeyState(
                    snapshotEntry.promised(), snapshotEntry.accepted(), snapshotEntry.value(),
                            snapshotEntry.tombstone());
            states.put(snapshotEntry.key(), keyState);
            replayStoredBytes(pairBytes(snapshotEntry.key(), keyState));
            trackTombstone(snapshotEntry.key(), keyState);
            // Maintain secondary index for incremental Merkle digests
            final int range = snapshotEntry.key().rangeOf(NUM_RANGES);
            keysByRange.computeIfAbsent(range, rangeNumber -> new TreeSet<>()).add(snapshotEntry.key());
            sortedKeys.add(snapshotEntry.key());
            dirtyRanges.add(range);
            trackBallot(snapshotEntry.promised());
            trackBallot(snapshotEntry.accepted());
        }

        private void applyWALEntry(final Wal.Entry entry) {
            if (entry instanceof Wal.Entry.Promise) {
                final Wal.Entry.Promise promiseEntry = (Wal.Entry.Promise) entry;
                final KeyState keyState = getOrCreate(promiseEntry.key());
                if (promiseEntry.ballot().compareTo(keyState.promised) > 0) {
                    keyState.promised = promiseEntry.ballot();
                }
                trackBallot(promiseEntry.ballot());
                return;
            }

            if (entry instanceof Wal.Entry.Accept) {
                final Wal.Entry.Accept acceptEntry = (Wal.Entry.Accept) entry;
                final KeyState keyState = getOrCreate(acceptEntry.key());
                if (acceptEntry.ballot().compareTo(keyState.accepted) > 0) {
                    // Accounted here as well as in writeAccept, and it has to be: replay does not
                    // go through that method, so a node that restarted would come back reporting an
                    // empty store and admit writes until something overwrote every key.
                    final long before = pairBytes(acceptEntry.key(), keyState);
                    keyState.accepted = acceptEntry.ballot();
                    keyState.value = acceptEntry.value();
                    keyState.tombstone = acceptEntry.tombstone();
                    replayStoredBytes(pairBytes(acceptEntry.key(), keyState) - before);
                    trackTombstone(acceptEntry.key(), keyState);
                    // State changed; mark range dirty for digest recomputation
                    dirtyRanges.add(acceptEntry.key().rangeOf(NUM_RANGES));
                }
                if (acceptEntry.ballot().compareTo(keyState.promised) > 0) {
                    keyState.promised = acceptEntry.ballot();
                }
                trackBallot(acceptEntry.ballot());
                return;
            }

            if (entry instanceof Wal.Entry.Purge) {
                final Wal.Entry.Purge purgeEntry = (Wal.Entry.Purge) entry;
                applyPurge(purgeEntry.key(), purgeEntry.ballot());
                return;
            }

            if (entry instanceof Wal.Entry.PromiseCeiling) {
                final Wal.Entry.PromiseCeiling ceiling = (Wal.Entry.PromiseCeiling) entry;
                if (ceiling.promisedUpTo() > promisedUpTo) {
                    promisedUpTo = ceiling.promisedUpTo();
                }
                return;
            }

            if (entry instanceof Wal.Entry.BallotBump) {
                final Wal.Entry.BallotBump ballotBump = (Wal.Entry.BallotBump) entry;
                if (ballotBump.reservedUpTo() > reservedProposerBallot) {
                    reservedProposerBallot = ballotBump.reservedUpTo();
                }
            }
        }
    }

    public ThrottledSnapshot beginSnapshot() {
        return new ThrottledSnapshot();
    }

    /**
     * A <b>fuzzy</b> snapshot: written in batches that yield to the event loop between them, so it
     * is not a point-in-time view and different keys may reflect different logical times.
     * <p>
     * That is safe because the snapshot records the WAL position it started at, every mutation
     * after that position stays in the tail, and recovery loads the snapshot and then replays the
     * tail, where a later ballot overrides stale snapshot state. The tail is truncated only after
     * the snapshot commits.
     * <p>
     * The alternative -- copying all state up front -- would stall the event loop for as long as
     * there are keys. The cost here is a slightly longer tail, since mutations during the write are
     * not compacted until the next snapshot.
     */
    public final class ThrottledSnapshot {
        private final Wal.SnapshotWriter writer;
        private final Wal.Reservations snapshottedReservations;
        private int currentRange = 0;
        private Iterator<HashedBytes> currentRangeKeyIterator = null;
        private boolean committed = false;

        ThrottledSnapshot() {
            // O(1): no data copy, just open writer (records WAL position)
            this.writer = wal.beginSnapshot();
            // Captured at open, like the key set: what the snapshot describes is the state as of
            // this point, and both bounds are part of that state.
            this.snapshottedReservations = new Wal.Reservations(reservedProposerBallot, promisedUpTo);
        }

        /**
         * Write up to {@code batchSize} entries, range by range, and return whether more work
         * remains. Mutations between batches are safe: they are in the WAL tail.
         */
        public boolean writeBatch(final int batchSize) {
            int written = 0;
            while (written < batchSize) {
                if (currentRangeKeyIterator == null || !currentRangeKeyIterator.hasNext()) {
                    if (!advanceToNextRange()) {
                        if (!committed) {
                            writer.commit(snapshottedReservations);
                            committed = true;
                            wal.truncateBeforeSnapshot();
                        }
                        return false;
                    }
                }

                final HashedBytes key = currentRangeKeyIterator.next();
                final KeyState keyState = states.get(key);
                // Exclude promised-only keys from snapshots
                // handlePrepare materializes a KeyState for every prepared key,
                // but keys that were never accepted are transient metadata with
                // no committed value. Persisting them wastes disk/memory and
                // causes them to survive restarts indefinitely. The same
                // accepted.isZero() guard is already used in keyDigestsForRange,
                // scanLocal, and recomputeRangeDigest
                if (keyState != null && !keyState.accepted.isZero()) {
                    writer.write(new Wal.SnapshotEntry(
                            key, keyState.promised, keyState.accepted, keyState.value, keyState.tombstone));
                    written++;
                }
            }
            return true;
        }

        public boolean isCommitted() {
            return committed;
        }

        /** Copies the range's key set, so a key added between batches cannot break iteration. */
        private boolean advanceToNextRange() {
            while (currentRange < NUM_RANGES) {
                final NavigableSet<HashedBytes> keys = keysByRange.get(currentRange);
                currentRange++;
                if (keys != null && !keys.isEmpty()) {
                    // Small copy: ~K/256 references. Prevents CME if keys are
                    // added to this range between batches
                    currentRangeKeyIterator = new ArrayList<>(keys).iterator();
                    return true;
                }
            }
            return false;
        }

        public void abort() {
            if (committed) {
                return;
            }
            writer.abort();
        }
    }

    /**
     * On first creation the key is registered in the secondary indexes and its range marked dirty
     * for digest recomputation.
     */
    KeyState getOrCreate(final HashedBytes key) {
        return states.computeIfAbsent(key, keyBytes -> {
            final int range = keyBytes.rangeOf(NUM_RANGES);
            keysByRange.computeIfAbsent(range, rangeNumber -> new TreeSet<>()).add(keyBytes);
            sortedKeys.add(keyBytes);
            dirtyRanges.add(range);
            return new KeyState();
        });
    }

    public KeyState get(final HashedBytes key) {
        return states.get(key);
    }

    public int keyCount() {
        return states.size();
    }

    boolean isWalDegraded() {
        return wal.isDegraded();
    }

    public void writePromise(final HashedBytes key, final Ballot ballot) {
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return;
        }
        if (!wal.append(new Wal.Entry.Promise(key, ballot))) {
            // The log refused the entry (it has just degraded). Applying it anyway would leave
            // this node serving a promise that no replay can reproduce.
            reportWalDegradedOnce();
            return;
        }
        final KeyState ks = getOrCreate(key);
        if (ballot.compareTo(ks.promised) > 0) {
            ks.promised = ballot;
            ks.promisedAtNanos = System.nanoTime();
        }
        trackBallot(ballot);
    }

    public void writeAccept(final HashedBytes key, final Ballot ballot,
                            final HashedBytes value, final boolean tombstone) {
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return;
        }
        if (!wal.append(new Wal.Entry.Accept(key, ballot, value, tombstone))) {
            // Same as writePromise: an accept that never entered the log must not be visible in
            // memory. It would otherwise be served to serializable reads and scans, and offered
            // to peers as a range digest, despite vanishing on the next restart.
            reportWalDegradedOnce();
            return;
        }
        final KeyState keyState = getOrCreate(key);
        if (ballot.compareTo(keyState.promised) > 0) {
            keyState.promised = ballot;
        }
        if (ballot.compareTo(keyState.accepted) > 0) {
            // Measured across the mutation rather than added to: an accept replaces whatever was
            // there, so what it costs is the difference, and an overwrite that shrinks the store
            // has to be seen to shrink it.
            final long before = pairBytes(key, keyState);
            keyState.accepted = ballot;
            keyState.value = value;
            keyState.tombstone = tombstone;
            storedBytes += pairBytes(key, keyState) - before;
            trackTombstone(key, keyState);
        }
        recentlyTouched.add(key);
        trackBallot(ballot);
        dirtyRanges.add(key.rangeOf(NUM_RANGES));
    }

    /**
     * Collect a tombstone the cluster has agreed no replica can resurrect: the key leaves this
     * store, its indexes and its range digest.
     * <p>
     * Not forced, unlike {@link #reservePromiseCeiling}: a purge lost with an unforced tail leaves
     * the tombstone in place, which is the state it was suppressing anyway, and the next sweep asks
     * again. There is nothing here a crash can turn into a resurrection.
     *
     * @param tombstoneBallot the ballot the purged tombstone was accepted at; the decision is about
     *                        that state and no other
     * @return whether the key was dropped. {@code false} means this node holds something the
     *         decision was not about -- nothing at all, or a state written above that ballot -- and
     *         nothing was logged
     */
    public boolean purge(final HashedBytes key, final Ballot tombstoneBallot) {
        if (!purgeApplies(key, tombstoneBallot)) {
            return false;
        }
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return false;
        }
        if (!wal.append(new Wal.Entry.Purge(key, tombstoneBallot))) {
            // Same rule as every other write path: memory must not lose a key the log will replay.
            reportWalDegradedOnce();
            return false;
        }
        return applyPurge(key, tombstoneBallot);
    }

    /**
     * Answer whether this node can still resurrect the value a tombstone suppresses -- the question
     * every member is asked before any of them collects.
     * <p>
     * {@link PurgeAnswer#HELD} is a claim about the disk, so the log is forced before it is made: a
     * tombstone is acknowledged before the log is forced, and a node answering off its in-memory
     * state and then losing power comes back holding the <em>value</em>, with no tombstone left
     * anywhere to out-vote it. A force this node cannot complete leaves it unable to make the claim,
     * which blocks the collection rather than risking it -- the refusal is free, the collection is
     * not undoable.
     *
     * @param tombstoneBallot the ballot the tombstone was accepted at; a tombstone at or above it
     *                        suppresses the same value, one below it does not
     */
    public PurgeAnswer purgeCheck(final HashedBytes key, final Ballot tombstoneBallot) {
        final KeyState keyState = states.get(key);
        if (keyState == null) {
            // Nothing to resurrect. Distinct from RETAINED, and deliberately: a member whose storage
            // was replaced answers this, and calling it a refusal would block every collection in
            // the cluster for as long as that member lives.
            return PurgeAnswer.ABSENT;
        }
        if (!keyState.tombstone || keyState.accepted.compareTo(tombstoneBallot) < 0) {
            return PurgeAnswer.RETAINED;
        }
        wal.force();
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return PurgeAnswer.RETAINED;
        }
        return PurgeAnswer.HELD;
    }

    /**
     * Whether the state this node holds for {@code key} is the state the purge decided about.
     * <p>
     * The whole condition, and the reason a purge is safe to replay: a key written again above the
     * tombstone's ballot is a key the decision did not cover, so it keeps what it has. Everything at
     * or below that ballot is what the tombstone already suppresses.
     */
    private boolean purgeApplies(final HashedBytes key, final Ballot tombstoneBallot) {
        final KeyState keyState = states.get(key);
        return keyState != null && keyState.accepted.compareTo(tombstoneBallot) <= 0;
    }

    private boolean applyPurge(final HashedBytes key, final Ballot tombstoneBallot) {
        if (!purgeApplies(key, tombstoneBallot)) {
            return false;
        }
        final KeyState dropped = states.remove(key);
        storedBytes -= pairBytes(key, dropped);
        dropIndexEntries(key);
        // The key contributed to its range's digest, so the digest has to be recomputed without it --
        // otherwise a purged replica still advertises the tombstone it no longer holds.
        dirtyRanges.add(key.rangeOf(NUM_RANGES));
        forgetPromise(dropped.promised);
        return true;
    }

    /**
     * Hand a promise this store is about to drop over to the recovery floor.
     * <p>
     * The floor's one job is to refuse ballots this node may have promised and can no longer
     * produce, and it makes no difference how they went missing: an unforced tail, a disk replaced
     * under the marker, a purged key or an evicted promise-only state. Every path that drops a
     * {@link KeyState} comes through here, so an accept delayed since before the drop meets the
     * bound instead of a zero promise and a fresh, empty state.
     * <p>
     * Durable without a record of its own: a ballot this acceptor promised is at or below the
     * ceiling it forced before promising it, and replay derives the floor from that ceiling.
     */
    private void forgetPromise(final Ballot promised) {
        if (promised.counter() > recoveryPromiseFloor) {
            recoveryPromiseFloor = promised.counter();
        }
    }

    /**
     * Keep the tombstone index in step with a key's accepted state. The one rule, applied wherever
     * an accepted state lands -- a live write, replay, a snapshot: <b>a key is a tombstone candidate
     * from the moment it was last written as a tombstone, and from no earlier moment.</b>
     * <p>
     * So a tombstone written again -- by a repair round re-replicating it, or by a delete of a key
     * that was deleted before -- goes to the back of the queue, and a key written back to a value
     * leaves it. Removing before adding is what makes that true: {@link LinkedHashSet#add} on an
     * element already present keeps its original position.
     * <p>
     * Order, and no clock. What the order is for is fairness between candidates, and it survives a
     * restart because replay writes in log order; a tombstone's <em>age</em> would not survive one,
     * and nothing here needs it -- what makes collection safe is the all-members condition, so a
     * sweep of a tombstone written a moment ago may be blocked, never wrong.
     */
    private void trackTombstone(final HashedBytes key, final KeyState state) {
        tombstones.remove(key);
        if (state.tombstone) {
            tombstones.add(key);
        }
    }

    /**
     * The oldest tombstone this node is responsible for sweeping -- the sweeper's candidate, and the
     * only one it is offered.
     * <p>
     * Costs one lookup when there is nothing to collect at all, which is the case worth keeping
     * cheap: most stores hold no tombstones. Otherwise it walks the queue from the oldest end until
     * it reaches one {@code owned} accepts, which is about {@code N} steps -- the queue is every
     * member's tombstones and this node owns roughly one in {@code N} of them.
     * <p>
     * Unbounded rather than giving up after a fixed number of steps. A bound would
     * turn "this node owns nothing near the head" into <em>nothing to collect</em>, which reads
     * exactly like a healthy empty store and would stop collection with no fault to point at. The
     * walk is the honest version: its worst case is a store where this node owns nothing, and that
     * walk ends at the end of the queue rather than at a wrong answer.
     * <p>
     * <b>Ownership orders the queue; it does not shorten it.</b> A node that owns none of the
     * tombstones it holds offers its oldest anyway, and that fallback is load-bearing rather than
     * tidy: a member that missed a {@code PURGE} keeps a tombstone the rest of the cluster has
     * dropped, and the only thing that ever collects it is <em>that member</em> offering it again --
     * the owner cannot, having already dropped the key and with nothing left in its own queue to
     * offer. Excluding foreign keys outright leaves such a tombstone in place forever, which is the
     * self-healing half of an unacknowledged broadcast (TOMBSTONE_GC.md item 3) quietly removed.
     * The duplicate sweep it costs in that case is idempotent and rare, and the alternative is
     * garbage that never goes.
     * <p>
     * A candidate that cannot be collected stays the candidate until it can, rather than being
     * rotated past. That is deliberate: a member blocking one key is a member an operator may need
     * to act on (item 7), and sweeping around it would hide exactly that. The one blocker that is
     * <em>not</em> a member needing attention -- a replica that is merely behind -- ends when
     * anti-entropy repairs the key, which rewrites the tombstone and sends it to the back of the
     * queue by the rule above.
     *
     * @param owned which keys this node may offer -- see {@code SweepAffinity}
     * @return a key this store holds a tombstone for and {@code owned} accepts, or {@code null}
     *         when there is no such key
     */
    public HashedBytes tombstoneCandidate(final Predicate<HashedBytes> owned) {
        HashedBytes oldest = null;
        for (final HashedBytes key : tombstones) {
            if (owned.test(key)) {
                return key;
            }
            if (oldest == null) {
                oldest = key;
            }
        }
        return oldest;
    }

    /** How many keys this store holds a tombstone for: key space that cannot shrink until they go. */
    public int tombstoneCount() {
        return tombstones.size();
    }

    /** Remove a key from every secondary index; the caller owns {@link #states}. */
    private void dropIndexEntries(final HashedBytes key) {
        tombstones.remove(key);
        final int range = key.rangeOf(NUM_RANGES);
        final NavigableSet<HashedBytes> rangeKeys = keysByRange.get(range);
        if (rangeKeys != null) {
            rangeKeys.remove(key);
            if (rangeKeys.isEmpty()) {
                keysByRange.remove(range);
            }
        }
        sortedKeys.remove(key);
    }

    public void reserveProposerBallot(final long upTo) {
        if (upTo <= reservedProposerBallot) {
            return;
        }
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return;
        }
        if (!wal.append(new Wal.Entry.BallotBump(upTo))) {
            // Believing in an unlogged reservation is the dangerous case: after a restart the
            // reservation is gone, and the proposer could reissue a ballot counter it had already
            // handed out.
            reportWalDegradedOnce();
            return;
        }
        reservedProposerBallot = upTo;
    }

    public long reservedProposerBallot() {
        return reservedProposerBallot;
    }

    /**
     * Reserve the right to promise ballot counters up to {@code upTo}, durably.
     * <p>
     * Unlike {@link #reserveProposerBallot} this <b>forces the log</b> before returning. The
     * proposer reservation may lag a crash harmlessly -- a lost reservation makes the proposer
     * re-reserve, and it never lowers a bound anyone relied on. This one is the bound the acceptor's
     * post-recovery floor is derived from, so an unforced reservation would leave exactly the gap it
     * exists to close: promises acknowledged inside the sync window, with no record that they could
     * have been made. This is the one sync on the promise path, and it is paid once per
     * {@code CHUNK} of ballot space rather than once per promise.
     *
     * @return whether the reservation is durable. {@code false} means the caller must not promise
     *         above the previous ceiling -- the refusal is safe, believing an unlogged reservation
     *         is not.
     */
    public boolean reservePromiseCeiling(final long upTo) {
        if (upTo <= promisedUpTo) {
            return true;
        }
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return false;
        }
        if (!wal.append(new Wal.Entry.PromiseCeiling(upTo))) {
            reportWalDegradedOnce();
            return false;
        }
        wal.force();
        if (wal.isDegraded()) {
            // The force failed, so the record may not have reached the platter. Refusing to raise
            // the in-memory ceiling keeps the two in step: a ceiling this node believes but cannot
            // reproduce is the whole hazard.
            reportWalDegradedOnce();
            return false;
        }
        promisedUpTo = upTo;
        return true;
    }

    /** The durable ceiling on what this acceptor may promise. */
    public long promisedUpTo() {
        return promisedUpTo;
    }

    /**
     * Adopt a floor obtained from the cluster, for a node that started with no state of its own.
     * <p>
     * Written to the log before it is believed, and by the same forced path as any other ceiling:
     * the floor has to survive a crash a second after adopting it, or the next start would ask the
     * cluster again -- and by then this node may have been the peer that answered, having promised
     * above what it can prove. Persisting it also makes this a one-time cost: a restart replays the
     * ceiling like any other and needs no round trip.
     * <p>
     * The entry is appended even when {@code floor} is zero, which {@link #reservePromiseCeiling}
     * would skip as a no-op: a cluster starting from nothing legitimately adopts zero, and leaving
     * its log empty would make the next start look like a node whose state was removed under its
     * marker.
     *
     * @return whether the floor is durable and in force. {@code false} leaves the node without one,
     *         which must keep it out of the quorum -- see {@code CeilingRecovery}.
     */
    public boolean adoptRecoveryFloor(final long floor) {
        if (floor < recoveryPromiseFloor) {
            throw new IllegalArgumentException("Recovery floor must not go backwards: "
                    + floor + " < " + recoveryPromiseFloor);
        }
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return false;
        }
        if (!wal.append(new Wal.Entry.PromiseCeiling(floor))) {
            reportWalDegradedOnce();
            return false;
        }
        wal.force();
        if (wal.isDegraded()) {
            reportWalDegradedOnce();
            return false;
        }
        if (floor > promisedUpTo) {
            promisedUpTo = floor;
        }
        recoveryPromiseFloor = floor;
        floorAdopted = true;
        return true;
    }

    /**
     * Drop a key this node cannot account for and no peer confirms.
     * <p>
     * The rule, and it is not about tombstones: <b>state I cannot account for, that no peer holds,
     * is not state.</b> A node that had to take its floor from the cluster has a hole in its
     * history, and everything at or below that floor is on the wrong side of it. If such a value had
     * ever been chosen, a quorum accepted it and a peer would still hold it; if no peer holds it, it
     * was never chosen and nothing was ever promised about it.
     * <p>
     * Without this, the same node <em>pushes</em> what it kept: anti-entropy sees a key no peer has,
     * calls it divergence, and a repair round adopts it as the highest accepted state -- which for a
     * hole that swallowed a tombstone's accept and kept the value's is the deleted value coming
     * back everywhere, with nothing left to out-vote it.
     * <p>
     * The one assumption, stated because it is the boundary rather than a hole in the argument: the
     * peers that answered still hold what they accepted. A cluster where a second member was wiped
     * <em>and</em> this one has a hole is two storage failures over one key, which no rule here
     * covers -- and the alternative to this one revives deleted data on a single failure.
     *
     * @return whether the key was dropped; {@code false} when this node can account for itself, does
     *         not hold the key, or holds it above the floor, where it is ordinary state
     */
    public boolean dropUnaccountedFor(final HashedBytes key) {
        if (!floorAdopted) {
            return false;
        }
        final KeyState state = states.get(key);
        if (state == null || state.accepted.counter() > recoveryPromiseFloor) {
            return false;
        }
        // Through purge, because it is the same act: the key leaves memory, both indexes and the
        // range digest, its promise becomes the floor, and a record says so, so replay does not
        // bring it back.
        return purge(key, state.accepted);
    }

    /**
     * The floor this acceptor will not promise below, fixed at recovery.
     * <p>
     * Every promise that ever existed had a counter at or below the replayed {@link #promisedUpTo},
     * so a promise lost with an unforced tail is still covered by refusing that whole range. A
     * snapshot rather than the live ceiling, deliberately: see {@link #reservePromiseCeiling}.
     */
    public long recoveryPromiseFloor() {
        return recoveryPromiseFloor;
    }

    /**
     * Returns a snapshot of range digests. Recomputes only dirty ranges
     * (those modified since last call). Thread-safe if called only from
     * the event loop (which is the case)
     */
    public Map<Integer, HashedBytes> computeRangeDigests() {
        if (!dirtyRanges.isEmpty()) {
            for (final int range : dirtyRanges) {
                recomputeRangeDigest(range);
            }
            dirtyRanges.clear();
        }
        // Return a defensive copy (passed into messages and anti-entropy)
        return new HashMap<>(rangeDigestCache);
    }

    /**
     * One bounded page of a range's key digests, in ascending key order, read off the secondary
     * index rather than by scanning every key.
     * <p>
     * Anti-entropy compares whole ranges, and a range's key list is otherwise an unbounded peer
     * message: a dense range could exceed the peer transport's frame and inflight budgets and get
     * the connection torn down by its own backpressure. Paging keeps each {@code KeysResp} inside
     * those budgets regardless of how many keys a range holds.
     *
     * @param range      the range index
     * @param startAfter exclusive lower bound, or {@code null} to start at the range's first key
     * @param limit      maximum digests to return; must be positive
     */
    public KeyDigestPage keyDigestsForRange(final int range,
                                            final ByteBuffer startAfter,
                                            final int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("Key digest limit must be >= 1, got " + limit);
        }
        final NavigableSet<HashedBytes> keysInRange = keysByRange.get(range);
        if (keysInRange == null || keysInRange.isEmpty()) {
            return new KeyDigestPage(Collections.emptyList(), false);
        }
        // The cursor is only ever compared, so it is wrapped rather than hashed, and once per
        // page rather than once per key.
        final NavigableSet<HashedBytes> tail = startAfter == null
                ? keysInRange
                : keysInRange.tailSet(HashedBytes.viewOf(startAfter), false);

        final int effectiveLimit = Math.min(limit, KvLimits.MAX_ANTI_ENTROPY_KEYS_PER_PAGE);
        final List<KeyDigest> page = new ArrayList<>(Math.min(effectiveLimit, 64));
        boolean hasMore = false;
        long pageBytes = 0;
        for (final HashedBytes key : tail) {
            final KeyState keyState = states.get(key);
            if (keyState == null || keyState.accepted.isZero()) {
                continue;
            }
            // Same two bounds as a client scan page: a count cap alone would still allow a huge
            // page if the keys are large.
            if (page.size() == effectiveLimit
                    || (!page.isEmpty()
                        && pageBytes + key.size() > KvLimits.MAX_ANTI_ENTROPY_PAGE_BYTES)) {
                hasMore = true;
                break;
            }
            page.add(keyState.digest(key));
            pageBytes += key.size();
        }
        return new KeyDigestPage(page, hasMore);
    }

    /** A bounded slice of a range's key digests plus whether more keys follow it. */
    public static final class KeyDigestPage {
        private final List<KeyDigest> digests;
        private final boolean hasMore;

        KeyDigestPage(final List<KeyDigest> digests, final boolean hasMore) {
            this.digests = digests;
            this.hasMore = hasMore;
        }

        public List<KeyDigest> digests() {
            return digests;
        }

        public boolean hasMore() {
            return hasMore;
        }
    }

    /**
     * One bounded page of this node's local key set, in ascending key order.
     * <p>
     * {@code accept} is applied while the page is being filled, not afterwards. Filtering after
     * the fact would make a page shorter than {@code limit} even when more matching keys remain,
     * which is indistinguishable from exhaustion and would end the caller's pagination early.
     * It is a plain {@link Predicate} so this class stays ignorant of the ACL layer.
     *
     * @param prefix     keys must start with these bytes; empty or {@code null} matches all
     * @param startAfter exclusive lower bound, or {@code null} to start at the first key
     * @param limit      maximum entries to return; must be positive
     * @param accept     per-key admission test (authorization); never {@code null}
     */
    public ScanPage scanLocal(final ByteBuffer prefix,
                              final ByteBuffer startAfter,
                              final int limit,
                              final Predicate<HashedBytes> accept) {
        if (limit < 1) {
            throw new IllegalArgumentException("Scan limit must be >= 1, got " + limit);
        }
        final ByteBuffer from = prefix == null ? ByteBuffers.EMPTY : prefix;
        // Seek straight to the cursor (or the prefix's first possible key) instead of walking
        // the whole key set -- this is the reason the sorted index exists. Start at whichever
        // bound is higher: a cursor that sorts before the prefix would otherwise land the
        // iterator on non-matching keys, and the ascending-order break below would end the page
        // immediately, reporting the range as empty.
        //
        // Both bounds are only ever compared, never hashed, so they are wrapped zero-copy and
        // once per page; the per-key prefix test below reads the raw buffer directly.
        final NavigableSet<HashedBytes> tail = startAfter == null || startAfter.compareTo(from) < 0
                ? sortedKeys.tailSet(HashedBytes.viewOf(from), true)
                : sortedKeys.tailSet(HashedBytes.viewOf(startAfter), false);

        final int effectiveLimit = Math.min(limit, KvLimits.MAX_SCAN_LIMIT);
        final List<ClientMessage.ScanEntry> page = new ArrayList<>(Math.min(effectiveLimit, 64));
        boolean hasMore = false;
        long pageBytes = 0;
        for (final HashedBytes key : tail) {
            if (!key.startsWith(from)) {
                // Ascending order: the first key past the prefix ends the range for good.
                break;
            }
            final KeyState keyState = states.get(key);
            if (keyState == null || keyState.accepted.isZero()) {
                continue; // promise-only keys are not part of the visible key set
            }
            if (!accept.test(key)) {
                continue;
            }
            // Stop on either bound. Checked against an admissible key, so hasMore reflects a key
            // that really would have been returned -- never a false "more" from a filtered one.
            // The byte budget is what actually bounds the response: the count limit alone would
            // still allow a gigantic page if keys are large.
            if (page.size() == effectiveLimit
                    || (!page.isEmpty() && pageBytes + key.size() > KvLimits.MAX_SCAN_PAGE_BYTES)) {
                hasMore = true;
                break;
            }
            page.add(new ClientMessage.ScanEntry(
                    key.toBuffer(), keyState.accepted, keyState.tombstone));
            pageBytes += key.size();
        }
        return new ScanPage(page, hasMore);
    }

    /** A bounded slice of the local key set plus whether more matching keys follow it. */
    public static final class ScanPage {
        private final List<ClientMessage.ScanEntry> entries;
        private final boolean hasMore;

        ScanPage(final List<ClientMessage.ScanEntry> entries, final boolean hasMore) {
            this.entries = entries;
            this.hasMore = hasMore;
        }

        public List<ClientMessage.ScanEntry> entries() {
            return entries;
        }

        public boolean hasMore() {
            return hasMore;
        }
    }

    /**
     * Evicts promise-only KeyState entries (never accepted) from the states map
     * and secondary index. These are transient metadata from PrepareReq that
     * never reached Accept and would otherwise accumulate without bound
     * <p>
     * The promise itself is not evicted with the state: {@link #forgetPromise} keeps it as a floor,
     * so a late accept below it is still refused. The age threshold bounds how much of the ballot
     * space that floor covers; it is not what makes the eviction safe.
     */
    public int evictPromiseOnly() {
        int evicted = 0;
        final long now = System.nanoTime();
        final Iterator<Map.Entry<HashedBytes, KeyState>> it = states.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<HashedBytes, KeyState> entry = it.next();
            final KeyState ks = entry.getValue();
            if (ks.accepted.isZero() && (now - ks.promisedAtNanos) >= MIN_PROMISE_AGE_NANOS) {
                dropIndexEntries(entry.getKey());
                forgetPromise(ks.promised);
                it.remove();
                evicted++;
            }
        }
        return evicted;
    }

    public boolean wasTouchedRecently(final HashedBytes key) {
        return recentlyTouched.contains(key);
    }

    public void clearRecentlyTouched() {
        recentlyTouched.clear();
    }

    long maxBallotSeen() {
        return maxBallotSeen;
    }

    private void trackBallot(final Ballot ballot) {
        if (ballot.counter() > maxBallotSeen) {
            maxBallotSeen = ballot.counter();
        }
    }

    /**
     * Recompute and cache the digest for a single range
     * Called only for ranges in the dirty set
     */
    private void recomputeRangeDigest(final int range) {
        final NavigableSet<HashedBytes> keysInRange = keysByRange.get(range);
        if (keysInRange == null || keysInRange.isEmpty()) {
            rangeDigestCache.remove(range);
            return;
        }
        final List<KeyDigest> digests = new ArrayList<>(keysInRange.size());
        for (final HashedBytes key : keysInRange) {
            final KeyState keyState = states.get(key);
            if (keyState != null && !keyState.accepted.isZero()) {
                digests.add(keyState.digest(key));
            }
        }
        if (digests.isEmpty()) {
            rangeDigestCache.remove(range);
            return;
        }
        // No sort: keysByRange iterates in key order, and hashDigestList requires that order.
        rangeDigestCache.put(range, hashDigestList(digests));
    }

    /**
     * Hash a range's digest list into the value peers compare during anti-entropy.
     * <p>
     * Both variable-length fields ({@code key} and the ballot's {@code nodeId}) are
     * length-prefixed. Concatenating them unframed let distinct digest lists collide -- a key
     * one byte longer against a node id one byte shorter produces identical input -- and two
     * replicas that disagree on a range would then agree on its digest and skip the repair.
     */
    private static HashedBytes hashDigestList(final List<KeyDigest> digests) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final ByteBuffer scalars = ByteBuffer.allocate(Integer.BYTES + Long.BYTES)
                    .order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < digests.size(); i++) {
                final KeyDigest keyDigest = digests.get(i);
                final ByteBuffer key = keyDigest.key().toBuffer();
                final byte[] nodeId = keyDigest.accepted().nodeId().value()
                        .getBytes(StandardCharsets.UTF_8);

                updateLengthPrefixed(digest, scalars, key.remaining());
                digest.update(key);

                scalars.clear();
                scalars.putLong(keyDigest.accepted().counter()).flip();
                digest.update(scalars);

                updateLengthPrefixed(digest, scalars, nodeId.length);
                digest.update(nodeId);

                // valueHash is a fixed-width SHA-256 and the flag is one byte, so neither
                // needs framing.
                digest.update(keyDigest.valueHash().toBuffer());
                digest.update((byte) (keyDigest.tombstone() ? 1 : 0));
            }
            return new HashedBytes(digest.digest());
        } catch (final Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void updateLengthPrefixed(final MessageDigest digest, final ByteBuffer scratch,
                                             final int length) {
        scratch.clear();
        scratch.putInt(length).flip();
        digest.update(scratch);
    }

}
