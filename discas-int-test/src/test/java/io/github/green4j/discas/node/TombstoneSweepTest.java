/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestAwait;
import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.EventLoop;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.transport.PeerTransport;
import io.github.green4j.discas.node.wal.InMemoryWal;
import io.github.green4j.discas.node.wal.Wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which tombstone is swept next, and how often -- the two halves of the policy that decides how fast
 * a node can collect at all.
 * <p>
 * The store keeps its tombstones in the order they were written, so the sweeper's candidate is
 * always the one left alone longest and <em>nothing to collect</em> costs one lookup. The sweeper
 * takes one candidate per interval and only once the previous decision has landed, which is what
 * keeps a cluster that cannot collect from asking any faster than a cluster that can.
 * <p>
 * The cluster of one here is deliberate: a sole member answers its own check, so every case is about
 * picking and pacing rather than about the decision, which is {@code PurgeDecisionTest}'s subject.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
@DisplayName("Tombstone sweep -- which candidate, and how often")
class TombstoneSweepTest {

    private static final NodeId SELF = NodeId.of("self");
    private static final Ballot VALUE_BALLOT = new Ballot(4L, SELF);
    private static final Ballot TOMBSTONE = new Ballot(7L, SELF);
    private static final Ballot LATER_TOMBSTONE = new Ballot(9L, SELF);
    private static final HashedBytes KEY_A = TestBytes.hashed("a");
    private static final HashedBytes KEY_B = TestBytes.hashed("b");
    private static final HashedBytes KEY_C = TestBytes.hashed("c");

    private static final Duration INTERVAL = Duration.ofMillis(20);

    /** A cluster of one: no peers to ask, so a sweep is decided by this node's own answer. */
    private static final class SoleMember implements PeerTransport {
        @Override
        public void send(final NodeId target, final PeerMessage message) {
            throw new AssertionError("A sole member has nobody to send to: " + message);
        }

        @Override
        public void register(final Consumer<PeerMessage> handler) {
        }

        @Override
        public List<NodeId> peers() {
            return List.of();
        }

        @Override
        public int clusterSize() {
            return 1;
        }
    }

    /** A member that is asked and never answers: silence blocks, so no sweep ever collects. */
    private static final class SilentPeer implements PeerTransport {
        private static final NodeId PEER = NodeId.of("silent");
        private final List<PeerMessage.PurgeCheckReq> checks =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void send(final NodeId target, final PeerMessage message) {
            if (message instanceof PeerMessage.PurgeCheckReq) {
                checks.add((PeerMessage.PurgeCheckReq) message);
            }
        }

        @Override
        public void register(final Consumer<PeerMessage> handler) {
        }

        @Override
        public List<NodeId> peers() {
            return List.of(PEER);
        }

        @Override
        public int clusterSize() {
            return 2;
        }
    }

    private final InMemoryWal wal = new InMemoryWal();
    private final LocalStore store = recovered(wal);
    private final List<HashedBytes> collectedOrder = new ArrayList<>();
    private final NodeObserver observer = new NodeObserver() {
        @Override
        public void tombstoneSwept(final TombstoneSweep sweep) {
            if (sweep.collected()) {
                collectedOrder.add(sweep.key());
            }
        }
    };

    private EventLoop loop;
    private TombstoneSweeper sweeper;

    @AfterEach
    void tearDown() {
        if (sweeper != null) {
            sweeper.close();
        }
        if (loop != null) {
            loop.shutdown();
        }
    }

    private static LocalStore recovered(final Wal wal) {
        final LocalStore store = new LocalStore(wal);
        final LocalStore.ThrottledLoader loader = store.beginRecovery();
        while (loader.loadBatch(1024)) {
            // drain
        }
        return store;
    }

    /**
     * Every key is this node's, so these cases are about the queue and nothing else. Which keys a
     * node offers is {@code SweepAffinityTest}'s subject, kept apart because the two rules compose
     * but are not the same rule: the queue decides the order, ownership decides the subset.
     */
    private static final Predicate<HashedBytes> ALL = key -> true;

    /**
     * Affinity over a single-member view, so this node owns every key and these cases stay about
     * pacing. Deliberately not the same transport the collector is given -- one of these tests
     * needs a peer that never answers, and giving that peer a share of the keys would make the test
     * about which keys it got.
     */
    private static SweepAffinity ownEverything() {
        return new SweepAffinity(SELF, new SoleMember());
    }

    private void delete(final HashedBytes key, final Ballot ballot) {
        store.writeAccept(key, ballot, null, true);
    }

    private void startSweeping() {
        loop = new EventLoop("sweep");
        loop.start();
        sweeper = new TombstoneSweeper(store, ownEverything(),
                new TombstoneCollector(SELF, store, new SoleMember(), loop,
                        new CorrelationIdGenerator(SELF), Duration.ofSeconds(1)),
                loop, observer, INTERVAL);
        sweeper.start();
    }

    @Test
    @DisplayName("A store with no tombstones has no candidate, and finding that out is one lookup")
    void nothingToCollect() {
        assertNull(store.tombstoneCandidate(ALL));
        assertEquals(0, store.tombstoneCount());

        store.writeAccept(KEY_A, VALUE_BALLOT, TestBytes.hashed("v"), false);

        assertNull(store.tombstoneCandidate(ALL), "A live key is not a candidate");
        assertEquals(0, store.tombstoneCount());
    }

    @Test
    @DisplayName("A tombstone is a candidate from the moment it is written -- there is no waiting period")
    void aTombstoneIsACandidateAtOnce() {
        delete(KEY_A, TOMBSTONE);

        assertEquals(1, store.tombstoneCount());
        assertEquals(KEY_A, store.tombstoneCandidate(ALL),
                "No minimum age: what makes a collection safe is that every member permits it, "
                        + "so a delete that has not reached everyone is refused, not deferred");
    }

    @Test
    @DisplayName("The candidate is the tombstone left alone longest")
    void oldestFirst() {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);

        assertEquals(KEY_A, store.tombstoneCandidate(ALL),
                "Insertion order is age order, and the oldest is the only candidate offered");
    }

    @Test
    @DisplayName("A tombstone written again is not one that has been left alone")
    void aRewrittenTombstoneGoesToTheBack() {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);

        // What a repair round does when it re-replicates a tombstone to a replica that is behind,
        // and what section 5 of the design means by a key under traffic restarting its wait.
        delete(KEY_A, LATER_TOMBSTONE);

        assertEquals(KEY_B, store.tombstoneCandidate(ALL),
                "The one just written is behind the one that has been sitting there");
    }

    @Test
    @DisplayName("A key written back to a value is not a tombstone any more")
    void aValueOverATombstoneLeavesTheQueue() {
        delete(KEY_A, TOMBSTONE);

        store.writeAccept(KEY_A, LATER_TOMBSTONE, TestBytes.hashed("v"), false);

        assertEquals(0, store.tombstoneCount(), "The key exists again; there is nothing to collect");
        assertNull(store.tombstoneCandidate(ALL));
    }

    @Test
    @DisplayName("A node that owns none of what it holds offers its oldest anyway")
    void aForeignTombstoneIsStillOfferedWhenNothingIsOwned() {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);

        // The state a member is left in when it misses a PURGE: it holds a tombstone the rest of
        // the cluster has dropped, and the key's owner cannot offer it again -- the owner dropped
        // the key and has nothing left in its own queue. If ownership were a veto rather than a
        // preference this tombstone would stay forever, which is what the chaos matrix caught.
        assertEquals(KEY_A, store.tombstoneCandidate(key -> false),
                "Ownership orders the queue; it does not shorten it");
    }

    @Test
    @DisplayName("What this node owns is offered ahead of what it merely holds")
    void anOwnedTombstoneWinsOverAnOlderForeignOne() {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);

        assertEquals(KEY_B, store.tombstoneCandidate(KEY_B::equals),
                "KEY_A is older, but its owner is another node and will offer it there");
    }

    @Test
    @DisplayName("A collected key leaves the queue with the key itself")
    void aPurgedKeyLeavesTheQueue() {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);

        assertTrue(store.purge(KEY_A, TOMBSTONE));

        assertEquals(1, store.tombstoneCount());
        assertEquals(KEY_B, store.tombstoneCandidate(ALL),
                "An index that kept a purged key would offer a candidate that is not there");
    }

    @Test
    @DisplayName("Replay rebuilds the queue: a tombstone read back from the log is a candidate again")
    void replayRebuildsTheQueue() {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);
        wal.force();

        final LocalStore afterRestart = recovered(wal);

        assertEquals(2, afterRestart.tombstoneCount(), "The tombstones are still there, as they must be");
        assertEquals(KEY_A, afterRestart.tombstoneCandidate(ALL),
                "And in log order, which is the order they were written in -- a restart costs a "
                        + "node its place in no queue, because the order is not a clock");
    }

    @Test
    @DisplayName("A tombstone is swept, and the key goes")
    void aTombstoneIsCollected() throws Exception {
        delete(KEY_A, TOMBSTONE);
        startSweeping();

        TestAwait.until("the tombstone is collected", Duration.ofSeconds(10), () -> {
            if (store.get(KEY_A) != null) {
                throw new IllegalStateException("Still held");
            }
        });
        assertEquals(1, wal.countEntries(Wal.Entry.Purge.class),
                "And the decision is in the log, so replay does not bring the key back");
        assertEquals(List.of(KEY_A), collectedOrder);
    }

    @Test
    @DisplayName("A blocked candidate is asked about again, not swept around -- and no faster than the interval")
    void aBlockedCandidateIsRetriedNotRotated() throws Exception {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);

        final Duration checkTimeout = Duration.ofMillis(50);
        final SilentPeer silent = new SilentPeer();
        loop = new EventLoop("sweep");
        loop.start();
        sweeper = new TombstoneSweeper(store, ownEverything(),
                new TombstoneCollector(SELF, store, silent, loop,
                        new CorrelationIdGenerator(SELF), checkTimeout),
                loop, observer, INTERVAL);
        final long startedAtNanos = System.nanoTime();
        sweeper.start();

        TestAwait.until("three sweeps have been blocked", Duration.ofSeconds(10), () -> {
            if (silent.checks.size() < 3) {
                throw new IllegalStateException(silent.checks.size() + " sweeps so far");
            }
        });
        final long elapsedNanos = System.nanoTime() - startedAtNanos;

        for (final PeerMessage.PurgeCheckReq check : List.copyOf(silent.checks)) {
            assertEquals(KEY_A, check.key(),
                    "The member holding this key up is what an operator has to act on; sweeping "
                            + "around it would hide exactly that");
        }
        assertEquals(2, store.tombstoneCount(), "And nothing was collected");
        // Each sweep costs its own deadline before the next one is even armed, which is what keeps a
        // cluster with a member down from asking as fast as it can.
        assertTrue(elapsedNanos >= 2 * (INTERVAL.toNanos() + checkTimeout.toNanos()),
                "Three blocked sweeps in " + Duration.ofNanos(elapsedNanos) + " is a retry storm");
    }

    @Test
    @DisplayName("One candidate at a time, oldest first, until there is nothing left to collect")
    void keysAreCollectedOneAtATimeInAgeOrder() throws Exception {
        delete(KEY_A, TOMBSTONE);
        delete(KEY_B, TOMBSTONE);
        delete(KEY_C, TOMBSTONE);
        startSweeping();

        TestAwait.until("every tombstone is collected", Duration.ofSeconds(10), () -> {
            if (store.tombstoneCount() != 0) {
                throw new IllegalStateException(store.tombstoneCount() + " left");
            }
        });

        // The order is the queue's, not the scheduler's: whatever the interval, a sweep takes the
        // one left alone longest, and takes exactly one.
        assertEquals(List.of(KEY_A, KEY_B, KEY_C), collectedOrder);
    }
}
