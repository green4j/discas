/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.TestBytes;
import io.github.green4j.discas.common.Ballot;
import io.github.green4j.discas.common.identity.NodeId;
import io.github.green4j.discas.node.wal.InMemoryWal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The accounting behind the capacity refusal, and the reason it needs a test of its own.
 * <p>
 * {@code storedBytes} is a running total maintained at every point that changes what a key holds --
 * an accept, a purge, a snapshot entry, a replayed log record. Nothing else in the node ever
 * recomputes it from the map, so a path that mutates state without adjusting the total is a drift
 * that no other test can see: too low and the guard admits writes it should refuse, too high and the
 * node refuses every write with plenty of room left, which looks exactly like a full cluster.
 * <p>
 * So the assertions here are arithmetic against an independently-computed expectation rather than
 * against the store's own view of itself, and the replay case is the one that matters most: replay
 * does not go through {@code writeAccept}, so it is the path most easily left out.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
@DisplayName("Store capacity -- what a key costs, and what refuses to grow")
class StoreCapacityTest {

    private static final NodeId SELF = NodeId.of("self");
    private static final HashedBytes KEY = TestBytes.hashed("key");
    private static final HashedBytes OTHER = TestBytes.hashed("other-key");
    private static final HashedBytes SMALL = TestBytes.hashed("v");
    private static final HashedBytes LARGE = TestBytes.hashed("vvvvvvvvvvvvvvvvvvvv");

    private static final long OVERHEAD = LocalStore.ENTRY_FOOTPRINT_BYTES;

    private final InMemoryWal wal = new InMemoryWal();
    private final LocalStore store = new LocalStore(wal, NodeObserver.NONE, Long.MAX_VALUE);

    private static Ballot ballot(final long counter) {
        return new Ballot(counter, SELF);
    }

    /** What one entry is expected to cost: its own bytes plus the objects around it, charged once. */
    private static long expected(final HashedBytes key, final HashedBytes value) {
        return OVERHEAD + key.size() + (value == null ? 0L : value.size());
    }

    @Test
    @DisplayName("A key costs its bytes plus one entry's overhead, and only one")
    void anEntryIsChargedOnce() {
        assertEquals(0L, store.storedBytes(), "An empty store holds nothing");

        store.writeAccept(KEY, ballot(1), SMALL, false);
        assertEquals(expected(KEY, SMALL), store.storedBytes());

        // Overwriting an existing key: the map node, the wrappers and the index entries are already
        // there, so only the values differ.
        store.writeAccept(KEY, ballot(2), LARGE, false);
        assertEquals(expected(KEY, LARGE), store.storedBytes(),
                "An overwrite pays the difference, not another entry");

        store.writeAccept(KEY, ballot(3), SMALL, false);
        assertEquals(expected(KEY, SMALL), store.storedBytes(),
                "And an overwrite that shrinks has to be seen to shrink");
    }

    @Test
    @DisplayName("A stale accept changes nothing, including the accounting")
    void aStaleAcceptDoesNotMoveTheTotal() {
        store.writeAccept(KEY, ballot(5), LARGE, false);
        final long afterWrite = store.storedBytes();

        store.writeAccept(KEY, ballot(2), SMALL, false);

        assertEquals(afterWrite, store.storedBytes(),
                "The state was not replaced, so nothing was released either");
    }

    @Test
    @DisplayName("A tombstone releases the value and keeps the key; a purge releases both")
    void deletingReleasesInTwoSteps() {
        store.writeAccept(KEY, ballot(1), LARGE, false);

        store.writeAccept(KEY, ballot(2), null, true);
        assertEquals(expected(KEY, null), store.storedBytes(),
                "The value is gone but the key and its entry are still held");

        assertTrue(store.purge(KEY, ballot(2)));
        assertEquals(0L, store.storedBytes(), "Collection is what finally gives the entry back");
    }

    /**
     * Also why {@code evictPromiseOnly} needs no accounting of its own: it removes exactly the
     * states charged for here, which is nothing.
     */
    @Test
    @DisplayName("A promise-only key costs nothing")
    void promisesAreNotStoredPairs() {
        store.writePromise(KEY, ballot(1));

        assertEquals(0L, store.storedBytes(),
                "A key that exists only as a promise holds no pair to charge for");
    }

    @Test
    @DisplayName("A node that replayed its log knows what it is holding")
    void replayRebuildsTheTotal() {
        store.writeAccept(KEY, ballot(1), LARGE, false);
        store.writeAccept(OTHER, ballot(2), SMALL, false);
        store.writeAccept(KEY, ballot(3), SMALL, false);
        final long live = store.storedBytes();

        // The path that does not go through writeAccept. A node coming back with a total of zero
        // would admit every write until something overwrote every key it holds.
        final LocalStore recovered = new LocalStore(wal, NodeObserver.NONE, Long.MAX_VALUE);
        final LocalStore.ThrottledLoader loader = recovered.beginRecovery();
        while (!loader.isDone()) {
            loader.loadBatch(1000);
        }

        assertEquals(live, recovered.storedBytes(),
                "A restart must not change what this node believes it is holding");
        assertEquals(expected(KEY, SMALL) + expected(OTHER, SMALL), recovered.storedBytes());
    }

    /**
     * The asymmetry worth stating: a serving node answers "no" and stays up, a replaying one stops.
     * There is no caller to refuse when a node is recovering what it already owns, and carrying on
     * regardless is how a member gets killed by the JVM halfway through starting.
     */
    @Test
    @DisplayName("Replay of a log that does not fit fails the node instead of half-loading it")
    void replayRefusesToOverfillTheHeap() {
        store.writeAccept(KEY, ballot(1), LARGE, false);
        store.writeAccept(OTHER, ballot(2), LARGE, false);

        // Room for one of the two entries: replay gets part way and must not continue.
        final LocalStore tooSmall = new LocalStore(
                wal, NodeObserver.NONE, expected(KEY, LARGE) + 1);
        final LocalStore.ThrottledLoader loader = tooSmall.beginRecovery();

        assertThrows(StoreCapacityExceededException.class, () -> {
            while (!loader.isDone()) {
                loader.loadBatch(1000);
            }
        }, "A node that cannot hold its own log must say so instead of starting");
    }

    @Test
    @DisplayName("The budget refuses what would grow the store, and never a delete")
    void theBudgetRefusesGrowthOnly() {
        final long roomForOne = expected(KEY, LARGE);
        final Capacity seen = new Capacity();
        final LocalStore bounded = new LocalStore(wal, seen, roomForOne);

        assertTrue(bounded.hasCapacityFor(KEY, LARGE, false), "The first key fits exactly");
        bounded.writeAccept(KEY, ballot(1), LARGE, false);

        assertFalse(bounded.hasCapacityFor(OTHER, SMALL, false), "A second entry does not");
        assertFalse(seen.available, "And the node says so once, on the way over the line");

        assertTrue(bounded.hasCapacityFor(OTHER, SMALL, true),
                "A delete is never refused -- a store that cannot shrink has no way back");
        assertTrue(bounded.hasCapacityFor(KEY, SMALL, false),
                "Nor is an overwrite that shrinks the store");
        assertTrue(seen.available, "Which is also what clears the condition");
    }

    /** Captures the flip only, which is all the store reports. */
    private static final class Capacity implements NodeObserver {
        private boolean available = true;

        @Override
        public void storeCapacity(final boolean nowAvailable, final long storedBytes,
                                  final long capacityBytes) {
            this.available = nowAvailable;
        }
    }
}
